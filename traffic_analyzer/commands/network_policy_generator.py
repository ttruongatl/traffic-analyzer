import click
import datetime
from typing import Dict, List, Set, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import ruamel.yaml
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class DependencyRecord:
    is_ccp: bool
    namespace: str
    label_key: str
    label_value: str
    index: int

class NetworkPolicyGenerator:
    def __init__(self, dbname="traffic_analyzer", user="postgres", host="localhost", port="5432"):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": click.prompt("Enter password for PostgreSQL user", hide_input=True),
            "host": host,
            "port": port
        }

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def fetch_dependency_data(self, component: str, start_date: datetime.date, 
                            end_date: datetime.date) -> List[Dict]:
        """
        Fetch dependency data from PostgreSQL with enhanced error handling and deduplication.
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute("""
                        WITH all_results AS (
                            SELECT 
                                jsonb_array_elements(results) as result,
                                dateonly,
                                chunk_start,
                                chunk_end
                            FROM public.component_dependency_chunks
                            WHERE dateonly BETWEEN %s AND %s
                            AND component = %s
                        ),
                        unique_dependencies AS (
                            SELECT DISTINCT ON (
                                (result->>'isCCP')::boolean,
                                result->>'PodNamespace',
                                result->>'LabelKey',
                                result->>'LabelValue'
                            )
                                (result->>'isCCP')::boolean as is_ccp,
                                result->>'PodNamespace' as namespace,
                                result->>'LabelKey' as label_key,
                                result->>'LabelValue' as label_value,
                                MIN(dateonly) as first_seen,
                                MAX(dateonly) as last_seen,
                                COUNT(DISTINCT dateonly) as days_seen
                            FROM all_results
                            GROUP BY 1,2,3,4
                        )
                        SELECT *
                        FROM unique_dependencies
                        ORDER BY 
                            is_ccp,
                            namespace NULLS LAST,
                            label_key,
                            label_value;
                    """, (start_date, end_date, component))
                    
                    results = cur.fetchall()
                    if not results:
                        click.echo(f"No data found for component '{component}' between {start_date} and {end_date}")
                        return []
                    return results
                except psycopg2.Error as e:
                    click.echo(f"Database error: {str(e)}")
                    raise

    def generate_network_policy(self, records: List[Dict], namespace: str) -> CommentedMap:
        """
        Generate NetworkPolicy with enhanced structure and metadata.
        """
        yaml = ruamel.yaml.YAML()
        yaml.preserve_quotes = True
        yaml.indent(mapping=2, sequence=4, offset=2)

        network_policy = CommentedMap({
            'apiVersion': 'networking.k8s.io/v1',
            'kind': 'NetworkPolicy',
            'metadata': CommentedMap({
                'name': f'allow-{namespace}-ingress',
                'namespace': namespace,
                'annotations': CommentedMap({
                    'description': 'Auto-generated NetworkPolicy based on observed traffic patterns',
                    'generated-at': datetime.utcnow().isoformat(),
                    'data-period': f"Records analyzed from {records[0]['first_seen']} to {records[0]['last_seen']}"
                })
            }),
            'spec': CommentedMap({
                'podSelector': CommentedMap({
                    'matchLabels': CommentedMap({
                        'app': namespace
                    })
                }),
                'policyTypes': ['Ingress'],
                'ingress': CommentedSeq()
            })
        })

        ingress_rules = network_policy['spec']['ingress']
        idx_counter = 1

        # Process non-CCP rules
        non_ccp_records = [r for r in records if not r['is_ccp']]
        if non_ccp_records:
            ingress_rules.yaml_set_start_comment('# Non-CCP Ingress Rules', indent=0)
            for record in non_ccp_records:
                self._add_ingress_rule(
                    ingress_rules, 
                    record, 
                    idx_counter,
                    f"Allow ingress traffic from {record['namespace']} ({record['label_value']}) - "
                    f"Seen {record['days_seen']} days between {record['first_seen']} and {record['last_seen']}"
                )
                idx_counter += 1

        # Process CCP rules
        ccp_records = [r for r in records if r['is_ccp']]
        if ccp_records:
            ccp_start_index = len(ingress_rules)
            ingress_rules.yaml_set_comment_before_after_key(
                ccp_start_index, 
                before='\n# CCP Ingress Rules', 
                indent=0
            )
            for record in ccp_records:
                self._add_ingress_rule(
                    ingress_rules,
                    record,
                    idx_counter,
                    f"Allow ingress traffic from CCP {record['label_value']} - "
                    f"Seen {record['days_seen']} days between {record['first_seen']} and {record['last_seen']}"
                )
                idx_counter += 1

        return network_policy

    def _add_ingress_rule(self, ingress_rules: CommentedSeq, record: Dict, 
                         index: int, comment: str) -> None:
        """Helper method to add an ingress rule with proper formatting."""
        namespace_selector = CommentedMap({
            'matchLabels': CommentedMap(
                {'aks.azure.com/msi-enabled': 'true'} if record['is_ccp']
                else {'kubernetes.io/metadata.name': record['namespace']}
            )
        })

        pod_selector = CommentedMap({
            'matchLabels': CommentedMap({
                record['label_key']: record['label_value']
            })
        })

        ingress_rule = CommentedMap({
            'from': [{
                'namespaceSelector': namespace_selector,
                'podSelector': pod_selector
            }],
            'ports': [
                CommentedMap({'protocol': 'TCP', 'port': 8081}),
                CommentedMap({'protocol': 'TCP', 'port': 9102})
            ]
        })
        
        ingress_rule.yaml_set_start_comment(f"{index}. {comment}", indent=0)
        ingress_rules.append(ingress_rule)

