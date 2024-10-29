# traffic_analyzer/cli.py

import click
from .commands.collect_traffic import TrafficCollector, PostgresManager
from .commands.kusto_tester import KustoTester
import datetime
from datetime import datetime, time, timedelta
import yaml
import ruamel.yaml
from ruamel.yaml.comments import CommentedMap, CommentedSeq

import psycopg2
from psycopg2.extras import Json
from typing import Dict, Any, List
import traceback
from dataclasses import dataclass

@click.group()
@click.version_option()
def cli():
    """A CLI tool to help with analyzing traffic"""
    pass


@cli.command()
@click.option('--component', '-c', required=True, help='Component to collect traffic data for')
@click.option('--days', default=2, help='Number of days to collect data for')
@click.option('--chunk-minutes', default=15, help='Number of minutes per chunk')
@click.option('--max-workers', default=16, help='Maximum number of parallel workers')
@click.option('--force', is_flag=True, help='Force replace existing data')
def collect(component: str, days: int, chunk_minutes: int, max_workers: int, force: bool):
    """Collect traffic data and store in PostgreSQL."""
    click.echo(f"Collecting traffic data for {component}...")
    
    try:
        collector = TrafficCollector(
            days_to_collect=days,
            chunk_minutes=chunk_minutes,
            max_workers=max_workers
        )
        
        collector.collect_and_store(component, force_replace=force)
        
        click.echo("\nCollection completed successfully!")
        
    except Exception as e:
        click.echo(f"\nError during collection: {str(e)}", err=True)
        raise click.Abort()

@cli.command()
@click.option('--component', '-c', required=True, help='Component to generate NetworkPolicy for')
@click.option('--days', default=30, help='Number of days to aggregate data for')
@click.option('--namespace', '-n', required=True, help='Namespace for the NetworkPolicy')
@click.option('--output', '-o', default='networkpolicy.yaml', help='Output file for the NetworkPolicy YAML')
def generate_np(component: str, days: int, namespace: str, output: str):
    """Generate NetworkPolicy YAML from collected data over a range of days."""
    # Calculate date range
    end_date = datetime.utcnow().date() - timedelta(days=1)  # Exclude today
    start_date = end_date - timedelta(days=days - 1)
    
    pg_manager = PostgresManager()
    results = pg_manager.get_results_for_date_range(start_date, end_date, component)
    if not results:
        click.echo(f"No data found for component '{component}' in the last {days} days")
        return
    
    # Separate ingress rules into non-CCP and CCP groups
    ingress_rule_entries = []
    
    for idx, record in enumerate(results, start=1):
        is_ccp = record.get('isCCP', 'False')
        label_key = record['LabelKey']
        label_value = record['LabelValue']
        pod_namespace = record.get('PodNamespace', 'default')
        
        if is_ccp == 'True':
            # CCP component
            namespace_selector = {'matchLabels': {'aks.azure.com/msi-enabled': 'true'}}
        else:
            # Non-CCP component
            namespace_selector = {'matchLabels': {'kubernetes.io/metadata.name': pod_namespace}}
        
        pod_selector = {'matchLabels': {label_key: label_value}}
        
        ingress_rule_entry = {
            'is_ccp': is_ccp,
            'namespace': pod_namespace,
            'label_key': label_key,
            'label_value': label_value,
            'namespace_selector': namespace_selector,
            'pod_selector': pod_selector,
            'index': idx  # Row number index
        }
        
        ingress_rule_entries.append(ingress_rule_entry)
    
    # Remove duplicates
    ingress_rule_entries_unique = {(
        entry['is_ccp'],
        tuple(entry['namespace_selector']['matchLabels'].items()),
        tuple(entry['pod_selector']['matchLabels'].items())
    ): entry for entry in ingress_rule_entries}.values()
    
    # Separate and sort the entries
    non_ccp_entries = [entry for entry in ingress_rule_entries_unique if entry['is_ccp'] == 'False']
    ccp_entries = [entry for entry in ingress_rule_entries_unique if entry['is_ccp'] == 'True']
    
    # Sort non-CCP entries by namespace name alphabetically
    non_ccp_entries.sort(key=lambda x: x['namespace'])
    
    # Sort CCP entries by label key
    ccp_entries.sort(key=lambda x: x['label_key'])
    
    # Initialize ruamel.yaml YAML object
    yaml = ruamel.yaml.YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    
    # Prepare the NetworkPolicy data structure
    network_policy = CommentedMap({
        'apiVersion': 'networking.k8s.io/v1',
        'kind': 'NetworkPolicy',
        'metadata': CommentedMap({
            'name': 'allow-msi-connector-ingress',
            'namespace': namespace
        }),
        'spec': CommentedMap({
            'podSelector': CommentedMap({
                'matchLabels': CommentedMap({
                    'app': 'msi-connector'
                })
            }),
            'policyTypes': ['Ingress'],
            'ingress': CommentedSeq()
        })
    })
    
    ingress_rules = network_policy['spec']['ingress']
    idx_counter = 1  # To keep track of the row numbers
    
    # Add non-CCP ingress rules
    if non_ccp_entries:
        # Add dividing comment before the first non-CCP ingress rule
        ingress_rules.yaml_set_start_comment('# Non-CCP Ingress Rules', indent=0)
        
        for entry in non_ccp_entries:
            comment = f"{idx_counter}. Allow ingress traffic from {entry['namespace']} ({entry['label_value']})"
            ingress_rule = CommentedMap({
                'from': [
                    {
                        'namespaceSelector': CommentedMap(entry['namespace_selector']),
                        'podSelector': CommentedMap(entry['pod_selector'])
                    }
                ],
                'ports': [
                    CommentedMap({'protocol': 'TCP', 'port': 8081}),
                    CommentedMap({'protocol': 'TCP', 'port': 9102})
                ]
            })
            ingress_rule.yaml_set_start_comment(comment, indent=0)
            ingress_rules.append(ingress_rule)
            idx_counter += 1
    
    # Add CCP ingress rules
    if ccp_entries:
        # Get the index where CCP entries start
        ccp_start_index = len(ingress_rules)
        # Add dividing comment before the first CCP ingress rule
        ingress_rules.yaml_set_comment_before_after_key(ccp_start_index, before='\n# CCP Ingress Rules', indent=0)
        
        for entry in ccp_entries:
            comment = f"{idx_counter}. Allow ingress traffic from {entry['label_value']} ({entry['label_value']})"
            ingress_rule = CommentedMap({
                'from': [
                    {
                        'namespaceSelector': CommentedMap(entry['namespace_selector']),
                        'podSelector': CommentedMap(entry['pod_selector'])
                    }
                ],
                'ports': [
                    CommentedMap({'protocol': 'TCP', 'port': 8081}),
                    CommentedMap({'protocol': 'TCP', 'port': 9102})
                ]
            })
            ingress_rule.yaml_set_start_comment(comment, indent=0)
            ingress_rules.append(ingress_rule)
            idx_counter += 1
    
    # Write to YAML file
    with open(output, 'w') as f:
        yaml.dump(network_policy, f)
    
    click.echo(f"NetworkPolicy YAML has been written to {output}")

@cli.command(name='test-kusto')
@click.option('--component', '-c', required=True, help='Component to test query for')
@click.option('--output', '-o', type=click.Path(), help='Output file path (optional)')
def test_kusto(component, output):
    """Test raw Kusto query with a single result."""
    click.echo(f"Testing raw Kusto query for component: {component}")

    tester = KustoTester()
    result = tester.test_raw_query(component)

    if result['success']:
        click.echo(click.style("✓ Query executed successfully", fg='green'))
        click.echo(f"Found {result['row_count']} results")

        # Print the first result with formatting
        if result['results']:
            first_result = result['results'][0]
            click.echo("\nRaw Result:")
            click.echo("-" * 40)
            # Convert the KustoResultRow to a dictionary
            first_result_dict = first_result.to_dict()
            for key, value in first_result_dict.items():
                # Format the output for better readability
                value_str = str(value)
                if len(value_str) > 100:
                    value_str = value_str[:97] + "..."
                click.echo(f"{key:15}: {value_str}")

        # Save to file if specified
        if output:
            import json
            with open(output, 'w') as f:
                json.dump(result, f, indent=2)
            click.echo(f"\nResults saved to {output}")
    else:
        click.echo(click.style("✗ Query failed", fg='red'))
        click.echo(f"Error: {result['error']}")
        if 'exception_type' in result:
            click.echo(f"Exception type: {result['exception_type']}")


if __name__ == '__main__':
    cli()
