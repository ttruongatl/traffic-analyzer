# traffic_analyzer/cli.py

import click
from .commands.collect_traffic import TrafficCollector, PostgresManager
from .commands.kusto_tester import KustoTester
import datetime
from datetime import datetime, time, timedelta
import yaml

@click.group()
@click.version_option()
def cli():
    """A CLI tool to help with analyzing traffic"""
    pass


@cli.command()
@click.option('--component', '-c', required=True, help='Component to collect traffic data for')
@click.option('--days', default=2, help='Number of days to collect data for')
@click.option('--chunk-hours', default=1, help='Number of hours per chunk')
@click.option('--max-workers', default=16, help='Maximum number of parallel workers')
@click.option('--force', is_flag=True, help='Force replace existing data')
def collect(component: str, days: int, chunk_hours: int, max_workers: int, force: bool):
    """Collect traffic data and store in PostgreSQL."""
    click.echo(f"Collecting traffic data for {component}...")
    
    try:
        collector = TrafficCollector(
            days_to_collect=days,
            chunk_hours=chunk_hours,
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

    ingress_rules_set = set()

    for record in results:
        is_ccp = record['isCCP']
        label_key = record['LabelKey']
        label_value = record['LabelValue']
        pod_namespace = record['PodNamespace']
        
        if is_ccp == 'True':
            # CCP component
            namespace_selector = {'matchLabels': {'aks.azure.com/msi-enabled': 'true'}}
        else:
            # Non-CCP component
            namespace_selector = {'matchLabels': {'kubernetes.io/metadata.name': pod_namespace}}
        
        pod_selector = {'matchLabels': {label_key: label_value}}
        
        # Represent the ingress rule as a tuple to be hashable
        ingress_rule_tuple = (
            frozenset(namespace_selector['matchLabels'].items()),
            frozenset(pod_selector['matchLabels'].items())
        )
        
        ingress_rules_set.add(ingress_rule_tuple)

    # Now, create the ingress rules list
    ingress_rules = []
    for ns_labels_items, pod_labels_items in ingress_rules_set:
        namespace_selector = {'matchLabels': dict(ns_labels_items)}
        pod_selector = {'matchLabels': dict(pod_labels_items)}
        ingress_rule = {
            'from': [
                {
                    'namespaceSelector': namespace_selector,
                    'podSelector': pod_selector
                }
            ],
            'ports': [
                {'protocol': 'TCP', 'port': 8081},
                {'protocol': 'TCP', 'port': 9102}
            ]
        }
        ingress_rules.append(ingress_rule)

    # Assemble the NetworkPolicy
    network_policy = {
        'apiVersion': 'networking.k8s.io/v1',
        'kind': 'NetworkPolicy',
        'metadata': {
            'name': 'allow-msi-connector-ingress',
            'namespace': namespace
        },
        'spec': {
            'podSelector': {
                'matchLabels': {
                    'app': 'msi-connector'
                }
            },
            'policyTypes': ['Ingress'],
            'ingress': ingress_rules
        }
    }

    # Write to YAML file
    with open(output, 'w') as f:
        yaml.dump(network_policy, f, default_flow_style=False)

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
