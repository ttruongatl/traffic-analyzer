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
from .commands.network_policy_generator import NetworkPolicyGenerator

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

@cli.command(name="generate-np")
@click.option('--component', '-c', required=True, help='Component to generate NetworkPolicy for')
@click.option('--days', default=30, help='Number of days to aggregate data for')
@click.option('--namespace', '-n', required=True, help='Namespace for the NetworkPolicy')
@click.option('--output', '-o', default='networkpolicy.yaml', help='Output file for the NetworkPolicy YAML')
@click.option('--min-days-seen', default=1, help='Minimum number of days a dependency must be seen to be included')
def generate_np(component: str, days: int, namespace: str, output: str, min_days_seen: int):
    """Generate NetworkPolicy YAML from collected data with enhanced filtering and metadata."""
    end_date = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)
    
    generator = NetworkPolicyGenerator()
    
    try:
        # Fetch and filter data
        records = generator.fetch_dependency_data(component, start_date, end_date)
        filtered_records = [r for r in records if r['days_seen'] >= min_days_seen]
        
        if not filtered_records:
            click.echo(f"No dependencies found meeting the minimum days seen criteria ({min_days_seen} days)")
            return

        # Generate and save NetworkPolicy
        network_policy = generator.generate_network_policy(filtered_records, namespace)
        
        yaml = ruamel.yaml.YAML()
        yaml.preserve_quotes = True
        yaml.indent(mapping=2, sequence=4, offset=2)
        
        with open(output, 'w') as f:
            yaml.dump(network_policy, f)
        
        # Print summary
        click.echo(f"\nNetworkPolicy generated successfully:")
        click.echo(f"- Total dependencies found: {len(records)}")
        click.echo(f"- Dependencies meeting {min_days_seen}-day minimum: {len(filtered_records)}")
        click.echo(f"- Non-CCP dependencies: {len([r for r in filtered_records if not r['is_ccp']])}")
        click.echo(f"- CCP dependencies: {len([r for r in filtered_records if r['is_ccp']])}")
        click.echo(f"- Output written to: {output}")
        
    except Exception as e:
        click.echo(f"Error generating NetworkPolicy: {str(e)}")
        raise

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
