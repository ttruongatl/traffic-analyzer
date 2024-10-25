# traffic_analyzer/cli.py

import click
from .commands.collect_traffic import TrafficCollector
from .commands.kusto_tester import KustoTester


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
