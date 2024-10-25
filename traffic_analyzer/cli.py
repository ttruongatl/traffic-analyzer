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
@click.option('--output', '-o', type=click.Path(), help='Output file path (optional)')
@click.option('--days', default=2, help='Number of days to collect data for')
@click.option('--chunk-hours', default=1, help='Number of hours per chunk')
@click.option('--max-workers', default=16, help='Maximum number of parallel workers')
def collect(component, output, days, chunk_hours, max_workers):
    """Collect traffic data for a specific component."""
    click.echo(f"Collecting traffic data for {component}...")
    
    collector = TrafficCollector(days_to_collect=days, chunk_hours=chunk_hours, max_workers=max_workers)
    result = collector.collect_traffic(component)
    
    if result.get('results'):
        # Print results
        for row in result['results']:
            label_key = row['LabelKey']
            label_value = row['LabelValue']
            click.echo(f"{row['RowNum']}: {label_key} -> {label_value}")
        
        if output:
            # Save full results including execution info to file
            import json
            with open(output, 'w') as f:
                json.dump(result, f, indent=2)
            click.echo(f"Results saved to {output}")
    else:
        click.echo("No results found or an error occurred.")
        
    # Always show execution info
    execution_info = result.get('execution_info', {})
    if execution_info:
        click.echo("\nExecution Summary:")
        click.echo("-" * 40)
        click.echo(f"Total Chunks: {execution_info.get('total_chunks', 0)}")
        click.echo(f"Successful Chunks: {execution_info.get('successful_chunks', 0)}")
        click.echo(f"Failed Chunks: {execution_info.get('failed_chunks', 0)}")
        click.echo(f"Total Results: {execution_info.get('total_results', 0)}")
        
        if execution_info.get('errors'):
            click.echo("\nError Summary:")
            click.echo("-" * 40)
            for i, error in enumerate(execution_info['errors'], 1):
                click.echo(f"\nError {i}:")
                click.echo(f"Type: {error.get('error_type', 'Unknown')}")
                click.echo(f"Message: {error.get('error_message', 'No message')}")
                click.echo(f"Chunk: {error.get('chunk_info', {})}")


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
