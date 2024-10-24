import click
from .commands.collect_traffic import TrafficCollector


@click.group()
@click.version_option()
def cli():
    """A CLI tool to help with analyzing traffic"""
    pass


@cli.command()
@click.argument('component')
@click.option('--output', '-o', type=click.Path(), help='Output file path (optional)')
def collect(component, output):
    """Collect traffic data for a specific component."""
    click.echo(f"Collecting traffic data for {component}...")

    collector = TrafficCollector()
    results = collector.collect_traffic(component)

    if results:
        # Format and display results
        for row in results:
            label_key = row['LabelKey']
            label_value = row['LabelValue']
            click.echo(f"{row['RowNum']}: {label_key} -> {label_value}")

        if output:
            # Save to file if output path is specified
            import json
            with open(output, 'w') as f:
                json.dump([dict(row) for row in results], f, indent=2)
            click.echo(f"Results saved to {output}")
    else:
        click.echo("No results found or an error occurred.")


if __name__ == '__main__':
    cli()
