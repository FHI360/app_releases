import click

@click.command()
@click.option('--names', '-n', default=('John','paul',), multiple=True, help='Enter one or more names')
@click.option(
    '--activitytodelete',
    default=None,
    help="activitytodelete"
)
def greet(names, activitytodelete):
    """Greet one or more people"""
    print(activitytodelete)
    for name in names:
        click.echo(f'Hello, {name}!')

if __name__ == '__main__':
    greet()
