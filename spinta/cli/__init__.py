import click

from spinta.store import Store
from spinta.config import get_config


@click.group()
@click.option('--option', '-o', multiple=True, help='Set configuration option, example: `-o option.name=value`.')
@click.pass_context
def main(ctx, option):
    config = get_config(option)

    store = Store()
    store.add_types()
    store.add_commands()
    store.configure(config)

    ctx.ensure_object(dict)
    ctx.obj['store'] = store


@main.command()
@click.pass_context
def check(ctx):
    click.echo("It works, the check.")


@main.command()
@click.pass_context
def migrate(ctx):
    store = ctx.obj['store']
    store.prepare(internal=True)
    store.migrate(internal=True)
    store.prepare()
    store.migrate()


@main.command(help='Pull data from an external dataset.')
@click.argument('source')
@click.pass_context
def pull(ctx, source):
    store = ctx.obj['store']
    store.prepare(internal=True)
    store.prepare()
    store.pull(source)


@main.command()
@click.pass_context
def run(ctx):
    import spinta.api
    spinta.api.run(ctx.obj['store'])
