import click

from spinta.cli import main
from spinta.cli.helpers.store import prepare_manifest
from spinta.core.config import KeyFormat


@main.command()
@click.pass_context
@click.argument('name', nargs=-1, required=False)
@click.option('-f', '--format', 'fmt', default='cfg', help=(
    'Configuration option name format, possible values: cfg, cli, env.'
))
def config(ctx, name=None, fmt='cfg'):
    context = ctx.obj
    rc = context.get('rc')
    rc.dump(*name, fmt=KeyFormat[fmt.upper()])


@main.command()
@click.pass_context
def check(ctx):
    context = ctx.obj
    prepare_manifest(context)
    click.echo("OK")
