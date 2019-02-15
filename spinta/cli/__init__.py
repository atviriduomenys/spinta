import pathlib

import click

from spinta.store import Store
from spinta.utils.cli import update_config_from_cli


@click.group()
@click.option('--option', '-o', multiple=True, help='Set configuration option, example: `-o option.name=value`.')
@click.pass_context
def main(ctx, option):
    config = {
        'backends': {
            'default': {
                'type': 'postgresql',
                'dsn': 'postgresql:///spinta',
            },
        },
        'manifests': {
            'default': {
                'path': pathlib.Path(),
            },
        },
        'ignore': [
            '.travis.yml',
            '/prefixes.yml',
            '/schema/',
            '/env/',
        ],
    }
    custom = {
        ('backends',): 'default',
        ('manifests',): 'default',
    }

    update_config_from_cli(config, custom, option)

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
