import os
import pathlib

import click

from spinta.config import Config
from spinta.utils.commands import load_commands
from spinta.components import Context, Store
from spinta import commands
from spinta import components


@click.group()
@click.option('--option', '-o', multiple=True, help='Set configuration option, example: `-o option.name=value`.')
@click.pass_context
def main(ctx, option):
    c = Config()
    c.set_env(os.environ)
    c.add_env_file('.env')
    c.add_cli_args(option)

    load_commands(c.get('commands', 'modules', cast=list))

    context = Context()
    config = context.set('config', components.Config())
    store = context.set('store', Store())

    commands.load(context, config, c)
    commands.load(context, store, c)
    commands.check(context, store)

    ctx.ensure_object(dict)
    ctx.obj['context'] = context


@main.command()
@click.pass_context
def check(ctx):
    click.echo("OK")


@main.command()
@click.pass_context
def migrate(ctx):
    context = ctx.obj['context']
    store = context.get('store')

    commands.prepare(context, store.internal)
    commands.migrate(context, store)
    commands.prepare(context, store)
    commands.migrate(context, store)


@main.command(help='Pull data from an external dataset.')
@click.argument('source')
@click.option('--model', '-m', multiple=True, help="Pull only specified modles.")
@click.option('--push', is_flag=True, default=False, help="Write pulled data to database.")
@click.option('--export', '-e', help="Export pulled data to a file or stdout. For stdout use 'stdout:<fmt>', where <fmt> can be 'csv' or other supported format.")
@click.pass_context
def pull(ctx, source, model, push, export):
    context = ctx.obj['context']
    store = context.get('store')

    commands.prepare(context, store.internal)
    commands.prepare(context, store)

    dataset = store.manifests['default'].objects['dataset'][source]

    with context.enter():
        context.bind('transaction', store.backends['default'].transaction, write=push)

        result = commands.pull(context, dataset, models=model)
        result = commands.push(context, store, result) if push else result

        if export is None and push is False:
            export = 'stdout'

        if export:
            formats = {
                'csv': 'csv',
                'json': 'json',
                'jsonl': 'jsonl',
                'asciitable': 'asciitable',
            }

            path = None

            if export == 'stdout':
                fmt = 'asciitable'
            elif export.startswith('stdout:'):
                fmt = export.split(':', 1)[1]
            else:
                path = pathlib.Path(export)
                fmt = export.suffix.strip('.')

            if fmt not in formats:
                raise click.UsageError(f"unknown export file type {fmt!r}")

            chunks = store.export(result, fmt)

            if path is None:
                for chunk in chunks:
                    print(chunk, end='')
            else:
                with export.open('wb') as f:
                    for chunk in chunks:
                        f.write(chunk)


@main.command()
@click.pass_context
def run(ctx):
    import spinta.api
    spinta.api.run(ctx.obj['context'])
