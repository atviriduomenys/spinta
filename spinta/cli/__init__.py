import pathlib

import click

from spinta.store import Store, get_model_from_params
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
    click.echo("OK")


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
@click.option('--model', '-m', multiple=True, help="Pull only specified modles.")
@click.option('--push', is_flag=True, default=False, help="Write pulled data to database.")
@click.option('--export', '-e', help="Export pulled data to a file or stdout. For stdout use 'stdout:<fmt>', where <fmt> can be 'csv' or other supported format.")
@click.pass_context
def pull(ctx, source, model, push, export):
    store = ctx.obj['store']
    store.prepare(internal=True)
    store.prepare()

    result = store.pull(source, {'models': model, 'push': push})

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
    spinta.api.run(ctx.obj['store'])
