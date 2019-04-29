import json
import operator
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
    c.read(cli_args=option)

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
                'ascii': 'ascii',
            }

            path = None

            if export == 'stdout':
                fmt = 'ascii'
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


@main.command()
@click.pass_context
def config(ctx):
    config = Config()
    config.read()
    for key, value in sorted(config.getall(), key=operator.itemgetter(0)):
        *key, name = key
        name = len(key) * '  ' + name
        click.echo(f'{name:<20} {value}')


@main.command()
@click.argument('path', type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def genkeys(ctx, path):
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.asymmetric import rsa

    from authlib.jose import jwk

    path = pathlib.Path(path)
    private = path / 'private.json'
    public = path / 'public.json'

    if private.exists():
        raise click.Abort(f"{private} file already exists.")

    if public.exists():
        raise click.Abort(f"{public} file already exists.")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

    with private.open('w') as f:
        json.dump(jwk.dumps(key), f, indent=4, ensure_ascii=False)
        click.echo(f"Private key saved to {private}.")

    with public.open('w') as f:
        json.dump(jwk.dumps(key.public_key()), f, indent=4, ensure_ascii=False)
        click.echo(f"Public key saved to {public}.")
