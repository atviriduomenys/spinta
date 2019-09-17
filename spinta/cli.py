import json
import pathlib
import uuid
import logging

import click
import tqdm

from spinta.components import Store
from spinta import commands
from spinta import components
from spinta.utils.itertools import consume
from spinta.auth import AdminToken
from spinta.config import create_context
from spinta import exceptions

log = logging.getLogger(__name__)


@click.group()
@click.option('--option', '-o', multiple=True, help='Set configuration option, example: `-o option.name=value`.')
@click.pass_context
def main(ctx, option):
    context = create_context(cli_args=option)
    rc = context.get('config.raw')
    _load_context(context, rc)
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
@click.option('--model', '-m', multiple=True, help="Pull only specified models.")
@click.option('--push', is_flag=True, default=False, help="Write pulled data to database.")
@click.option('--export', '-e', help="Export pulled data to a file or stdout. For stdout use 'stdout:<fmt>', where <fmt> can be 'csv' or other supported format.")
@click.pass_context
def pull(ctx, source, model, push, export):
    context = ctx.obj['context']
    store = context.get('store')

    if context.get('config').env != 'test':
        commands.prepare(context, store.internal)
        commands.prepare(context, store)

        # TODO: probably commands should also use an exsiting token in order to
        #       track who changed what.
        context.set('auth.token', AdminToken())

    manifest = store.manifests['default']
    if source in manifest.objects['dataset']:
        dataset = manifest.objects['dataset'][source]
    else:
        raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='dataset', name=source)))

    try:
        with context:
            context.attach('transaction', store.backends['default'].transaction, write=push)

            rows = commands.pull(context, dataset, models=model)
            rows = commands.push(context, store, rows) if push else rows

            if export is None and push is False:
                export = 'stdout'

            if export:
                path = None

                if export == 'stdout':
                    fmt = 'ascii'
                elif export.startswith('stdout:'):
                    fmt = export.split(':', 1)[1]
                else:
                    path = pathlib.Path(export)
                    fmt = export.suffix.strip('.')

                config = context.get('config')

                if fmt not in config.exporters:
                    raise click.UsageError(f"unknown export file type {fmt!r}")

                exporter = config.exporters[fmt]
                chunks = exporter(rows)

                if path is None:
                    for chunk in chunks:
                        print(chunk, end='')
                else:
                    with export.open('wb') as f:
                        for chunk in chunks:
                            f.write(chunk)

            else:
                consume(tqdm.tqdm(rows, desc=source))
    except exceptions.BaseError as e:
        print()
        raise click.ClickException(str(e))


@main.command()
@click.pass_context
def run(ctx):
    import uvicorn
    import spinta.api

    context = ctx.obj['context']
    spinta.api.context_var.set(context)
    store = context.get('store')
    manifest = store.manifests['default']
    log.info("Spinta has started!")
    log.info('manifest: %s', manifest.path.resolve())
    spinta.api.app.debug = context.get('config').debug
    uvicorn.run(spinta.api.app, host='0.0.0.0', port=8000)


@main.command()
@click.pass_context
@click.argument('name', nargs=-1, required=False)
def config(ctx, name=None):
    context = ctx.obj['context']
    config = context.get('config.raw')
    config.dump(name)


@main.command()
@click.option('--path', '-p', type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def genkeys(ctx, path):
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.asymmetric import rsa

    from authlib.jose import jwk

    if path is None:
        context = ctx.obj['context']
        config = context.get('config')
        path = config.config_path / 'keys'
        path.mkdir(exist_ok=True)
    else:
        path = pathlib.Path(path)

    private_file = path / 'private.json'
    public_file = path / 'public.json'

    if private_file.exists():
        raise click.Abort(f"{private_file} file already exists.")

    if public_file.exists():
        raise click.Abort(f"{public_file} file already exists.")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

    with private_file.open('w') as f:
        json.dump(jwk.dumps(key), f, indent=4, ensure_ascii=False)
        click.echo(f"Private key saved to {private_file}.")

    with public_file.open('w') as f:
        json.dump(jwk.dumps(key.public_key()), f, indent=4, ensure_ascii=False)
        click.echo(f"Public key saved to {public_file}.")


@main.group()
@click.pass_context
def client(ctx):
    pass


@client.command('add')
@click.option('--path', '-p', type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def client_add(ctx, path):
    import ruamel.yaml
    from spinta.utils import passwords

    yaml = ruamel.yaml.YAML(typ='safe')

    if path is None:
        context = ctx.obj['context']
        config = context.get('config')
        path = config.config_path / 'clients'
        path.mkdir(exist_ok=True)
    else:
        path = pathlib.Path(path)

    client_id = str(uuid.uuid4())
    client_file = path / f'{client_id}.yml'

    if client is None and client_file.exists():
        raise click.Abort(f"{client_file} file already exists.")

    client_secret = passwords.gensecret(32)
    client_secret_hash = passwords.crypt(client_secret)

    data = {
        'client_id': client_id,
        'client_secret_hash': client_secret_hash,
        'scopes': [],
    }

    yaml.dump(data, client_file)

    click.echo(
        f"New client created and saved to:\n\n"
        f"    {client_file}\n\n"
        f"Client secret:\n\n"
        f"    {client_secret}\n\n"
        f"Remember this client secret, because only a secure hash of\n"
        f"client secret will be stored in the config file."
    )


def _load_context(context, rc):
    config = context.set('config', components.Config())
    store = context.set('store', Store())
    commands.load(context, config, rc)
    commands.check(context, config)
    commands.load(context, store, rc)
    commands.check(context, store)
