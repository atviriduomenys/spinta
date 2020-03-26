from typing import Optional, Iterable

import asyncio
import configparser
import json
import pathlib
import uuid
import logging
import types
import urllib.parse

import click
import tqdm
import requests

from spinta import commands
from spinta import components
from spinta import exceptions
from spinta.auth import AdminToken
from spinta.commands.formats import Format
from spinta.commands.write import write
from spinta.components import Context, DataStream
from spinta.core.context import create_context
from spinta.core.config import KeyFormat
from spinta.utils.aiotools import alist
from spinta.accesslog import create_accesslog

log = logging.getLogger(__name__)


@click.group()
@click.option('--option', '-o', multiple=True, help=(
    "Set configuration option, example: `-o option.name=value`."
))
@click.pass_context
def main(ctx, option):
    ctx.obj = ctx.obj or create_context('cli', args=option)


@main.command()
@click.pass_context
def check(ctx):
    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    config = context.get('config')
    components.load(context, config)
    components.check(context, config)

    components.load(context, ctype='config', group='core')
    components.load(context, ctype='store', group='core')

    click.echo("OK")


@main.command()
@click.pass_context
def freeze(ctx):
    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    with context:
        _require_auth(context)
        commands.freeze(context, store)

    click.echo("Done.")


@main.command(help="Wait while all backends are up.")
@click.argument('seconds', type=int, required=False)
@click.pass_context
def wait(ctx, seconds):
    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    commands.wait(context, store, rc, seconds=seconds)

    click.echo("All backends ar up.")


@main.command()
@click.pass_context
def bootstrap(ctx):
    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    commands.init(context, store)

    with context:
        _require_auth(context)
        backend = store.manifest.backend
        context.attach('transaction', backend.transaction, write=True)
        commands.bootstrap(context, store)


@main.command()
@click.pass_context
def migrate(ctx):
    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    commands.init(context, store)

    with context:
        _require_auth(context)
        backend = store.manifest.backend
        context.attach('transaction', backend.transaction, write=True)
        commands.bootstrap(context, store)
        commands.sync(context, store)
        commands.migrate(context, store)


@main.command(help='Pull data from an external dataset.')
@click.argument('source')
@click.option('--model', '-m', multiple=True, help="Pull only specified models.")
@click.option('--push', is_flag=True, default=False, help="Write pulled data to database.")
@click.option('--export', '-e', help="Export pulled data to a file or stdout. For stdout use 'stdout:<fmt>', where <fmt> can be 'csv' or other supported format.")
@click.pass_context
def pull(ctx, source, model, push, export):
    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    commands.init(context, store)

    manifest = store.manifest
    if source in manifest.objects['dataset']:
        dataset = manifest.objects['dataset'][source]
    else:
        raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='dataset', name=source)))

    try:
        with context:
            _require_auth(context)
            backend = store.backends['default']
            context.attach('transaction', backend.transaction, write=push)

            path = None
            exporter = None

            stream = commands.pull(context, dataset, models=model)
            if push:
                root = manifest.objects['ns']['']
                stream = write(context, root, stream, changed=True)

            if export is None and push is False:
                export = 'stdout'

            if export:
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

            asyncio.get_event_loop().run_until_complete(
                _process_stream(source, stream, exporter, path)
            )

    except exceptions.BaseError as e:
        print()
        raise click.ClickException(str(e))


async def _process_stream(
    source: str,
    stream: DataStream,
    exporter: Optional[Format] = None,
    path: Optional[pathlib.Path] = None,
) -> None:
    if exporter:
        # TODO: Probably exporters should support async generators.
        if isinstance(stream, types.AsyncGeneratorType):
            stream = await alist(stream)
        chunks = exporter(stream)
        if path is None:
            for chunk in chunks:
                print(chunk, end='')
        else:
            with path.open('wb') as f:
                for chunk in chunks:
                    f.write(chunk)
    else:
        with tqdm.tqdm(desc=source, ascii=True) as pbar:
            async for _ in stream:
                pbar.update(1)


@main.command(help='Push data to external data store.')
@click.argument('target')
@click.option('--dataset', '-d', help="Push only specified dataset.")
@click.option('--credentials', '-r', help="Credentials file.")
@click.option('--client', '-c', help="Client name from credentials file.")
@click.pass_context
def push(ctx, target, dataset, credentials, client):
    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    commands.init(context, store)

    manifest = store.manifest
    if dataset and dataset not in manifest.objects['dataset']:
        raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset)))

    ns = manifest.objects['ns']['']

    with context:
        _require_auth(context)
        token = _get_access_token(credentials, client, target)
        headers = {
            'Content-Type': 'application/x-ndjson',
            'Authorization': f'Bearer {token}',
        }
        context.attach('transaction', manifest.backend.transaction)
        stream = _read_data(context, ns, dataset)
        resp = requests.post(target, headers=headers, data=stream)
        if resp.status_code >= 400:
            click.echo(resp.text)
            raise click.Abort("Error while pushing data.")


def _get_access_token(credsfile, client, url):
    url = urllib.parse.urlparse(url)
    section = f'{client}@{url.hostname}'
    creds = configparser.ConfigParser()
    creds.read(credsfile)
    auth = (
        creds.get(section, 'client_id'),
        creds.get(section, 'client_secret'),
    )
    resp = requests.post(f'{url.scheme}://{url.netloc}/auth/token', auth=auth, data={
        'grant_type': 'client_credentials',
        'scope': creds.get(section, 'scopes'),
    })
    if resp.status_code >= 400:
        click.echo(resp.text)
        raise click.Abort("Can't get access token.")
    return resp.json()['access_token']


def _read_data(
    context: components.Context,
    ns: components.Namespace,
    dataset: Optional[str] = None,
) -> Iterable[dict]:
    stream = commands.getall(
        context, ns, None,
        action=components.Action.GETALL,
        dataset_=dataset,
    )
    for data in stream:
        id_ = data['_id']
        type_ = data['_type']
        click.echo(f'{type_:42}  {id_}')
        payload = {
            '_op': 'upsert',
            '_type': type_,
            '_id': id_,
            '_where': f'_id="{id_}"',
            **{k: v for k, v in data.items() if not k.startswith('_')}
        }
        yield json.dumps(payload).encode('utf-8') + b'\n'


@main.command()
@click.pass_context
def run(ctx):
    import uvicorn
    import spinta.api

    context = ctx.obj

    rc = context.get('rc')
    config = context.get('config')
    commands.load(context, config, rc)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store, config)
    commands.check(context, store)

    commands.init(context, store)

    app = spinta.api.init(context)

    log.info("Spinta has started!")
    uvicorn.run(app, host='0.0.0.0', port=8000)


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
@click.option('--path', '-p', type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def genkeys(ctx, path):
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.asymmetric import rsa

    from authlib.jose import jwk

    context = ctx.obj
    with context:
        _require_auth(context)

        if path is None:
            context = ctx.obj

            rc = context.get('rc')
            config = context.get('config')
            commands.load(context, config, rc)

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
@click.option('--name', '-n', help="client name")
@click.option('--secret', '-s', help="client secret")
@click.option('--add-secret', is_flag=True, default=False, help="add client secret in plain text to file")
@click.option('--path', '-p', type=click.Path(exists=True, file_okay=False, writable=True))
@click.pass_context
def client_add(ctx, name, secret, add_secret, path):
    import ruamel.yaml
    from spinta.utils import passwords

    context = ctx.obj
    with context:
        _require_auth(context)

        yaml = ruamel.yaml.YAML(typ='safe')

        if path is None:
            context = ctx.obj

            rc = context.get('rc')
            config = context.get('config')
            commands.load(context, config, rc)

            path = config.config_path / 'clients'
            path.mkdir(exist_ok=True)
        else:
            path = pathlib.Path(path)

        client_id = name or str(uuid.uuid4())
        client_file = path / f'{client_id}.yml'

        if client is None and client_file.exists():
            raise click.Abort(f"{client_file} file already exists.")

        client_secret = secret or passwords.gensecret(32)
        client_secret_hash = passwords.crypt(client_secret)

        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'client_secret_hash': client_secret_hash,
            'scopes': [],
        }

        if not add_secret:
            del data['client_secret']

        yaml.dump(data, client_file)

        click.echo(
            f"New client created and saved to:\n\n"
            f"    {client_file}\n\n"
            f"Client secret:\n\n"
            f"    {client_secret}\n\n"
            f"Remember this client secret, because only a secure hash of\n"
            f"client secret will be stored in the config file."
        )


def _require_auth(context: Context):
    # TODO: probably commands should also use an exsiting token in order to
    #       track who changed what.
    context.set('auth.token', AdminToken())
    context.attach('accesslog', create_accesslog, context, loaders=(
        context.get('store'),
        context.get("auth.token"),
    ))
