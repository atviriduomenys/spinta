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

from spinta.components import Store, DataStream
from spinta import commands
from spinta import components
from spinta.auth import AdminToken
from spinta.config import create_context
from spinta import exceptions
from spinta.commands.write import push_stream, dataitem_from_payload
from spinta.commands.formats import Format
from spinta.utils.aiotools import alist, aiter
from spinta.auth import ResourceProtector, BearerTokenValidator

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

    if not context.has('auth.token'):
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

            path = None
            exporter = None

            stream = commands.pull(context, dataset, models=model)
            if push:
                stream = push_stream(context, aiter(
                    dataitem_from_payload(context, dataset, data)
                    for data in stream
                ))

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

            asyncio.run(
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
    context = ctx.obj['context']
    store = context.get('store')

    if context.get('config').env != 'test':
        commands.prepare(context, store.internal)
        commands.prepare(context, store)

    if not context.has('auth.token'):
        # TODO: probably commands should also use an exsiting token in order to
        #       track who changed what.
        context.set('auth.token', AdminToken())
        context.set('auth.resource_protector', ResourceProtector(context, BearerTokenValidator))

    manifest = store.manifests['default']
    if dataset and dataset not in manifest.objects['dataset']:
        raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset)))

    ns = manifest.objects['ns']['']

    with context:
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
            '_where': f'_id={id_}',
            **{k: v for k, v in data.items() if not k.startswith('_')}
        }
        yield json.dumps(payload).encode('utf-8') + b'\n'


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
