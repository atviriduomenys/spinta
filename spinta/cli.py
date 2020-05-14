from typing import Optional, Iterable, List

import asyncio
import configparser
import json
import pathlib
import uuid
import logging
import types
import sys
import urllib.parse

import click
import tqdm
import requests
from authlib.jose import jwt

from spinta import spyna
from spinta import commands
from spinta import components
from spinta import exceptions
from spinta.auth import AdminToken
from spinta.auth import KeyType
from spinta.auth import load_key
from spinta.auth import gen_auth_server_keys
from spinta.auth import KeyFileExists
from spinta.auth import create_client_file
from spinta.commands.formats import Format
from spinta.commands.write import write
from spinta.components import Context, DataStream, Model
from spinta.manifests.components import Manifest
from spinta.datasets.components import Dataset
from spinta.datasets.components import ExternalBackend
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

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.load(context, store.internal, into=store.manifest)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)

    click.echo("OK")


@main.command()
@click.pass_context
def freeze(ctx):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    # Load just store, manifests will be loaded by freeze command.
    store = context.get('store')
    commands.load(context, store)

    with context:
        _require_auth(context)
        commands.freeze(context, store.manifest)

    click.echo("Done.")


@main.command(help="Wait while all backends are up.")
@click.argument('seconds', type=int, required=False)
@click.pass_context
def wait(ctx, seconds):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.wait(context, store, seconds=seconds)

    click.echo("All backends ar up.")


@main.command()
@click.pass_context
def bootstrap(ctx):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)
    with context:
        _require_auth(context)
        commands.bootstrap(context, store.manifest)


@main.command()
@click.pass_context
def sync(ctx):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.load(context, store.internal, into=store.manifest)
    with context:
        _require_auth(context)
        coro = commands.sync(context, store.manifest)
        asyncio.run(coro)

    click.echo("Done.")


@main.command()
@click.pass_context
def migrate(ctx):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)

    with context:
        _require_auth(context)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(commands.migrate(context, store.manifest))


@main.command(help='Pull data from an external dataset.')
@click.argument('dataset')
@click.option('--model', '-m', multiple=True, help="Pull only specified models.")
@click.option('--push', is_flag=True, default=False, help="Write pulled data to database.")
@click.option('--export', '-e', help="Export pulled data to a file or stdout. For stdout use 'stdout:<fmt>', where <fmt> can be 'csv' or other supported format.")
@click.pass_context
def pull(ctx, dataset, model, push, export):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.link(context, store)
    commands.check(context, store)

    commands.prepare(context, store)

    manifest = store.manifest
    if dataset in manifest.objects['dataset']:
        dataset = manifest.objects['dataset'][dataset]
    else:
        raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset)))

    if model:
        models = []
        for model in model:
            if model not in manifest.models:
                raise click.ClickException(str(exceptions.NodeNotFound(manifest, type='model', name=model)))
            models.append(manifest.models[model])
    else:
        models = _get_dataset_models(manifest, dataset)

    try:
        with context:
            _require_auth(context)
            backend = store.backends['default']
            context.attach('transaction', backend.transaction, write=push)

            path = None
            exporter = None

            stream = _pull_models(context, models)
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
                _process_stream(dataset.name, stream, exporter, path)
            )

    except exceptions.BaseError as e:
        print()
        raise click.ClickException(str(e))


def _get_dataset_models(manifest: Manifest, dataset: Dataset):
    for model in manifest.models.values():
        if model.external and model.external.dataset and model.external.dataset.name == dataset.name:
            yield model


def _pull_models(context: Context, models: List[Model]):
    for model in models:
        external = model.external
        backend = external.resource.backend
        rows = commands.getall(context, external, backend)
        yield from rows


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

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)

    if credentials:
        credentials = pathlib.Path(credentials)
        if not credentials.exists():
            raise click.Abort(f"Credentials file {credentials} does not exit.")

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
        for backend in store.backends.values():
            context.attach(f'transaction.{backend.name}', backend.begin)
        stream = _read_data(context, ns, dataset)
        resp = requests.post(target, headers=headers, data=stream)
        if resp.status_code >= 400:
            click.echo(resp.text)
            raise click.Abort("Error while pushing data.")


def _get_access_token(credsfile: pathlib.Path, client, url) -> str:
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
    manifest = context.get('store').manifest
    for data in stream:
        _id = data['_id']
        _type = data['_type']
        click.echo(f'{_type:42}  {_id}')
        model = manifest.models[_type]

        if isinstance(model.backend, ExternalBackend):
            where = []
            for prop in model.external.pkeys:
                where.append({
                    'name': 'eq',
                    'args': [
                        {'name': 'bind', 'args': [prop.name]},
                        data[prop.name],
                    ]
                })

            if len(where) > 1:
                where = {'name': 'and', 'args': where}
            elif len(where) == 1:
                where = where[0]
            else:
                raise Exception(f"Model {model} does not have `external.pkeys`.")

            payload = {
                '_op': 'upsert',
                '_type': _type,
                '_where': spyna.unparse(where),
                **{k: v for k, v in data.items() if not k.startswith('_')}
            }

        else:
            where = {
                'name': 'eq',
                'args': [
                    {'name': 'bind', 'args': ['_id']},
                    _id,
                ]
            }
            payload = {
                '_op': 'upsert',
                '_type': _type,
                '_id': _id,
                '_where': spyna.unparse(where),
                **{k: v for k, v in data.items() if not k.startswith('_')}
            }

        yield json.dumps(payload).encode('utf-8') + b'\n'


@main.command()
@click.pass_context
def run(ctx):
    import uvicorn
    import spinta.api

    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    commands.link(context, store)
    commands.check(context, store)

    commands.prepare(context, store)

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
    context = ctx.obj
    with context:
        _require_auth(context)

        if path is None:
            context = ctx.obj
            config = context.get('config')
            commands.load(context, config)
            path = config.config_path
        else:
            path = pathlib.Path(path)

        try:
            prv, pub = gen_auth_server_keys(path)
        except KeyFileExists as e:
            raise click.Abort(str(e))

        click.echo(f"Private key saved to {prv}.")
        click.echo(f"Public key saved to {pub}.")


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
    context = ctx.obj
    with context:
        _require_auth(context)

        if path is None:
            context = ctx.obj

            config = context.get('config')
            commands.load(context, config)

            path = config.config_path / 'clients'
            path.mkdir(exist_ok=True)
        else:
            path = pathlib.Path(path)

        name = name or str(uuid.uuid4())

        client_file, client = create_client_file(
            path,
            name,
            secret,
            add_secret=add_secret,
        )

        client_secret = client['client_secret']
        click.echo(
            f"New client created and saved to:\n\n"
            f"    {client_file}\n\n"
            f"Client secret:\n\n"
            f"    {client_secret}\n\n"
            f"Remember this client secret, because only a secure hash of\n"
            f"client secret will be stored in the config file."
        )


@main.command('decode-token')
@click.pass_context
def decode_token(ctx):
    context = ctx.obj
    config = context.get('config')
    commands.load(context, config)
    key = load_key(context, KeyType.public)
    token = sys.stdin.read().strip()
    token = jwt.decode(token, key)
    click.echo(json.dumps(token, indent='  '))


def _require_auth(context: Context):
    # TODO: probably commands should also use an exsiting token in order to
    #       track who changed what.
    context.set('auth.token', AdminToken())
    context.attach('accesslog', create_accesslog, context, loaders=(
        context.get('store'),
        context.get("auth.token"),
    ))
