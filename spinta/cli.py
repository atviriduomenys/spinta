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
import itertools

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
from spinta.auth import create_client_access_token
from spinta.auth import Token
from spinta.auth import BearerTokenValidator
from spinta.core.ufuncs import Expr
from spinta.commands.formats import Format
from spinta.commands.write import write
from spinta.components import Context, DataStream, Action, Model
from spinta.manifests.components import Manifest
from spinta.datasets.components import Dataset
from spinta.core.context import create_context
from spinta.core.config import KeyFormat
from spinta.utils.aiotools import alist
from spinta.utils.json import fix_data_for_json
from spinta.accesslog import create_accesslog
from spinta.types.namespace import sort_models_by_refs

log = logging.getLogger(__name__)


@click.group()
@click.option('--option', '-o', multiple=True, help=(
    "Set configuration option, example: `-o option.name=value`."
))
@click.option('--env-file', help="Load configuration from a given .env file.")
@click.pass_context
def main(ctx, option, env_file):
    ctx.obj = ctx.obj or create_context('cli', args=option, envfile=env_file)


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
@click.option('--auth', '-a', help="Authorize as client.")
@click.option('--limit', type=int, help="Limit number of rows read from each model.")
@click.pass_context
def push(ctx, target, dataset, credentials, client, auth, limit):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    click.echo(f"Loading manifest {store.manifest.name}...")
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
        _require_auth(context, auth)
        click.echo(f"Get access token from {target}")
        token = _get_access_token(credentials, client, target)
        headers = {
            'Content-Type': 'application/x-ndjson',
            'Authorization': f'Bearer {token}',
        }
        context.attach('transaction', manifest.backend.transaction)
        for backend in store.backends.values():
            context.attach(f'transaction.{backend.name}', backend.begin)
        for keymap in store.keymaps.values():
            context.attach(f'keymap.{keymap.name}', lambda: keymap)

        from spinta.types.namespace import traverse_ns_models

        models = traverse_ns_models(context, ns, Action.SEARCH, dataset)
        models = sort_models_by_refs(models)
        models = list(reversed(list(models)))

        counts = {}
        for model in tqdm.tqdm(models, 'Count rows', leave=False):
            try:
                counts[model.name] = _get_row_count(context, model)
            except Exception:
                log.exception("Error on _get_row_count({model.name}).")

        if limit is None:
            total = sum(counts.values())
        else:
            total = sum(min(c, limit) for c in counts.values())
        pbar = tqdm.tqdm(desc='PUSH', total=total)

        for i, model in enumerate(models, 1):
            try:
                stream = _read_model_data(
                    context,
                    model,
                    limit=limit,
                    count=counts.get(model.name),
                    pbar=pbar,
                )
                resp = requests.post(target, headers=headers, data=stream)
                resp.raise_for_status()
            except Exception:
                log.exception("Erro on push for {model.name}.")


def _get_access_token(credsfile: pathlib.Path, client, url) -> str:
    url = urllib.parse.urlparse(url)
    section = f'{client}@{url.hostname}'
    if url.port:
        section += f':{url.port}'
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


def _get_row_count(
    context: components.Context,
    model: components.Model,
) -> int:
    stream = commands.getall(context, model, model.backend, query=Expr('count'))
    for data in stream:
        return data['count()']


def _read_model_data(
    context: components.Context,
    model: components.Model,
    *,
    count: int = None,
    limit: int = None,
    pbar=None,
) -> Iterable[dict]:

    if limit is None:
        query = None
    else:
        query = Expr('limit', limit)
        count = min(count, limit) if count else count

    stream = commands.getall(context, model, model.backend, query=query)

    if pbar is not None:
        stream = tqdm.tqdm(stream, model.name, total=count, leave=False)

    for data in stream:
        _id = data['_id']
        _type = data['_type']
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
        payload = fix_data_for_json(payload)
        yield json.dumps(payload).encode('utf-8') + b'\n'

        if pbar is not None:
            pbar.update(1)


@main.command('import', help='Import data from a file.')
@click.argument('source')
@click.option('--auth', '-a', help="Authorize as client.")
@click.option('--limit', type=int, help="Limit number of rows read from source.")
@click.pass_context
def import_(ctx, source, auth, limit):
    context = ctx.obj

    config = context.get('config')
    commands.load(context, config)
    commands.check(context, config)

    store = context.get('store')
    commands.load(context, store)
    click.echo(f"Loading manifest {store.manifest.name}...")
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)

    manifest = store.manifest
    root = manifest.objects['ns']['']

    with context:
        _require_auth(context)
        context.attach('transaction', manifest.backend.transaction, write=True)
        with open(source) as f:
            stream = (json.loads(line.strip()) for line in f)
            stream = itertools.islice(stream, limit) if limit else stream
            stream = write(context, root, stream, changed=True)
            coro = _process_stream(source, stream)
            asyncio.get_event_loop().run_until_complete(coro)


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


def _require_auth(context: Context, client: str = None):
    # TODO: probably commands should also use an exsiting token in order to
    #       track who changed what.
    if client is None:
        token = AdminToken()
    else:
        if client == 'default':
            config = context.get('config')
            client = config.default_auth_client
        token = create_client_access_token(context, client)
        token = Token(token, BearerTokenValidator(context))
    context.set('auth.token', token)
    context.attach('accesslog', create_accesslog, context, loaders=(
        context.get('store'),
        context.get("auth.token"),
    ))
