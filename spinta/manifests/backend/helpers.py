import collections
import datetime
import itertools
from typing import AsyncIterator
from typing import Iterable
from typing import Iterator
from typing import Tuple
from typing import cast

from toposort import toposort

from spinta import commands
from spinta.commands.write import push_stream
from spinta.commands.write import write
from spinta.components import Action
from spinta.components import Context
from spinta.components import DataItem
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import bind
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.components import Manifest
from spinta.utils.aiotools import adrain
from spinta.utils.aiotools import aiter
from spinta.utils.itertools import last
from spinta.utils.json import fix_data_for_json


async def run_bootstrap(context: Context, manifest: BackendManifest):
    # Bootstrap all backends
    for backend in manifest.store.backends.values():
        commands.bootstrap(context, backend)

    # Sync versions
    stream = read_sync_versions(context, manifest)
    stream = versions_to_dstream(manifest, stream, applied=True)
    await adrain(push_stream(context, stream))

    # Update schemas to last version
    for schema in read_lastest_version_schemas(context, manifest):
        await update_schema_version(context, manifest, schema)


async def run_migrations(context: Context, manifest: BackendManifest):
    # Sync versions
    stream = read_sync_versions(context, manifest)
    stream = versions_to_dstream(manifest, stream)
    await adrain(push_stream(context, stream))

    # Apply unapplied versions
    store = manifest.store
    model = manifest.objects['ns']['']
    backends = {}
    versions = read_unapplied_versions(context, manifest)
    versions = itertools.groupby(versions, key=lambda v: v.get('backend', 'default'))
    for name, group in versions:
        if name not in backends:
            backend = store.backends[name]
            context.attach(f'transaction.{backend.name}', backend.begin)
            backends[backend] = commands.migrate(context, manifest, backend)
        else:
            backend = None
        execute = backends[backend]
        async for version in execute(group):
            schema = version['schema']
            assert schema['version'] == version['id'], (
                schema['version'],
                version['id'],
            )
            await write(context, model, [
                {
                    '_op': 'patch',
                    '_type': '_schema/version',
                    '_where': '_id="%s"' % version['id'],
                    'applied': datetime.datetime.now(datetime.timezone.utc),
                },
                {
                    '_op': 'upsert',
                    '_type': '_schema',
                    '_where': '_id="%s"' % schema['id'],
                    '_id': schema['id'],
                    'type': schema['type'],
                    'name': schema['name'],
                    'version': schema['version'],
                    'schema': fix_data_for_json(schema),
                },
            ])


def read_unapplied_versions(
    context: Context,
    manifest: Manifest,
):
    model = manifest.objects['model']['_schema/version']
    query = Expr(
        'and',
        Expr('select', bind('id'), bind('_id'), bind('parents')),
        Expr('ne', bind('applied'), None),
    )
    versions = {}
    for row in commands.getall(context, model, model.backend, query=query):
        versions[row['_id']] = set(row['parents'])
    for group in toposort(versions):
        for vid in sorted(group):
            yield get_version_schema(context, manifest, vid)


def read_sync_versions(context: Context, manifest: Manifest):
    sources = [cast(Manifest, manifest.store.internal)] + manifest.sync
    for source in sources:
        for eid in commands.manifest_list_schemas(context, source):
            for version in commands.manifest_read_versions(context, source, eid=eid):
                yield version


async def versions_to_dstream(
    manifest: BackendManifest,
    versions: Iterable[dict],
    *,
    applied: bool = False,
) -> AsyncIterator[DataItem]:
    now = datetime.datetime.now(datetime.timezone.utc)
    model = manifest.objects['model']['_schema/version']
    for version in versions:
        payload = {
            '_op': 'upsert',
            '_where': '_id="%s"' % version['id'],
            '_id': version['id'],
            'type': version['schema']['type'],
            'name': version['schema']['name'],
            'id': version['schema']['id'],
            'created': version['date'],
            'synced': now,
            'applied': None,
            'parents': version['parents'],
            'schema': fix_data_for_json(version['schema']),
            'migrate': version['migrate'],
            'changes': fix_data_for_json(version['changes']),
        }
        if applied:
            payload['applied'] = now
        yield DataItem(model, action=Action.UPSERT, payload=payload)


def list_schemas(context: Context, manifest: BackendManifest):
    model = manifest.objects['model']['_schema']
    query = {
        'select': ['_id'],
    }
    for row in commands.getall(context, model, model.backend, **query):
        yield row['_id']


def read_schema(context: Context, manifest: BackendManifest, eid: str):
    model = manifest.objects['model']['_schema']
    row = commands.getone(context, model, model.backend, id_=eid)
    return row['schema']


def list_sorted_unapplied_versions(
    context: Context,
    manifest: Manifest,
) -> Iterator[Tuple[str, str]]:
    model = manifest.objects['model']['_schema/version']
    query = {
        'select': ['id', '_id', 'parents'],
        'query': [
            {'name': 'eq', 'args': ['applied', None]}
        ],
    }
    schemas = {}
    versions = {}
    for row in commands.getall(context, model, model.backend, **query):
        schemas[row['_id']] = row['id']
        versions[row['_id']] = row['parents']

    for group in toposort(versions):
        for vid in sorted(group):
            yield schemas[vid], vid


def read_lastest_version_schemas(
    context: Context,
    manifest: Manifest,
) -> Iterator[Tuple[str, str]]:
    model = manifest.objects['model']['_schema/version']
    query = Expr(
        'and',
        Expr('select', bind('id'), bind('_id'), bind('parents')),
        Expr('ne', bind('applied'), None),
    )
    schemas = collections.defaultdict(dict)
    for row in commands.getall(context, model, model.backend, query=query):
        schemas[row['id']][row['_id']] = set(row['parents'])

    for schema_id, versions in schemas.items():
        last_version = last(toposort(versions))
        assert len(last_version) == 1, last_version
        last_version = next(iter(last_version))
        yield get_version_schema(context, manifest, last_version)


def get_last_version_eid(
    context: Context,
    manifest: Manifest,
    schema_eid: str,
) -> Iterator[Tuple[str, str]]:
    model = manifest.objects['model']['_schema/version']
    query = Expr(
        'and',
        Expr('select', bind('_id'), bind('parents')),
        Expr('eq', bind('id'), schema_eid),
        Expr('ne', bind('applied'), None),
    )
    versions = {}
    for row in commands.getall(context, model, model.backend, query=query):
        versions[row['_id']] = row['parents']
    last_version = last(toposort(versions))
    assert len(last_version) == 1, last_version
    return last_version[0]


def get_version_schema(
    context: Context,
    manifest: Manifest,
    version_eid: str,
) -> Iterator[Tuple[str, str]]:
    model = manifest.objects['model']['_schema/version']
    version = commands.getone(context, model, model.backend, id_=version_eid)
    return version['schema']


async def update_schema_version(context: Context, manifest: Manifest, schema: dict):
    model = manifest.objects['model']['_schema']
    data = DataItem(model, action=Action.UPSERT, payload={
        '_op': 'upsert',
        '_where': '_id="%s"' % schema['id'],
        '_id': schema['id'],
        'type': schema['type'],
        'name': schema['name'],
        'version': schema['version'],
        'schema': fix_data_for_json(schema),
    })
    await adrain(push_stream(context, aiter([data])))
