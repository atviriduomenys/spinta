from typing import AsyncIterator, Iterator, Tuple, Iterable

import datetime

from toposort import toposort

from spinta import commands
from spinta.utils.json import fix_data_for_json
from spinta.utils.aiotools import aiter
from spinta.utils.aiotools import adrain
from spinta.commands.write import push_stream
from spinta.components import Context, DataItem, Action
from spinta.manifests.components import Manifest
from spinta.manifests.backend.components import BackendManifest


async def run_bootstrap(context: Context, manifest: BackendManifest):
    # Load manifest from sync sources
    for source in manifest.sync:
        commands.load(context, source, into=manifest, freezed=True)
    commands.link(context, manifest)
    commands.check(context, manifest)
    commands.prepare(context, manifest)

    # Bootstrap all backends
    for backend in manifest.store.backends.values():
        commands.bootstrap(context, backend)

    # Sync versions
    stream = read_sync_versions(context, manifest)
    stream = versions_to_dstream(manifest, stream, applied=True)
    await adrain(push_stream(context, stream))

    # Update current schemas
    for nodes in manifest.objects.values():
        for node in nodes.values():
            schema = commands.manifest_read_freezed(context, manifest, node.eid)
            update_schema_version(context, schema)


async def run_migrations(context: Context, manifest: BackendManifest):
    # Sync versions
    stream = read_sync_versions(context, manifest)
    stream = versions_to_dstream(manifest, stream)
    await adrain(push_stream(context, stream))

    # Apply unapplied versions
    for version in read_unapplied_versions():
        apply_version(version)
        update_schema_version(context, version['schema'])


def read_unapplied_versions():
    raise NotImplementedError


def apply_version():
    raise NotImplementedError


def read_sync_versions(context: Context, manifest: Manifest):
    for source in manifest.sync:
        for eid in commands.manifest_list_schemas(context, source):
            for version in commands.manifest_read_versions(context, source, eid):
                yield version


async def versions_to_dstream(
    manifest: BackendManifest,
    versions: Iterable[dict],
    *,
    applied: bool = False,
) -> AsyncIterator[DataItem]:
    now = datetime.datetime.now(datetime.timezone.utc)
    model = manifest.objects['model']['_version']
    for version in versions:
        payload = {
            '_op': 'upsert',
            '_where': '_id="%s"' % version['id'],
            '_id': version['id'],
            'created': version['date'],
            'synced': now,
            'parents': version['parents'],
            'changes': fix_data_for_json(version['changes']),
            'migrate': version['migrate'],
            'schema_type': version['schema']['type'],
            'schema_name': version['schema']['name'],
            'schema_id': version['schema']['id'],
            'schema': fix_data_for_json(version['schema']),
        }
        if applied:
            payload['applied'] = now
        yield DataItem(model, action=Action.UPSERT, payload=payload)


def read_manifest_schemas(context: Context, manifest: BackendManifest):
    model = manifest.objects['model']['_schema']
    query = {
        'select': ['schema'],
    }
    for row in commands.getall(context, model, model.backend, **query):
        yield row['_id'], row['schema']


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


def list_sorted_versions(
    context: Context,
    manifest: Manifest,
    *,
    applied: bool = False,
) -> Iterator[Tuple[str, str]]:
    model = manifest.objects['model']['_version']
    query = {
        'select': ['schema_id', '_id', 'parents'],
        'query': [],
    }
    if applied:
        query['query'].append({'name': 'eq', 'args': ['applied', None]})
    else:
        query['query'].append({'name': 'ne', 'args': ['applied', None]})
    schemas = {}
    versions = {}
    for row in commands.getall(context, model, model.backend, **query):
        schemas[row['_id']] = row['schema_id']
        versions[row['_id']] = row['parents']

    for group in toposort(versions):
        for vid in sorted(group):
            yield schemas[vid], vid


def update_schema_version(context: Context, manifest: Manifest, sid: str, vid: str):
    model = manifest.objects['model']['_version']
    version = commands.getone(context, model, model.backend, id_=vid)
    data = DataItem(model, action=Action.UPSERT, payload={
        '_op': 'upsert',
        '_where': '_id="%s"' % sid,
        '_id': version['schema_id'],
        'type': version['schema_type'],
        'name': version['schema_name'],
        'version': {'_id': version['_id']},
        'schema': fix_data_for_json(version['schema']),
    })
    adrain(push_stream(context, aiter([data])))
