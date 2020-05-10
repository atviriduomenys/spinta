import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.backend.components import BackendManifest
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.manifest import read_manifest_schemas

log = logging.getLogger(__name__)


@commands.load.register(Context, BackendManifest, PostgreSQL)
def load(
    context: Context,
    manifest: BackendManifest,
    backend: PostgreSQL,
    *,
    into: Manifest = None,
    freezed: bool = True,
) -> None:
    if manifest.backend.bootstrapped():
        if into:
            log.info(
                'Loading manifest %r into %r from %r backend.',
                manifest.name,
                into.name,
                manifest.backend.name,
            )
        else:
            log.info(
                'Loading manifest %r from %r backend.',
                manifest.name,
                manifest.backend.name,
            )
        with backend.engine.begin() as conn:
            schemas = read_manifest_schemas(context, backend, conn)
            if into:
                load_manifest_nodes(context, into, schemas, source=manifest)
            else:
                load_manifest_nodes(context, manifest, schemas)

    else:
        log.warning(
            "Can't load manifest %r from %r backend. Loading manifest from sync sources instead.",
            manifest.name,
            manifest.backend.name,
        )

        target = into or manifest
        if '_schema' not in target.models:
            store = context.get('store')
            commands.load(context, store.internal, into=target)

        for source in manifest.sync:
            commands.load(context, source, into=into or manifest, freezed=freezed)
