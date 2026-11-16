from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.rdf.components import RdfManifest
from spinta.manifests.rdf.helpers import read_rdf_manifest


@commands.load.register(Context, RdfManifest)
def load(
    context: Context,
    manifest: RdfManifest,
    *,
    into: Manifest = None,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load=False,
):
    if load_internal:
        target = into or manifest
        if not commands.has_model(context, target, "_schema"):
            store = context.get("store")
            commands.load(context, store.internal, into=target, full_load=full_load)

    if manifest.path is None:
        return

    # Every RDF source loaded into the same target is read together, because
    # they describe one dataset and their definitions refer to each other.
    # They are loaded once, by the first source of the group, the others are
    # already covered by it.
    sources = _rdf_sources(manifest, into)
    if sources[0] is manifest:
        dataset_name = context.get("rc").get("given_dataset_name")
        fetch_remote = context.get("config").rdf_fetch_remote
        schemas = read_rdf_manifest(sources, dataset_name, fetch_remote=fetch_remote)

        if into:
            load_manifest_nodes(context, into, schemas, source=manifest)
        else:
            load_manifest_nodes(context, manifest, schemas)

    for source in manifest.sync:
        commands.load(
            context,
            source,
            into=into or manifest,
            rename_duplicates=rename_duplicates,
            load_internal=load_internal,
            full_load=full_load,
        )


def _rdf_sources(manifest: RdfManifest, into: Manifest | None) -> list[RdfManifest]:
    """Return RDF manifests that are loaded into the same target as `manifest`."""
    target = into or manifest
    candidates = [target] if isinstance(target, RdfManifest) else []
    candidates.extend(s for s in target.sync if isinstance(s, RdfManifest))
    candidates.append(manifest)

    sources: list[RdfManifest] = []
    for candidate in candidates:
        # Compare by identity, `Manifest.__eq__` is not meant for this.
        if candidate.path and not any(candidate is source for source in sources):
            sources.append(candidate)
    return sources
