from typing import Any
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Property, Context
from spinta.datasets.backends.notimpl.components import BackendNotImplemented
from spinta.datasets.components import ExternalBackend
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import NotImplementedFeature
from spinta.formats.helpers import get_format_by_content_type


def handle_ref_key_assignment(keymap: KeyMap, value: Any, prop: Property) -> dict:
    if not prop.level or prop.level.value > 3:
        val = keymap.encode(prop.dtype.model.model_type(), value)
        val = {'_id': val}
    else:
        if len(prop.dtype.refprops) > 1 and not prop.model.external.unknown_primary_key:
            raise NotImplementedFeature(prop, feature="Ability to have multiple refprops")
        if len(prop.dtype.refprops) == 1:
            val = {
                prop.dtype.refprops[0].name: value
            }
        elif not prop.model.external.unknown_primary_key and prop.model.external.pkeys:
            val = {
                prop.model.external.pkeys[0].name: value
            }
        else:
            val = keymap.encode(prop.dtype.model.model_type(), value)
            val = {'_id': val}
    return val


def detect_backend_from_content_type(context, content_type):
    config = context.get('config')
    backends = config.components['backends']
    for backend in backends.values():
        if issubclass(backend, ExternalBackend) and backend != BackendNotImplemented:
            if backend.accept_types and content_type in backend.accept_types:
                return backend
    raise ValueError(
        f"Can't find a matching external backend for the given content type {content_type!r}"
    )


def get_stream_for_direct_upload(
    context: Context,
    rows,
    content_type
):
    from spinta.commands.write import write
    store = prepare_manifest(context)
    manifest = store.manifest
    root = manifest.objects['ns']['']
    fmt = get_format_by_content_type(context, content_type)
    stream = write(context, root, rows, changed=True, fmt=fmt)
    return stream
