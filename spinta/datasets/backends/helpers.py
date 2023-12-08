from typing import Any

from spinta.components import Model
from spinta.core.ufuncs import Env
from spinta.datasets.enums import Level
from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Property, Context
from spinta.datasets.backends.notimpl.components import BackendNotImplemented
from spinta.datasets.components import ExternalBackend
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import GivenValueCountMissmatch, NoMatchingBackendDetected
from spinta.types.datatype import Ref, Array
from spinta.formats.helpers import get_response_type_as_format_class


def handle_ref_key_assignment(keymap: KeyMap, env: Env, value: Any, ref: Ref) -> dict:
    keymap_name = ref.model.model_type()
    if ref.refprops != ref.model.external.pkeys:
        keymap_name = f'{keymap_name}.{"_".join(prop.name for prop in ref.refprops)}'

    if not isinstance(value, (tuple, list)):
        value = [value]
    else:
        value = list(value)

    prop_count_mapping = {}
    for prop in ref.refprops:
        if isinstance(prop.dtype, Array) and env:
            items = env.resolve(prop.external.prepare)
            prop_count_mapping[prop.name] = len(items)
        else:
            prop_count_mapping[prop.name] = 1
    expected_count = sum(item for item in prop_count_mapping.values())
    if len(value) != expected_count:
        raise GivenValueCountMissmatch(given_count=len(value), expected_count=expected_count)

    if not ref.prop.level or ref.prop.level.value > 3:
        if len(value) == 1:
            value = value[0]
        val = keymap.encode(keymap_name, value)
        val = {'_id': val}
    else:
        val = {}
        i = 0
        for prop, count in prop_count_mapping.items():
            values = value[i:i + count]
            if len(values) == 1:
                values = values[0]
            val[prop] = values
    return val


def generate_pk_for_row(model: Model, row: Any, keymap, pk_val: Any):
    pk = None
    if model.base and ((model.base.level and model.base.level >= Level.identifiable) or not model.base.level):
        pk_val_base = extract_values_from_row(row, model.base.parent, model.base.pk or model.base.parent.external.pkeys)
        key = model.base.parent.model_type()
        if model.base.pk and model.base.pk != model.base.parent.external.pkeys:
            joined = '_'.join(pk.name for pk in model.base.pk)
            key = f'{key}.{joined}'
        pk = keymap.encode(key, pk_val_base)

    pk = keymap.encode(model.model_type(), pk_val, pk)
    if pk and model.required_keymap_properties:
        for combination in model.required_keymap_properties:
            joined = '_'.join(combination)
            key = f'{model.model_type()}.{joined}'
            val = extract_values_from_row(row, model, combination)
            keymap.encode(key, val, pk)
    return pk


def extract_values_from_row(row: Any, model: Model, keys: list):
    return_list = []
    for key in keys:
        if isinstance(key, str):
            key = model.properties[key]
        if isinstance(key, Property):
            if key.external and key.external.name:
                key = key.external.name
            else:
                key = key.name

        return_list.append(row[key])
    if len(return_list) == 1:
        return_list = return_list[0]
    return return_list


def detect_backend_from_content_type(context, content_type):
    config = context.get('config')
    backends = config.components['backends']
    for backend in backends.values():
        if issubclass(backend, ExternalBackend) and not issubclass(backend, BackendNotImplemented):
            if backend.accept_types and content_type in backend.accept_types:
                return backend()
    raise NoMatchingBackendDetected


def get_stream_for_direct_upload(
    context: Context,
    rows,
    request,
    params,
    content_type
):
    from spinta.commands.write import write
    store = prepare_manifest(context)
    manifest = store.manifest
    root = manifest.objects['ns']['']
    fmt = get_response_type_as_format_class(context, request, params, content_type)
    stream = write(context, root, rows, changed=True, fmt=fmt)
    return stream
