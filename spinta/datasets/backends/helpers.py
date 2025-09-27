import json
from typing import Any

from spinta import commands, spyna
from spinta.components import Property, Model, Context
from spinta.core.enums import Mode
from spinta.core.ufuncs import Env, asttoexpr
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import GivenValueCountMissmatch, MultiplePrimaryKeyCandidatesFound, NoPrimaryKeyCandidatesFound
from spinta.types.datatype import Ref, Array
from spinta.types.namespace import check_if_model_has_backend_and_source
from spinta.utils.schema import NA


def handle_ref_key_assignment(context: Context, keymap: KeyMap, env: Env, value: Any, ref: Ref) -> dict:
    keymap_name = ref.model.model_type()
    if ref.refprops != ref.model.external.pkeys:
        keymap_name = f"{keymap_name}.{'_'.join(prop.name for prop in ref.refprops)}"
    if isinstance(value, dict):
        new_value = []
        for prop in ref.refprops:
            new_value.append(value[prop.name])
        value = new_value
    elif not isinstance(value, (tuple, list)):
        value = [value]
    else:
        value = list(value)

    prop_count_mapping = {}
    for prop in ref.refprops:
        if isinstance(prop.dtype, Array) and env:
            items = env.resolve(prop.external.prepare)
            prop_count_mapping[prop.name] = len(items) if items is not NA else 1
        else:
            prop_count_mapping[prop.name] = 1
    expected_count = sum(item for item in prop_count_mapping.values())
    if len(value) != expected_count:
        raise GivenValueCountMissmatch(ref, given_count=len(value), expected_count=expected_count)

    if commands.identifiable(ref.prop):
        target_value = value
        if len(value) == 1:
            target_value = value[0]

        val = None
        contains = None
        if ref.refprops[0].name != "_id":
            contains = keymap.contains(keymap_name, target_value)
        if not contains:
            if target_value is None:
                return {"_id": None}

            ref_model = ref.model

            # FIXME Quick hack when trying to get `Internal` model keys while running in `External` mode (should probably return error, or None)
            if ref_model.mode == Mode.external and not check_if_model_has_backend_and_source(ref_model):
                return {"_id": keymap.encode(keymap_name, target_value)}

            expr_parts = ["select()"]
            for i, prop in enumerate(ref.refprops):
                expr_parts.append(f'{prop.place}="{value[i]}"')
            expr = asttoexpr(spyna.parse("&".join(expr_parts)))
            rows = commands.getall(context, ref_model, ref_model.backend, query=expr)

            found_value = False
            for row in rows:
                if val is not None:
                    raise MultiplePrimaryKeyCandidatesFound(ref, values=target_value)
                val = row["_id"]
                found_value = True

            if not found_value:
                raise NoPrimaryKeyCandidatesFound(ref, values=target_value)
        else:
            val = keymap.encode(keymap_name, target_value)
        val = {"_id": val}
    else:
        val = {}
        i = 0
        for prop, count in prop_count_mapping.items():
            values = value[i : i + count]
            if len(values) == 1:
                values = values[0]
            val[prop] = values
            i = i + count
    return val


def generate_ref_id_using_select(context: Context, dtype: Ref, data: dict) -> str:
    ref_model = dtype.model
    expr_parts = ["select()"]
    for prop in dtype.refprops:
        expr_parts.append(f'{prop.place}="{data[prop.name]}"')
    expr = asttoexpr(spyna.parse("&".join(expr_parts)))
    rows = commands.getall(context, ref_model, ref_model.backend, query=expr)

    found_value = False
    val = None
    for row in rows:
        if val is not None:
            raise MultiplePrimaryKeyCandidatesFound(dtype, values=data)
        val = row["_id"]
        found_value = True

    if not found_value:
        raise NoPrimaryKeyCandidatesFound(dtype, values=data)

    return val


def generate_pk_for_row(context: Context, model: Model, row: Any, keymap, pk_val: Any):
    pk = None
    if model.base and commands.identifiable(model.base):
        pk_val_base = extract_values_from_row(row, model.base.parent, model.base.pk or model.base.parent.external.pkeys)
        key = model.base.parent.model_type()
        if model.base.pk and model.base.pk != model.base.parent.external.pkeys:
            joined = "_".join(pk.name for pk in model.base.pk)
            key = f"{key}.{joined}"
        pk = keymap.encode(key, pk_val_base)

    pk = keymap.encode(model.model_type(), pk_val, pk)
    if pk and model.required_keymap_properties:
        for combination in model.required_keymap_properties:
            joined = "_".join(combination)
            key = f"{model.model_type()}.{joined}"
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


def handle_external_array_type(context: Context, dtype: Array, keymap: KeyMap, env: Env, val: Any):
    if isinstance(val, str):
        val = json.loads(val)

    if not isinstance(val, list):
        return val

    if isinstance(dtype.items.dtype, Ref):
        return [handle_ref_key_assignment(context, keymap, env, value, dtype.items.dtype) for value in val]

    return val


def flatten_keymap_encoding_values(data: object):
    # Backwards compatibility function, that converts nested dict values to flat list values
    # Used for generating and looking up keymap values
    if isinstance(data, dict):
        if len(data) == 1:
            return flatten_keymap_encoding_values(list(data.values())[0])
        return flatten_keymap_encoding_values(list(data.values()))
    elif isinstance(data, (list, tuple)):
        return list(flatten_keymap_encoding_values(item) for item in data)
    return data
