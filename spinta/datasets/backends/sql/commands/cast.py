from __future__ import annotations

from typing import List, Any

from spinta import commands
from spinta.backends import Backend
from spinta.backends.helpers import is_custom_id_prop
from spinta.components import Context, Model
from spinta.core.enums import Mode
from spinta.datasets.backends.helpers import generate_ref_id_using_select, flatten_keymap_encoding_values
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.helpers import extract_and_cast_properties_from_list, process_data_for_pkey
from spinta.datasets.backends.sql.ufuncs.query.components import SQL_PK_KEY, SQL_PK_COMBINATION_KEY
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import GivenValueCountMissmatch, KeymapValueNotFound, PropertyNotFound
from spinta.types.datatype import Ref, ExternalRef, Array, PrimaryKey
from spinta.types.namespace import check_if_model_has_backend_and_source


@commands.cast_backend_to_python.register(Context, Model, Sql, dict)
def cast_backend_to_python(
    context: Context, model: Model, backend: Sql, data: dict, *, keymap: KeyMap = None, **kwargs
):
    if keymap is None:
        keymap = context.get(f"keymap.{model.keymap.name}")

    super_ = commands.cast_backend_to_python[Context, Model, Backend, dict]
    return super_(context, model, backend, data, keymap=keymap, **kwargs)


@commands.cast_backend_to_python.register(Context, Ref, Sql, dict)
def cast_backend_to_python(context: Context, dtype: Ref, backend: Sql, data: dict, *, keymap: KeyMap = None, **kwargs):
    processed_data = {}
    if keymap is None:
        keymap = context.get(f"keymap.{dtype.prop.model.keymap.name}")

    cached_results = {}
    model = dtype.prop.model
    for key, value in data.items():
        # Skip '_id', it will be processed below, since it needs to be converted to uuid type
        if key == SQL_PK_KEY:
            continue

        dtype_key = f"{dtype.prop.place}.{key}"
        prop = commands.resolve_property(model, dtype_key)
        if prop is None:
            raise PropertyNotFound(model, property=dtype_key)

        if dtype_key not in cached_results:
            cached_results[dtype_key] = commands.cast_backend_to_python(
                context, prop, backend, value, keymap=keymap, **kwargs
            )
        processed_data[key] = cached_results[dtype_key]

    ref_model = dtype.model
    keymap_name = ref_model.model_type()
    if dtype.refprops != ref_model.external.pkeys:
        keymap_name = f"{keymap_name}.{'_'.join(prop.name for prop in dtype.refprops)}"

    id_data = data.get(SQL_PK_KEY)
    if not id_data:
        return processed_data

    for prop in dtype.refprops:
        if prop.name not in id_data:
            # Skip trying to create _id, since not all refprop are given
            return processed_data

    id_processed_data = extract_and_cast_properties_from_list(
        context=context,
        backend=backend,
        model=model,
        data=id_data,
        keymap=keymap,
        cache=cached_results,
        prefix=dtype.prop.place,
        **kwargs,
    )
    encoding_values = list(id_processed_data.values())

    if all(value is None for value in encoding_values):
        return None

    if len(encoding_values) == 1:
        encoding_values = encoding_values[0]

    # Backwards compatibility, all nested values are converted to list values without keys
    encoding_values = flatten_keymap_encoding_values(encoding_values)
    if is_custom_id_prop(ref_model.id_prop):
        id_value = generate_ref_id_using_select(context, dtype, id_processed_data)
    else:
        contains = keymap.contains(keymap_name, encoding_values)

        if contains:
            id_value = keymap.encode(keymap_name, encoding_values)
        elif encoding_values is None:
            id_value = None
        elif ref_model.mode == Mode.external and not check_if_model_has_backend_and_source(ref_model):
            raise KeymapValueNotFound(
                dtype,
                keymap=keymap.name,
                model_name=dtype.model.name,
                values=encoding_values,
            )
        else:
            id_value = generate_ref_id_using_select(context, dtype, id_processed_data)

    processed_data[SQL_PK_KEY] = id_value

    if not processed_data or all(value is None for value in processed_data.values()):
        return None

    return processed_data


@commands.cast_backend_to_python.register(Context, ExternalRef, Sql, dict)
def cast_backend_to_python(context: Context, dtype: ExternalRef, backend: Sql, data: dict, **kwargs):
    processed_data = {}
    for key, value in data.items():
        prop = commands.resolve_property(dtype.prop.model, f"{dtype.prop.place}.{key}")
        if prop is not None:
            processed_data[key] = commands.cast_backend_to_python(context, prop, backend, value, **kwargs)

    for prop in dtype.refprops:
        if prop.name not in processed_data and prop.name in data:
            processed_data[prop.name] = commands.cast_backend_to_python(
                context, prop, backend, data[prop.name], **kwargs
            )

    if not processed_data or all(value is None for value in processed_data.values()):
        return None

    return processed_data


@commands.cast_backend_to_python.register(Context, Ref, Sql, object)
def cast_backend_to_python(context: Context, dtype: Ref, backend: Sql, data: object, **kwargs):
    if len(dtype.refprops) != 1:
        raise GivenValueCountMissmatch(dtype, given_count=1, expected_count=len(dtype.refprops))

    return commands.cast_backend_to_python(
        context, dtype, backend, {SQL_PK_KEY: {dtype.refprops[0].name: data}}, **kwargs
    )


@commands.cast_backend_to_python.register(Context, ExternalRef, Sql, object)
def cast_backend_to_python(context: Context, dtype: Ref, backend: Sql, data: object, **kwargs):
    if len(dtype.refprops) != 1:
        raise GivenValueCountMissmatch(dtype, given_count=1, expected_count=len(dtype.refprops))

    return commands.cast_backend_to_python(context, dtype, backend, {dtype.refprops[0].name: data}, **kwargs)


@commands.cast_backend_to_python.register(Context, Ref, Sql, (list, tuple))
def cast_backend_to_python(context: Context, dtype: Ref, backend: Sql, data: list | tuple, **kwargs):
    # This kinda of convertion is not preferred (dict values are better), but it's kept to support
    # dialects that cannot cast to json objects.
    # It will only attempt to map dtype.refprops to given values, meaning Denorm mapping is lost
    if len(data) != len(dtype.refprops):
        raise GivenValueCountMissmatch(dtype, given_count=len(data), expected_count=len(dtype.refprops))

    return commands.cast_backend_to_python(
        context,
        dtype,
        backend,
        {
            SQL_PK_KEY: {prop.name: data[i] for i, prop in enumerate(dtype.refprops)},
        },
        **kwargs,
    )


@commands.cast_backend_to_python.register(Context, ExternalRef, Sql, (list, tuple))
def cast_backend_to_python(context: Context, dtype: ExternalRef, backend: Sql, data: list | tuple, **kwargs):
    # This kinda of convertion is not preferred (dict values are better), but it's kept to support
    # dialects that cannot cast to json objects.
    # It will only attempt to map dtype.refprops to given values, meaning Denorm mapping is lost
    if len(data) != len(dtype.refprops):
        raise GivenValueCountMissmatch(dtype, given_count=len(data), expected_count=len(dtype.refprops))

    return commands.cast_backend_to_python(
        context, dtype, backend, {prop.name: data[i] for i, prop in enumerate(dtype.refprops)}, **kwargs
    )


@commands.cast_backend_to_python.register(Context, Array, Sql, list)
def cast_backend_to_python(context: Context, dtype: Array, backend: Sql, data: List[Any], **kwargs) -> List[Any]:
    # Edge case, when using intermediate table with Sql backend, None values get returned as [None] instead of None
    if dtype.model is not None and len(data) == 1 and data[0] is None:
        return commands.cast_backend_to_python(context, dtype.items.dtype, backend, None, **kwargs)

    if data and dtype:
        data = [commands.cast_backend_to_python(context, dtype.items.dtype, backend, v, **kwargs) for v in data]
        if all(v is None for v in data):
            return None
        return data

    return data


@commands.cast_backend_to_python.register(Context, PrimaryKey, Sql, dict)
def cast_backend_to_python(
    context: Context, dtype: PrimaryKey, backend: Sql, data: dict, *, keymap: KeyMap = None, **kwargs
):
    if keymap is None:
        keymap = context.get(f"keymap.{dtype.prop.model.keymap.name}")

    model = dtype.prop.model

    pk = None
    cached_results = {}
    pk_data = data.get(SQL_PK_KEY)
    # Extract pkey key data
    processed_data = process_data_for_pkey(
        context=context, backend=backend, model=model, data=pk_data, keymap=keymap, cache=cached_results, **kwargs
    )
    if processed_data is None:
        return None

    # In case model has base extract pkey from it
    if model.base and commands.identifiable(model.base):
        key = model.base.parent.model_type()
        if model.base.pk and model.base.pk != model.base.parent.external.pkeys:
            joined = "_".join(pk.name for pk in model.base.pk)
            key = f"{key}.{joined}"
        pk = keymap.encode(key, processed_data)

    # Assign pkey to model
    pk = keymap.encode(model.model_type(), processed_data, pk)

    # Handle required keymap properties combinations
    all_combination_data = data.get(SQL_PK_COMBINATION_KEY)
    if pk and model.required_keymap_properties and all_combination_data:
        for combination in model.required_keymap_properties:
            joined = "_".join(combination)
            key = f"{model.model_type()}.{joined}"

            combination_data = all_combination_data.get(combination)
            if combination_data is None or list(combination_data.keys()) != list(combination):
                continue

            processed_data = process_data_for_pkey(
                context=context,
                backend=backend,
                model=model,
                data=combination_data,
                keymap=keymap,
                cache=cached_results,
                **kwargs,
            )
            if processed_data is None:
                continue

            keymap.encode(key, processed_data, pk)
    return pk
