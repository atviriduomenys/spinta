from __future__ import annotations

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context, Model
from spinta.core.enums import Mode
from spinta.datasets.backends.helpers import generate_ref_id_using_select, flatten_keymap_encoding_values
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import GivenValueCountMissmatch, KeymapValueNotFound
from spinta.types.datatype import Ref, ExternalRef
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

    for key, value in data.items():
        # Skip '_id', it will be processed below, since it needs to be converted to uuid type
        if key == "_id":
            continue

        prop = commands.resolve_property(dtype.prop.model, f"{dtype.prop.place}.{key}")
        if prop is not None:
            processed_data[key] = commands.cast_backend_to_python(
                context, prop, backend, value, keymap=keymap, **kwargs
            )

    ref_model = dtype.model
    keymap_name = ref_model.model_type()
    if dtype.refprops != ref_model.external.pkeys:
        keymap_name = f"{keymap_name}.{'_'.join(prop.name for prop in dtype.refprops)}"

    values = {}
    id_data = data.get("_id")
    if not id_data:
        return processed_data

    for prop in dtype.refprops:
        if prop.name not in id_data:
            # Skip trying to create _id, since not all refprop are given
            return processed_data
        values[prop.name] = id_data[prop.name]

    encoding_values = list(values.values())
    if len(encoding_values) == 1:
        encoding_values = encoding_values[0]

    # Backwards compatibility, all nested values are converted to list values without keys
    encoding_values = flatten_keymap_encoding_values(encoding_values)
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
        id_value = generate_ref_id_using_select(context, dtype, values)

    processed_data["_id"] = commands.cast_backend_to_python(
        context, ref_model.properties["_id"], backend, id_value, keymap=keymap, **kwargs
    )

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

    return commands.cast_backend_to_python(context, dtype, backend, {"_id": {dtype.refprops[0].name: data}}, **kwargs)


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
            "_id": {prop.name: data[i] for i, prop in enumerate(dtype.refprops)},
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
