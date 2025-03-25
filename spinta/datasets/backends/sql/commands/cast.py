from __future__ import annotations

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context, Model, Mode
from spinta.datasets.backends.helpers import generate_ref_id_using_select
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.keymaps.components import KeyMap
from spinta.types.datatype import Ref, ExternalRef
from spinta.types.namespace import check_if_model_has_backend_and_source


@commands.cast_backend_to_python.register(Context, Model, Sql, dict)
def cast_backend_to_python(
    context: Context,
    model: Model,
    backend: Sql,
    data: dict,
    *,
    keymap: KeyMap = None,
    **kwargs
):
    if keymap is None:
        keymap = context.get(f'keymap.{model.keymap.name}')

    super_ = commands.cast_backend_to_python[Context, Model, Backend, dict]
    return super_(context, model, backend, data, keymap=keymap, **kwargs)


@commands.cast_backend_to_python.register(Context, Ref, Sql, dict)
def cast_backend_to_python(
    context: Context,
    dtype: Ref,
    backend: Sql,
    data: dict,
    *,
    keymap: KeyMap = None,
    **kwargs
):
    processed_data = {}
    if keymap is None:
        keymap = context.get(f'keymap.{dtype.prop.model.keymap.name}')

    for key in data:
        prop = commands.resolve_property(dtype.prop.model, f'{dtype.prop.place}.{key}')
        if prop is not None:
            processed_data[key] = commands.cast_backend_to_python(
                context,
                prop,
                backend,
                data[key],
                keymap=keymap,
                **kwargs
            )

    ref_model = dtype.model
    keymap_name = ref_model.model_type()
    if dtype.refprops != ref_model.external.pkeys:
        keymap_name = f'{keymap_name}.{"_".join(prop.name for prop in dtype.refprops)}'

    values = {}
    for prop in dtype.refprops:
        if prop.name not in data:
            # Skip trying to create _id, since not all refprop are given
            return processed_data
        values[prop.name] = (data[prop.name])

    encoding_values = list(values.values())
    if len(encoding_values) == 1:
        encoding_values = encoding_values[0]

    contains = keymap.contains(keymap_name, encoding_values)
    if contains:
        id_value = keymap.encode(keymap_name, encoding_values)
    elif encoding_values is None:
        id_value = None
    elif ref_model.mode == Mode.external and not check_if_model_has_backend_and_source(ref_model):
        # FIXME Quick hack when trying to get `Internal` model keys while running in `External` mode (should probably return error, or None)
        id_value = keymap.encode(keymap_name, encoding_values)
    else:
        id_value = generate_ref_id_using_select(context, dtype, values)

    processed_data['_id'] = commands.cast_backend_to_python(
        context,
        ref_model.properties['_id'],
        backend,
        id_value,
        keymap=keymap,
        **kwargs
    )

    if not processed_data or all(value is None for value in processed_data.values()):
        return None

    return processed_data


@commands.cast_backend_to_python.register(Context, ExternalRef, Sql, dict)
def cast_backend_to_python(
    context: Context,
    dtype: ExternalRef,
    backend: Sql,
    data: dict,
    **kwargs
):
    processed_data = {}
    for key in data:
        prop = commands.resolve_property(dtype.prop.model, f'{dtype.prop.place}.{key}')
        if prop is not None:
            processed_data[key] = commands.cast_backend_to_python(
                context,
                prop,
                backend,
                data[key],
                **kwargs
            )

    for prop in dtype.refprops:
        if prop.name not in processed_data and prop.name in data:
            processed_data[prop.name] = commands.cast_backend_to_python(
                context,
                prop,
                backend,
                data[prop.name],
                **kwargs
            )

    if not processed_data or all(value is None for value in processed_data.values()):
        return None

    return processed_data


@commands.cast_backend_to_python.register(Context, Ref, Sql, object)
def cast_backend_to_python(
    context: Context,
    dtype: Ref,
    backend: Sql,
    data: object,
    **kwargs
):
    if len(dtype.refprops) != 1:
        raise Exception("CANNOT MAP UNKNOWN VALUE", dtype.refprops, data)

    return commands.cast_backend_to_python(
        context,
        dtype,
        backend, {
            dtype.refprops[0].name: data
        },
        **kwargs
    )


@commands.cast_backend_to_python.register(Context, Ref, Sql, (list, tuple))
def cast_backend_to_python(
    context: Context,
    dtype: Ref,
    backend: Sql,
    data: list | tuple,
    **kwargs
):
    if len(data) != len(dtype.refprops):
        raise Exception("CANNOT MAP UNKNOWN VALUE", dtype.refprops, data)

    return commands.cast_backend_to_python(
        context,
        dtype,
        backend, {prop.name: data[i] for i, prop in enumerate(dtype.refprops)},
        **kwargs
    )
