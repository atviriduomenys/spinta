from __future__ import annotations

from typing import Any, List

from spinta import commands
from spinta.backends import Backend
from spinta.backends.helpers import check_if_model_primary_key_is_composite, is_custom_id_prop
from spinta.components import Context, Model
from spinta.core.enums import Mode
from spinta.datasets.backends.helpers import flatten_keymap_encoding_values, generate_ref_id_using_select
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.helpers import encode_composite_string_id
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import GivenValueCountMissmatch, KeymapValueNotFound
from spinta.types.datatype import Array, Base32, ExternalRef, Ref
from spinta.types.namespace import check_if_model_has_backend_and_source


@commands.cast_backend_to_python.register(Context, Model, Sql, dict)
def cast_backend_to_python(
    context: Context, model: Model, backend: Sql, data: dict, *, keymap: KeyMap = None, **kwargs
):
    if keymap is None:
        keymap = context.get(f"keymap.{model.keymap.name}")

    super_ = commands.cast_backend_to_python[Context, Model, Backend, dict]
    return super_(context, model, backend, data, keymap=keymap, **kwargs)


def _cast_custom_ref_id(
    context: Context,
    dtype: Ref,
    backend: Sql,
    ref_model: Model,
    ref_id_prop,
    values: dict,
    ref_filters_disabled: bool,
):
    # A custom `_id` is derived from the referenced model's primary keys. When
    # implicit ref filters are disabled, the referenced row might be filtered
    # out at the source (e.g. by an explicit filter on the ref model), so
    # selecting it could find nothing. Since the refprop values already give us
    # those primary keys, rebuild the id the same way the referenced model would
    # (mirrors `build_row_result`) instead of requiring the row to be selectable
    # -- but only when the refprops are exactly those primary keys.
    if ref_filters_disabled and dtype.refprops == ref_model.external.pkeys:
        pk_values = list(values.values())
        is_composite = check_if_model_primary_key_is_composite(ref_model)
        if is_composite and not isinstance(ref_id_prop.dtype, Base32):
            # `build_row_result` joins composite primary keys into a single
            # string for non Base32 ids.
            return encode_composite_string_id(pk_values, ref_model.external.pkeys)
        # Single primary key ids (and Base32 ids, which cbor encode composite
        # keys) are produced by casting the raw primary key value(s) through the
        # `_id` data type.
        data = pk_values if is_composite else pk_values[0]
        return commands.cast_backend_to_python(context, ref_id_prop.dtype, backend, data)

    return generate_ref_id_using_select(context, dtype, values)


def _cast_keymap_ref_id(
    context: Context,
    dtype: Ref,
    ref_model: Model,
    keymap: KeyMap,
    keymap_name: str,
    encoding_values,
    values: dict,
    ref_filters_disabled: bool,
):
    if keymap.contains(keymap_name, encoding_values):
        return keymap.encode(keymap_name, encoding_values)

    if encoding_values is None:
        return None

    if ref_model.mode == Mode.external and not check_if_model_has_backend_and_source(ref_model):
        raise KeymapValueNotFound(
            dtype,
            keymap=keymap.name,
            model_name=dtype.model.name,
            values=encoding_values,
        )

    # When implicit ref filters are disabled, the referenced row might be
    # filtered out at the source (e.g. by an explicit filter on the ref model),
    # so selecting it could find nothing. The `_id` of a keymap based model is
    # deterministic given its primary keys, so encode it directly instead of
    # requiring the row to be selectable.
    #
    # Models with an identifiable `base` are excluded: their `_id` is seeded by
    # the base's key (see `generate_pk_for_row`), which cannot be reconstructed
    # from the foreign key alone. Encoding directly would mint a base-unaware id
    # that diverges from -- and later clashes with -- the value the referenced
    # model produces, so fall back to selecting.
    base_seeded = ref_model.base and commands.identifiable(ref_model.base)
    if ref_filters_disabled and not base_seeded:
        return keymap.encode(keymap_name, encoding_values)

    return generate_ref_id_using_select(context, dtype, values)


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

    # If all values are None, treat it as None given
    if all(value is None for value in encoding_values):
        return None

    if len(encoding_values) == 1:
        encoding_values = encoding_values[0]

    # Backwards compatibility, all nested values are converted to list values without keys
    encoding_values = flatten_keymap_encoding_values(encoding_values)

    # First decide where the referenced model's `_id` comes from: a custom `_id`
    # is derived from its own primary keys, while a default `_id` is resolved
    # through the keymap. The two are produced differently, so handle each in its
    # own helper (each also decides whether the id can be rebuilt locally or
    # whether the referenced row must be selected).
    ref_id_prop = ref_model.properties["_id"]
    ref_filters_disabled = not context.get("config").check_ref_filters
    if is_custom_id_prop(ref_id_prop):
        id_value = _cast_custom_ref_id(context, dtype, backend, ref_model, ref_id_prop, values, ref_filters_disabled)
    else:
        id_value = _cast_keymap_ref_id(
            context, dtype, ref_model, keymap, keymap_name, encoding_values, values, ref_filters_disabled
        )

    processed_data["_id"] = id_value

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
