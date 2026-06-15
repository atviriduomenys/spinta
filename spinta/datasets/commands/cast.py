from __future__ import annotations

from spinta import commands
from spinta.backends import Backend
from spinta.backends.helpers import check_if_model_primary_key_is_composite, is_custom_id_prop
from spinta.components import Context, Model
from spinta.core.enums import Mode
from spinta.datasets.backends.helpers import generate_ref_id_using_select, flatten_keymap_encoding_values
from spinta.datasets.components import ExternalBackend
from spinta.datasets.enums import ExternalIdPattern
from spinta.datasets.helpers import (
    extract_and_cast_properties_from_list,
    encode_composite_string_id,
    process_data_for_pkey,
    compare_ref_property_count,
)
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import KeymapValueNotFound, PropertyNotFound, GivenValueCountMissmatch
from spinta.types.datatype import Base32, ExternalRef, PrimaryKey
from spinta.types.datatype import Ref
from spinta.types.namespace import check_if_model_has_backend_and_source


def _cast_custom_ref_id(
    context: Context,
    dtype: Ref,
    backend: ExternalBackend,
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

    # Returns None if there was no permission to access dtype.model (`getall` or `search`).
    # Potentially might want to raise an error in that scenario, but need confirmation in documentation.
    return generate_ref_id_using_select(context, dtype, values)


@commands.cast_backend_to_python.register(Context, Model, ExternalBackend, dict)
def cast_backend_to_python(
    context: Context, model: Model, backend: ExternalBackend, data: dict, *, keymap: KeyMap = None, **kwargs
):
    if keymap is None:
        keymap = context.get(f"keymap.{model.keymap.name}")

    super_ = commands.cast_backend_to_python[Context, Model, Backend, dict]
    return super_(context, model, backend, data, keymap=keymap, **kwargs)


@commands.cast_backend_to_python.register(Context, Ref, ExternalBackend, dict)
def cast_backend_to_python(
    context: Context, dtype: Ref, backend: ExternalBackend, data: dict, *, keymap: KeyMap = None, **kwargs
):
    # This implementation expects ExternalIdPattern explained pattern, if `ExternalBackend` implements different solution
    # it needs to create new `cast_backend_to_python` for it self.
    id_constant = ExternalIdPattern.ID_KEY.value
    processed_data = {}
    if keymap is None:
        keymap = context.get(f"keymap.{dtype.prop.model.keymap.name}")

    cached_results = {}
    model = dtype.prop.model
    for key, value in data.items():
        # Skip '_id', it will be processed below, since it needs to be converted to uuid type
        if key == id_constant:
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

    id_data = data.get(id_constant)
    if not id_data:
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

    # If all values are None, treat it as None given
    if all(value is None for value in encoding_values):
        return None

    # Backwards compatibility, all nested values are converted to list values without keys
    encoding_values = flatten_keymap_encoding_values(encoding_values)
    # Check if the given value count is equal to the expected refprop count
    compare_ref_property_count(dtype, encoding_values)
    if len(encoding_values) == 1:
        encoding_values = encoding_values[0]

    # First decide where the referenced model's `_id` comes from: a custom `_id`
    # is derived from its own primary keys, while a default `_id` is resolved
    # through the keymap. The two are produced differently, so handle each in its
    # own helper (each also decides whether the id can be rebuilt locally or
    # whether the referenced row must be selected).
    ref_id_prop = ref_model.id_prop
    ref_filters_disabled = not context.get("config").check_ref_filters
    if is_custom_id_prop(ref_id_prop):
        id_value = _cast_custom_ref_id(
            context, dtype, backend, ref_model, ref_id_prop, id_processed_data, ref_filters_disabled
        )
    else:
        id_value = _cast_keymap_ref_id(
            context, dtype, ref_model, keymap, keymap_name, encoding_values, id_processed_data, ref_filters_disabled
        )

    processed_data[id_constant] = id_value

    if not processed_data or all(value is None for value in processed_data.values()):
        return None

    return processed_data


@commands.cast_backend_to_python.register(Context, ExternalRef, ExternalBackend, dict)
def cast_backend_to_python(context: Context, dtype: ExternalRef, backend: ExternalBackend, data: dict, **kwargs):
    # Reusing global `Backend` implementation, this is needed here to remove ambiguity with dispatchers
    # ([Context, ExternalRef, ExternalBackend, object] clashes with [Context, ExternalRef, Backend, dict])
    super_ = commands.cast_backend_to_python[Context, ExternalRef, Backend, dict]
    return super_(context, dtype, backend, data, **kwargs)


@commands.cast_backend_to_python.register(Context, Ref, ExternalBackend, object)
def cast_backend_to_python(context: Context, dtype: Ref, backend: ExternalBackend, data: object, **kwargs):
    if len(dtype.refprops) != 1:
        raise GivenValueCountMissmatch(dtype, given_count=1, expected_count=len(dtype.refprops))

    return commands.cast_backend_to_python(
        context, dtype, backend, {ExternalIdPattern.ID_KEY.value: {dtype.refprops[0].name: data}}, **kwargs
    )


@commands.cast_backend_to_python.register(Context, ExternalRef, ExternalBackend, object)
def cast_backend_to_python(context: Context, dtype: ExternalRef, backend: ExternalBackend, data: object, **kwargs):
    if len(dtype.refprops) != 1:
        raise GivenValueCountMissmatch(dtype, given_count=1, expected_count=len(dtype.refprops))

    return commands.cast_backend_to_python(context, dtype, backend, {dtype.refprops[0].name: data}, **kwargs)


@commands.cast_backend_to_python.register(Context, Ref, ExternalBackend, (list, tuple))
def cast_backend_to_python(context: Context, dtype: Ref, backend: ExternalBackend, data: list | tuple, **kwargs):
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
            ExternalIdPattern.ID_KEY.value: {prop.name: data[i] for i, prop in enumerate(dtype.refprops)},
        },
        **kwargs,
    )


@commands.cast_backend_to_python.register(Context, ExternalRef, ExternalBackend, (list, tuple))
def cast_backend_to_python(
    context: Context, dtype: ExternalRef, backend: ExternalBackend, data: list | tuple, **kwargs
):
    # This kinda of convertion is not preferred (dict values are better), but it's kept to support
    # dialects that cannot cast to json objects.
    # It will only attempt to map dtype.refprops to given values, meaning Denorm mapping is lost
    if len(data) != len(dtype.refprops):
        raise GivenValueCountMissmatch(dtype, given_count=len(data), expected_count=len(dtype.refprops))

    return commands.cast_backend_to_python(
        context, dtype, backend, {prop.name: data[i] for i, prop in enumerate(dtype.refprops)}, **kwargs
    )


@commands.cast_backend_to_python.register(Context, PrimaryKey, ExternalBackend, dict)
def cast_backend_to_python(
    context: Context, dtype: PrimaryKey, backend: ExternalBackend, data: dict, *, keymap: KeyMap = None, **kwargs
):
    if keymap is None:
        keymap = context.get(f"keymap.{dtype.prop.model.keymap.name}")

    model = dtype.prop.model

    pk = None
    cached_results = {}
    pk_data = data.get(ExternalIdPattern.ID_KEY.value)
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
    all_combination_data = data.get(ExternalIdPattern.COMBINATIONS_KEY.value)
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
