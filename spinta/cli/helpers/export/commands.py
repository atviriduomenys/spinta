from typing import Optional, Union

from spinta import commands
from spinta.backends import Backend
from spinta.cli.helpers.errors import cli_error
from spinta.components import Context, Model, DataSubItem, Property
from spinta.core.enums import Access
from spinta.formats.components import Format
from spinta.types.datatype import DataType, Ref, ExternalRef, Denorm, Object, Inherit, BackRef, Array, File
from spinta.utils.data import take
from spinta.utils.nestedstruct import flatten_value
from spinta.utils.schema import NA, NotAvailable


@commands.export_data.register(Context, Model, Format)
def export_data(context: Context, model: Model, fmt: Format, *, data: object, **kwargs):
    cli_error(f"Export for {type(fmt)!r} Format is supported yet.")


@commands.export_data.register(Context, Model, Backend)
def export_data(context: Context, model: Model, backend: Backend, *, data: object, **kwargs):
    cli_error(f"Export for {backend.type!r} Backend is supported yet.")


@commands.before_export.register(Context, Model, Backend)
def before_export(context: Context, model: Model, backend: Backend, *, data: DataSubItem):
    patch = take(["_id"], data.patch)
    patch["_revision"] = take("_revision", data.patch, data.saved)
    for prop in take(model.properties).values():
        if not prop.dtype.inherited:
            prop_data = data[prop.name]
            value = commands.before_export(
                context,
                prop.dtype,
                backend,
                data=prop_data,
            )
            patch.update(value)
    return patch


@commands.before_export.register(Context, Property, Backend)
def before_export(context: Context, prop: Property, backend: Backend, *, data: DataSubItem):
    return before_export(context, prop.dtype, backend, data=data)


@commands.before_export.register(Context, DataType, Backend)
def before_export(context: Context, dtype: DataType, backend: Backend, *, data: DataSubItem):
    return take(all, {dtype.prop.place: data.patch})


@commands.before_export.register(Context, Ref, Backend)
def before_export(
    context: Context,
    dtype: Ref,
    backend: Backend,
    *,
    data: DataSubItem,
) -> dict:
    # If patch is None, it means that parent was set to null, meaning all children should also be set to null

    if data.patch is None:
        patch = {}

        if not dtype.inherited:
            patch[f"{dtype.prop.place}._id"] = None

        for prop in dtype.properties.values():
            value = commands.before_export(
                context,
                prop.dtype,
                backend,
                data=data,
            )
            patch.update(value)
        return patch

    patch = flatten_value(data.patch, dtype.prop)
    return {f"{dtype.prop.place}.{k}": v for k, v in patch.items() if k}


@commands.before_export.register(Context, ExternalRef, Backend)
def before_export(
    context: Context,
    dtype: ExternalRef,
    backend: Backend,
    *,
    data: DataSubItem,
) -> dict:
    # If patch is None, it means that parent was set to null, meaning all children should also be set to null
    if data.patch is None:
        patch = {}
        if not dtype.inherited:
            if dtype.explicit or not dtype.model.external.unknown_primary_key:
                for ref_prop in dtype.refprops:
                    patch[f"{dtype.prop.place}.{ref_prop.name}"] = None
            else:
                patch[f"{dtype.prop.place}._id"] = None

        for prop in dtype.properties.values():
            value = commands.before_export(
                context,
                prop.dtype,
                backend,
                data=data,
            )
            patch.update(value)

        return patch

    patch = flatten_value(data.patch, dtype.prop)
    return {f"{dtype.prop.place}.{k}": v for k, v in patch.items() if k}


@commands.before_export.register(Context, Denorm, Backend)
def before_export(
    context: Context,
    dtype: Denorm,
    backend: Backend,
    *,
    data: DataSubItem,
) -> dict:
    # If patch is None, it means that all children should be set to null
    if data.patch is None:
        patch = commands.before_export(context, dtype.rel_prop.dtype, backend, data=data)
        return {
            key.replace(dtype.rel_prop.place, dtype.prop.place): value
            for key, value in patch.items()
            if key != dtype.rel_prop.place
        }

    patch = flatten_value(data.patch, dtype.prop)
    key = dtype.prop.place.split(".", maxsplit=1)[-1]
    if patch.get(key):
        return {dtype.prop.place: patch.get(key)}
    else:
        return {}


@commands.before_export.register(Context, Object, Backend)
def before_export(
    context: Context,
    dtype: Object,
    backend: Backend,
    *,
    data: DataSubItem,
) -> dict:
    patch = {}
    # If patch is None, then just set every child to None
    if data.patch is None:
        for prop in dtype.properties.values():
            value = commands.before_export(
                context,
                prop.dtype,
                backend,
                data=data,
            )
            patch.update(value)
        return patch

    for prop in dtype.properties.values():
        value = commands.before_export(
            context,
            prop.dtype,
            backend,
            data=data[prop.name],
        )
        patch.update(value)
    return patch


@commands.validate_export_output.register(Context, Backend, type(None))
def validate_export_output(context: Context, backend: Backend, output):
    cli_error("Output argument is required (`--output`).")


@commands.validate_export_output.register(Context, Format, type(None))
def validate_export_output(context: Context, backend: Format, output):
    cli_error("Output argument is required (`--output`).")


@commands.validate_export_output.register(Context, Backend, object)
def validate_export_output(context: Context, backend: Backend, output):
    return


@commands.validate_export_output.register(Context, Format, object)
def validate_export_output(context: Context, fmt: Format, output):
    return


@commands.build_data_patch_for_export.register(Context, Model)
def build_data_patch_for_export(context: Context, model: Model, *, given: dict, access: Access) -> dict:
    props = take(model.properties).values()
    props = [prop for prop in props if prop.access >= access]

    patch = {}
    for prop in props:
        value = build_data_patch_for_export(context, prop.dtype, given=given.get(prop.name, NA), access=access)
        if value is not NA:
            patch[prop.name] = value
    return patch


@commands.build_data_patch_for_export.register(Context, Property)
def build_data_patch_for_export(context: Context, prop: Property, *, given: dict, access: Access) -> dict:
    value = build_data_patch_for_export(context, prop.dtype, given=given.get(prop.name, NA), access=access)
    if value is not NA:
        return {prop.name: value}
    else:
        return {}


@commands.build_data_patch_for_export.register(Context, DataType)
def build_data_patch_for_export(
    context: Context, dtype: DataType, *, given: Optional[object], access: Access
) -> Union[dict, NotAvailable]:
    if given is NA:
        given = dtype.default
    return given


@commands.build_data_patch_for_export.register(Context, Object)
def build_data_patch_for_export(
    context: Context, dtype: Object, *, given: Optional[dict], access: Access
) -> Union[dict, NotAvailable]:
    props = take(dtype.properties).values()

    props = [prop for prop in props if prop.access >= access]

    patch = {}
    for prop in props:
        value = build_data_patch_for_export(
            context, prop.dtype, given=given.get(prop.name, NA) if given else NA, access=access
        )
        if value is not NA:
            patch[prop.name] = value
    return patch or NA


@commands.build_data_patch_for_export.register(Context, Ref)
def build_data_patch_for_export(
    context: Context, dtype: Ref, *, given: Optional[dict], access: Access
) -> Union[dict, NotAvailable]:
    if given is None:
        return given

    patch = {"_id": given.get("_id", NA)}

    props = take(dtype.properties).values()
    props = [prop for prop in props if prop.access >= access]

    for prop in props:
        value = build_data_patch_for_export(
            context, prop.dtype, given=given.get(prop.name, NA) if given else NA, access=access
        )
        if value is not NA:
            patch[prop.name] = value
    return patch or NA


@commands.build_data_patch_for_export.register(Context, ExternalRef)
def build_data_patch_for_export(
    context: Context, dtype: ExternalRef, *, given: Optional[dict], access: Access
) -> Union[dict, NotAvailable]:
    if given is None:
        return given

    patch = {}

    refprops = [prop for prop in dtype.refprops if prop.access >= access]
    for prop in refprops:
        value = build_data_patch_for_export(
            context, prop.dtype, given=given.get(prop.name, NA) if given else NA, access=access
        )
        if value is not NA:
            patch[prop.name] = value

    props = take(dtype.properties).values()
    props = [prop for prop in props if prop.access >= access]

    for prop in props:
        value = build_data_patch_for_export(
            context, prop.dtype, given=given.get(prop.name, NA) if given else NA, access=access
        )
        if value is not NA:
            patch[prop.name] = value
    return patch or NA


@commands.build_data_patch_for_export.register(Context, Inherit)
def build_data_patch_for_export(
    context: Context, dtype: Inherit, *, given: Optional[object], access: Access
) -> Union[dict, NotAvailable]:
    # Needs to be implemented when it's possible to modify Inherit type
    return NA


@commands.build_data_patch_for_export.register(Context, BackRef)
def build_data_patch_for_export(
    context: Context, dtype: BackRef, *, given: Optional[dict], access: Access
) -> Union[dict, NotAvailable]:
    return NA


@commands.build_data_patch_for_export.register(Context, Array)
def build_data_patch_for_export(
    context: Context, dtype: Array, *, given: Optional[Union[list, NotAvailable]], access: Access
) -> Union[list, None, NotAvailable]:
    if dtype.items.access < access:
        return NA

    if given is NA:
        return []

    if given is None:
        return []

    return [build_data_patch_for_export(context, dtype.items.dtype, given=value, access=access) for value in given]


@commands.build_data_patch_for_export.register(Context, File)
def build_data_patch_for_export(
    context: Context, dtype: File, *, given: Optional[dict], access: Access
) -> Union[dict, NotAvailable]:
    given = {
        "_id": given.get("_id", None) if given else None,
        "_content_type": given.get("_content_type", None) if given else None,
        "_content": given.get("_content", NA) if given else NA,
    }
    given = {k: v for k, v in given.items() if v is not NA}
    return given or NA
