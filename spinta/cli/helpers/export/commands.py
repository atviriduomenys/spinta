from spinta import commands
from spinta.backends import Backend
from spinta.cli.helpers.errors import cli_error
from spinta.components import Context, Model, DataSubItem, Property
from spinta.exceptions import NotImplementedFeature
from spinta.formats.components import Format
from spinta.types.datatype import DataType, Ref, ExternalRef, Denorm, Object
from spinta.utils.data import take
from spinta.utils.nestedstruct import flatten_value


@commands.export_data.register(Context, Model, Format)
def export_data(context: Context, model: Model, fmt: Format, *, data: object, **kwargs):
    cli_error(
        f"Export for {type(fmt)!r} Format is supported yet."
    )


@commands.export_data.register(Context, Model, Backend)
def export_data(context: Context, model: Model, backend: Backend, *, data: object, **kwargs):
    cli_error(
        f"Export for {backend.type!r} Backend is supported yet."
    )


@commands.before_export.register(Context, Model, Backend)
def before_export(
    context: Context,
    model: Model,
    backend: Backend,
    *,
    data: DataSubItem
):
    patch = take(['_id'], data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
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
def before_export(
    context: Context,
    prop: Property,
    backend: Backend,
    *,
    data: DataSubItem
):
    return before_export(context, prop.dtype, backend, data=data)


@commands.before_export.register(Context, DataType, Backend)
def before_export(
    context: Context,
    dtype: DataType,
    backend: Backend,
    *,
    data: DataSubItem
):
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
            patch[f'{dtype.prop.place}._id'] = None

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
    return {
        f'{dtype.prop.place}.{k}': v for k, v in patch.items() if k
    }


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
                    patch[f'{dtype.prop.place}.{ref_prop.name}'] = None
            else:
                patch[f'{dtype.prop.place}._id'] = None

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
    return {
        f'{dtype.prop.place}.{k}': v for k, v in patch.items() if k
    }


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
            for key, value in patch.items() if key != dtype.rel_prop.place
        }

    patch = flatten_value(data.patch, dtype.prop)
    key = dtype.prop.place.split('.', maxsplit=1)[-1]
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
    cli_error(
        "Output argument is required (`--output`)."
    )


@commands.validate_export_output.register(Context, Format, type(None))
def validate_export_output(context: Context, backend: Format, output):
    cli_error(
        "Output argument is required (`--output`)."
    )


@commands.validate_export_output.register(Context, Backend, object)
def validate_export_output(context: Context, backend: Backend, output):
    return


@commands.validate_export_output.register(Context, Format, object)
def validate_export_output(context: Context, fmt: Format, output):
    return
