from spinta import commands
from spinta.components import Context
from spinta.exceptions import RefPropTypeMissmatch
from spinta.types.datatype import ExternalRef, Ref


@commands.check.register(Context, Ref)
def check(context: Context, dtype: Ref):
    for prop in dtype.properties.values():
        commands.check(context, prop)


@commands.check.register(Context, ExternalRef)
def check(context: Context, dtype: ExternalRef):
    for prop in dtype.properties.values():
        commands.check(context, prop)
        for ref_prop in dtype.refprops:
            if ref_prop.name == prop.name:
                if not isinstance(ref_prop.dtype, type(prop.dtype)):
                    raise RefPropTypeMissmatch(prop.dtype, refprop=ref_prop.name, required_type=ref_prop.dtype.name, given_type=prop.dtype.name)
