from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array, Ref


@commands.check.register(Context, Array)
def check(context: Context, dtype: Array):
    if dtype.items is not None:
        commands.check(context, dtype.items)

    if dtype.model is not None:
        if dtype.left_prop is None:
            raise Exception("IF INTERMEDIATE MODEL GIVEN, LEFT PROP IS MUST")

        if dtype.right_prop is None:
            raise Exception("IF INTERMEDIATE MODEL GIVEN, RIGHT PROP IS MUST")

        if not isinstance(dtype.left_prop.dtype, Ref):
            raise Exception("INTERMEDIATE MODEL MAPPING PROP NEEDS TO BE REF TYPE")

        if not isinstance(dtype.right_prop.dtype, Ref):
            raise Exception("INTERMEDIATE MODEL MAPPING PROP NEEDS TO BE REF TYPE")

        if dtype.items is not None and not isinstance(dtype.items.dtype, Ref):
            raise Exception("INTERMEDIATE MODEL REQUIRES REF TYPE TO BE MAPPED FOR RETURN")

        if dtype.left_prop.dtype.model != dtype.prop.model:
            raise Exception("INTERMEDIATE MODEL LEFT PROP MODEL HAS TO BE SAME AS SOURCE")

        if dtype.items.dtype.model != dtype.right_prop.dtype.model:
            raise Exception("INTERMEDIATE MODEL RIGHT PROP DOES NOT MATCH REF MODEL WITH ITEM")
