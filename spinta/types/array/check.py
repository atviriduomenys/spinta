from spinta import commands
from spinta.components import Context
from spinta.exceptions import (
    IntermediateTableMappingInvalidType,
    IntermediateTableValueTypeMissmatch,
    IntermediateTableRefModelMissmatch,
    IntermediateTableRefPropertyModelMissmatch,
    IntermediateTableMissingMappingProperty,
)
from spinta.types.datatype import Array, Ref


@commands.check.register(Context, Array)
def check(context: Context, dtype: Array):
    if dtype.items is not None:
        commands.check(context, dtype.items)

    if dtype.model is not None:
        if dtype.left_prop is None:
            raise IntermediateTableMissingMappingProperty(dtype, side="left")

        if dtype.right_prop is None:
            raise IntermediateTableMissingMappingProperty(dtype, side="right")

        if not isinstance(dtype.left_prop.dtype, Ref):
            raise IntermediateTableMappingInvalidType(
                dtype, property_name=dtype.left_prop.name, property_type=dtype.left_prop.dtype.name
            )

        if not isinstance(dtype.right_prop.dtype, Ref):
            raise IntermediateTableMappingInvalidType(
                dtype, property_name=dtype.right_prop.name, property_type=dtype.right_prop.dtype.name
            )

        if dtype.items is not None and not isinstance(dtype.items.dtype, type(dtype.right_prop.dtype)):
            raise IntermediateTableValueTypeMissmatch(
                dtype, array_type=dtype.items.dtype.name, intermediate_type=dtype.right_prop.dtype.name
            )

        if dtype.left_prop.dtype.model != dtype.prop.model:
            raise IntermediateTableRefModelMissmatch(
                dtype, array_model=dtype.prop.model.name, left_model=dtype.left_prop.dtype.model.name
            )

        if dtype.items.dtype.model != dtype.right_prop.dtype.model:
            raise IntermediateTableRefPropertyModelMissmatch(
                dtype, array_ref_model=dtype.items.dtype.model.name, right_model=dtype.right_prop.dtype.model.name
            )
