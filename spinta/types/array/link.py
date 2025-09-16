from __future__ import annotations

from typing import List

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.exceptions import (
    ModelReferenceNotFound,
    ModelReferenceKeyNotFound,
    InvalidIntermediateTableMappingRefCount,
    UnableToMapIntermediateTable,
    SameModelIntermediateTableMapping,
)
from spinta.types.datatype import Array, PartialArray, ArrayBackRef, Ref
from spinta.types.helpers import set_dtype_backend


@commands.link.register(Context, Array)
def link(context: Context, dtype: Array) -> None:
    set_dtype_backend(dtype)
    # In case of a dynamic array, dtype of items is not known.
    if dtype.items:
        commands.link(context, dtype.items.dtype)

    intermediate_model: str = dtype.model
    if intermediate_model is None:
        return

    if intermediate_model == dtype.prop.model.name:
        # Self reference.
        dtype.model = dtype.prop.model
    else:
        if not commands.has_model(context, dtype.prop.model.manifest, intermediate_model):
            raise ModelReferenceNotFound(dtype, ref=intermediate_model)
        dtype.model = commands.get_model(context, dtype.prop.model.manifest, intermediate_model)

    intermediate_model: Model = dtype.model
    if dtype.refprops:
        refprops = []
        raw_refprops: List[str] = dtype.refprops
        for rprop in raw_refprops:
            if rprop not in dtype.model.properties:
                raise ModelReferenceKeyNotFound(dtype, ref=rprop, model=dtype.model)
            refprops.append(dtype.model.properties[rprop])
        dtype.refprops = refprops
        dtype.explicit = True
    else:
        dtype.refprops = []

    dtype.left_prop, dtype.right_prop = _extract_intermediate_table_properties(dtype, intermediate_model)


def _compare_models(source: Model, target: str | Model) -> bool:
    if isinstance(target, Model):
        return source == target

    return source.name == target


def _extract_intermediate_table_properties(source: Array, intermediate_model: Model) -> (Property, Property):
    if source.refprops:
        if len(source.refprops) != 2:
            raise InvalidIntermediateTableMappingRefCount(source, ref_count=len(source.refprops))

        return source.refprops

    if intermediate_model.external and not intermediate_model.external.unknown_primary_key:
        if len(intermediate_model.external.pkeys) == 2 and all(
            isinstance(prop.dtype, Ref) for prop in intermediate_model.external.pkeys
        ):
            return intermediate_model.external.pkeys

    ref_properties = [prop for prop in intermediate_model.flatprops.values() if isinstance(prop.dtype, Ref)]
    if len(ref_properties) != 2:
        raise UnableToMapIntermediateTable(source, model=source.model.name)

    if all(_compare_models(source.prop.model, prop.dtype.model) for prop in ref_properties):
        raise SameModelIntermediateTableMapping(source)
    return ref_properties


@commands.link.register(Context, PartialArray)
def link(context: Context, dtype: PartialArray):
    dtype.prop.given.name = ""
    super_ = commands.link[Context, Array]
    return super_(context, dtype)


@commands.link.register(Context, ArrayBackRef)
def link(context: Context, dtype: ArrayBackRef):
    dtype.prop.given.name = ""

    set_dtype_backend(dtype)
    # In case of a dynamic array, dtype of items is not known.
    if dtype.items:
        commands.link(context, dtype.items.dtype)
