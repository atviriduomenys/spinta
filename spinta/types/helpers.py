from __future__ import annotations


from typing import TYPE_CHECKING, Iterable

from spinta import exceptions
from spinta.components import Config
from spinta.components import Context
from spinta.components import Model
from spinta.components import Property
from spinta.exceptions import BackendNotFound
from spinta.exceptions import InvalidName
from spinta.utils.naming import is_valid_model_name, is_valid_property_name

if TYPE_CHECKING:
    from spinta.types.datatype import DataType


SPECIAL_PROPERTY_NAMES = {"_id", "_revision", "_created", "_updated", "_label"}

RESERVED_PROPERTY_NAMES = {
    *SPECIAL_PROPERTY_NAMES,
    "_op",
    "_type",
    "_txn",
    "_cid",
    "_where",
    "_base",
    "_uri",
    "_page",
    "_same_as",
}


def check_no_extra_keys(dtype: DataType, schema: Iterable, data: Iterable):
    unknown = set(data) - set(schema)
    if unknown:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(
                dtype.prop,
                property=f"{dtype.prop.place}.{prop}",
            )
            for prop in sorted(unknown)
        )


def set_dtype_backend(dtype: DataType):
    if dtype.backend:
        backends = dtype.prop.model.manifest.store.backends
        if isinstance(dtype.backend, str):
            if dtype.backend not in backends:
                raise BackendNotFound(dtype, name=dtype.backend)
            dtype.backend = backends[dtype.backend]
    else:
        dtype.backend = dtype.prop.model.backend


def check_model_name(context: Context, model: Model):
    config: Config = context.get("config")
    if config.check_names:
        if "/" in model.name:
            name = model.name.rsplit("/", 1)[1]
        else:
            name = model.name
        if name not in ["_ns", "_txn", "_schema", "_schema/Version"]:
            if not is_valid_model_name(name):
                raise InvalidName(model, name=name, type="model")


def check_property_name(context: Context, prop: Property):
    config: Config = context.get("config")
    if config.check_names:
        if not prop.explicitly_given and prop.name.startswith("_") and prop.name in RESERVED_PROPERTY_NAMES:
            return
        # Check for explicitly given properties starting with underscore that are in SPECIAL_DSA_PROPERTIES
        if prop.explicitly_given and prop.name.startswith("_") and prop.name in SPECIAL_PROPERTY_NAMES:
            return
        if is_valid_property_name(prop.name):
            return
        raise InvalidName(prop, name=prop.name, type="property")
