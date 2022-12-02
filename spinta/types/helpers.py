from __future__ import annotations

import re

from typing import TYPE_CHECKING, Iterable

from spinta import exceptions
from spinta.components import Config
from spinta.components import Context
from spinta.components import Model
from spinta.components import Property
from spinta.exceptions import BackendNotFound
from spinta.exceptions import InvalidName

if TYPE_CHECKING:
    from spinta.types.datatype import DataType

upper_camel_case = re.compile(r'^[a-z][a-z0-9]+(/[a-z][a-z0-9]*)+(/([A-Z][a-z0-9]+)*)$')
snake_case = re.compile(r'^[a-z][a-z0-9]+(_[a-z0-9]+)*$')


def check_no_extra_keys(dtype: DataType, schema: Iterable, data: Iterable):
    unknown = set(data) - set(schema)
    if unknown:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(
                dtype.prop,
                property=f'{dtype.prop.place}.{prop}',
            )
            for prop in sorted(unknown)
        )


def set_dtype_backend(dtype: DataType):
    if dtype.backend:
        backends = dtype.prop.model.manifest.store.backends
        if dtype.backend not in backends:
            raise BackendNotFound(dtype, name=dtype.backend)
        dtype.backend = backends[dtype.backend]
    else:
        dtype.backend = dtype.prop.model.backend


def check_model_name(context: Context, model: Model):
    config: Config = context.get('config')
    if config.check_names:
        name = model.name
        if name not in ['_ns', '_txn', '_schema', '_schema/version']:
            if upper_camel_case.match(name) is None:
                raise InvalidName(
                    class_name=model.__class__.__name__,
                    name=name
                )


def check_property_name(context: Context, prop: Property):
    config: Config = context.get('config')
    if config.check_names:
        name = prop.name
        if name not in ['_op', '_type', '_id', '_revision', '_txn', '_cid', '_created', '_where']:
            if snake_case.match(name) is None:
                raise InvalidName(
                    class_name=prop.__class__.__name__,
                    name=name
                )
