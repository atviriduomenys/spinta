from __future__ import annotations

import re

from typing import TYPE_CHECKING, Iterable

from spinta import exceptions
from spinta.components import Config, Context, Model, Property
from spinta.exceptions import BackendNotFound, InvalidName

if TYPE_CHECKING:
    from spinta.types.datatype import DataType


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
        if not name.startswith('_'):
            camel_case = re.compile(r'[A-Z][a-z]+')
            if not camel_case.findall(name):
                raise InvalidName(
                    class_name=model.__class__.__name__,
                    name=name
                )


def check_property_name(context: Context, prop: Property):
    config: Config = context.get('config')
    if config.check_names:
        name = prop.name
        if not name.startswith('_'):
            snake_case = re.compile(r'[a-z0-1_]+')
            if not snake_case.match(name):
                raise InvalidName(
                    class_name=prop.__class__.__name__,
                    name=name
                )
