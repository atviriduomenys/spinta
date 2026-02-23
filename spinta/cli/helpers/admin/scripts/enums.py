import dataclasses
import json
from typing import Iterator

from click import echo
from multipledispatch import dispatch

from spinta import commands
from spinta.backends import Backend
from spinta.backends.postgresql.components import PostgreSQL
from spinta.cli.helpers.script.helpers import ensure_store_is_loaded
from spinta.components import Context, Model, Property
from spinta.manifests.components import Manifest

import sqlalchemy as sa


@dispatch(Backend, Property)
def gather_unique_property_values():
    raise NotImplementedError


@dispatch(PostgreSQL, Property)
def gather_unique_property_values(backend: PostgreSQL, prop: Property):
    table = backend.get_table(prop)
    column = backend.get_column(table, prop)
    result = backend.engine.execute(sa.select(column).distinct()).scalars().all()
    return result


@dispatch(Property)
def gather_unique_property_values(prop: Property):
    return gather_unique_property_values(prop.dtype.backend, prop)


def get_models_with_enums(context: Context, manifest: Manifest) -> Iterator[Model]:
    for model in commands.get_models(context, manifest).values():
        for prop in model.flatprops.values():
            if prop.enum:
                yield model
                break


@dataclasses.dataclass
class InvalidEnumProperty:
    prop: Property
    invalid_values: list = dataclasses.field(default_factory=list)

    def add_invalid_value(self, value: object):
        if value not in self.invalid_values:
            self.invalid_values.append(value)


@dataclasses.dataclass
class InvalidEnumModel:
    model: Model
    enum_props: dict[str, InvalidEnumProperty] = dataclasses.field(default_factory=dict)

    def add_invalid_value(self, prop: Property, value: object):
        self.get_prop(prop).add_invalid_value(value)

    def get_prop(self, prop: Property) -> InvalidEnumProperty:
        if prop.place not in self.enum_props:
            self.enum_props[prop.place] = InvalidEnumProperty(prop=prop)
        return self.enum_props[prop.place]


def gather_distinct_values(context: Context, destructive: bool, **kwargs):
    store = ensure_store_is_loaded(context)
    manifest = store.manifest
    invalid_models = {}
    with context:
        for model in get_models_with_enums(context, manifest):
            enum_model = InvalidEnumModel(model=model)
            for prop in model.flatprops.values():
                if not prop.enum:
                    continue

                values = gather_unique_property_values(prop)
                for value in values:
                    if value is None and not prop.dtype.required:
                        continue
                    elif value is None:
                        enum_model.add_invalid_value(prop, value)
                        continue

                    if str(value) not in prop.enum:
                        enum_model.add_invalid_value(prop, value)

            if enum_model.enum_props:
                invalid_models[model.model_type()] = enum_model

    output = {}
    for key, model in sorted(invalid_models.items()):
        output[key] = {key: prop.invalid_values for key, prop in model.enum_props.items()}
    echo(json.dumps(output))
