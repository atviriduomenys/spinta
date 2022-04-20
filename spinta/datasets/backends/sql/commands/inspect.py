from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set
from typing import TypedDict

import frictionless
from sqlalchemy.engine.base import Engine as SaEngine

from spinta import commands
from spinta import spyna
from spinta.components import Context
from spinta.components import Model
from spinta.components import Property
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.components import Engine
from spinta.datasets.backends.sql.ufuncs.components import SqlResource
from spinta.datasets.backends.sql.frictionless import GeoSqlStorage
from spinta.datasets.components import Resource
from spinta.exceptions import UnexpectedFormulaResult
from spinta.manifests.components import ManifestSchema
from spinta.manifests.components import NodeSchema
from spinta.manifests.helpers import entity_to_schema
from spinta.manifests.helpers import model_to_schema
from spinta.utils.imports import full_class_name
from spinta.utils.naming import Deduplicator
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def _ensure_list(value: Optional[Any]):
    if value is None:
        return value
    elif isinstance(value, list):
        return value
    else:
        return [value]


class _FrictionlessForeignKeyReference(TypedDict):
    fields: List[str]
    resource: str


class _FrictionlessForeignKey(TypedDict):
    fields: List[str]
    reference: _FrictionlessForeignKeyReference


@dataclass()
class _ForeignKey:
    resource: frictionless.Resource
    field: frictionless.Field
    foreign_key: _FrictionlessForeignKey


@dataclass()
class _CompositeForeignKey:
    resource: frictionless.Resource
    foreign_key: _FrictionlessForeignKey


_ForeignKeysByProp = Dict[str, _FrictionlessForeignKey]
_ForeignKeysByField = Dict[str, _FrictionlessForeignKey]


def _read_frictionless_field(field: frictionless.Field) -> Dict[str, Any]:
    return {
        'type': field.type,
        'external': {
            'name': field.name,
        }
    }


@commands.inspect.register(Context, Sql, Model, _ForeignKey)
def inspect(
    context: Context,
    backend: Sql,
    model: Model,
    source: _ForeignKey,
) -> NodeSchema:
    ref_model_name = to_model_name(source.foreign_key['reference']['resource'])
    if ref_model_name:
        if model.external:
            ref_model_name = f'{model.external.dataset.name}/{ref_model_name}'
    else:
        # If ref_model_name is empty string, it means, this is a self reference.
        ref_model_name = model.name

    return {
        'type': 'ref',
        'model': ref_model_name,
        'refprops': [
            to_property_name(ref_field)
            for ref_field in source.foreign_key['reference']['fields']
        ],
        'external': {
            'name': source.field.name,
        }
    }


@commands.inspect.register(Context, Sql, Model, _CompositeForeignKey)
def inspect(
    context: Context,
    backend: Sql,
    model: Model,
    source: _CompositeForeignKey,
) -> NodeSchema:
    ref_model_name = to_model_name(source.foreign_key['reference']['resource'])
    if ref_model_name:
        if model.external:
            ref_model_name = f'{model.external.dataset.name}/{ref_model_name}'
    else:
        # If ref_model_name is empty string, it means, this is a self reference.
        ref_model_name = model.name

    return {
        'type': 'ref',
        'model': ref_model_name,
        'refprops': [
            to_property_name(ref_field)
            for ref_field in source.foreign_key['reference']['fields']
        ],
    }


@commands.inspect.register(Context, Sql, Resource, _ForeignKey)
def inspect(
    context: Context,
    backend: Sql,
    resource: Resource,
    source: _ForeignKey,
) -> NodeSchema:
    ref_model_name = source.foreign_key['reference']['resource']
    if ref_model_name:
        ref_model_name = to_model_name(ref_model_name)
    else:
        # If ref_model_name is empty string, it means, this is a self reference.
        ref_model_name = to_model_name(source.resource.name)
    ref_model_name = f'{resource.dataset.name}/{ref_model_name}'
    return {
        'type': 'ref',
        'model': ref_model_name,
        'refprops': [
            to_property_name(ref_field)
            for ref_field in source.foreign_key['reference']['fields']
        ],
        'external': {
            'name': source.field.name,
        }
    }


@commands.inspect.register(Context, Sql, Property, _ForeignKey)
def inspect(
    context: Context,
    backend: Sql,
    prop: Property,
    source: _ForeignKey,
) -> NodeSchema:
    ref_model_name = source.foreign_key['reference']['resource']
    if ref_model_name:
        ref_model_name = to_model_name(ref_model_name)
    else:
        # If ref_model_name is empty string, it means, this is a self reference.
        ref_model_name = to_model_name(source.resource.name)
    ref_model_name = f'{prop.model.external.dataset.name}/{ref_model_name}'
    return {
        'type': 'ref',
        'model': ref_model_name,
        'refprops': [
            to_property_name(ref_field)
            for ref_field in source.foreign_key['reference']['fields']
        ],
        'external': {
            'name': source.field.name,
            'prepare': prop.external.prepare,
        },
        'access': prop.given.access,
        'title': prop.title,
        'description': prop.description,
    }


@commands.inspect.register(Context, Sql, Property, frictionless.Field)
def inspect(
    context: Context,
    backend: Sql,
    prop: Property,
    source: frictionless.Field,
) -> NodeSchema:
    return {
        'type': source.type,
        'external': {
            'name': source.name,
            'prepare': prop.external.prepare,
        },
        'access': prop.given.access,
        'title': prop.title,
        'description': prop.description,
    }


@commands.inspect.register(Context, Sql, Model, frictionless.Field)
def inspect(
    context: Context,
    backend: Sql,
    model: Model,
    source: frictionless.Field,
) -> NodeSchema:
    return {
        'type': source.type,
        'external': {
            'name': source.name,
        }
    }


@commands.inspect.register(Context, Sql, Resource, frictionless.Field)
def inspect(
    context: Context,
    backend: Sql,
    resource: Resource,
    source: frictionless.Field,
) -> NodeSchema:
    return {
        'type': source.type,
        'external': {
            'name': source.name,
        }
    }


@commands.inspect.register(Context, Sql, Property, type(None))
def inspect(
    context: Context,
    backend: Sql,
    prop: Property,
    source: frictionless.Field,
) -> NodeSchema:
    return {
        'type': prop.dtype.name,
        'external': {
            'name': prop.name,
            'prepare': prop.external.prepare,
        }
    }


@commands.inspect.register(Context, Sql, Model, frictionless.Resource)
def inspect(
    context: Context,
    backend: Sql,
    model: Model,
    source: frictionless.Resource,
) -> Iterator[ManifestSchema]:
    schema = source.schema
    eid, data = model_to_schema(model)

    if model.external:
        data['external'] = entity_to_schema(model.external)[1]
        if not data['external']['pk']:
            data['external']['pk'] = [
                to_property_name(p)
                for p in _ensure_list(schema.primary_key)
            ]
    else:
        data['external'] = {}

    data['external']['name'] = source.name

    source_props: Set[str] = set()
    model_props = {
        prop.external.name: prop
        for prop in model.properties.values()
        if prop.external
    }

    foreign_keys = _get_foreign_keys_by_field(schema.foreign_keys)

    props = {}
    deduplicate = Deduplicator('_{}')
    for field in schema.fields:
        source_props.add(field.name)

        if field.name in model_props:
            prop = model_props[field.name]
            node = prop
            name = prop.name
            deduplicate(name)
        else:
            node = model
            name = to_property_name(field.name)
            name = deduplicate(name)

        if field.name in foreign_keys:
            fk = _ForeignKey(source, field, foreign_keys[field.name])
            props[name] = commands.inspect(context, backend, node, fk)
        else:
            props[name] = commands.inspect(context, backend, node, field)

    for field, prop in model_props.items():
        if field not in source_props:
            props[prop.name] = commands.inspect(context, backend, prop, None)

    for field, fk in foreign_keys.items():
        if field not in source_props:
            cfk = _CompositeForeignKey(source, fk)
            props[field] = commands.inspect(context, backend, model, cfk)

    data['properties'] = props

    yield eid, data


def _get_foreign_keys_by_field(
    foreign_keys: List[_FrictionlessForeignKey],
) -> _ForeignKeysByField:
    result: _ForeignKeysByField = {}
    for fk in foreign_keys:
        name = '_'.join(fk['fields'])
        result[name] = fk
    return result


@commands.inspect.register(Context, Sql, Resource, frictionless.Resource)
def inspect(
    context: Context,
    backend: Sql,
    resource: Resource,
    source: frictionless.Resource,
) -> Iterator[ManifestSchema]:
    """Add a new model"""
    schema = source.schema
    eid, data = model_to_schema(None)

    deduplicate: Deduplicator = context.get('deduplicate.model')
    data['name'] = deduplicate(to_model_name(source.name))
    data['external'] = {
        'name': source.name,
        'pk': [
            to_property_name(p)
            for p in _ensure_list(schema.primary_key)
        ]
    }

    foreign_keys = _get_foreign_keys_by_field(schema.foreign_keys)

    props = {}
    deduplicate = Deduplicator('_{}')
    for field in schema.fields:
        name = deduplicate(to_property_name(field.name))
        if field.name in foreign_keys:
            fk = _ForeignKey(source, field, foreign_keys[field.name])
            props[name] = commands.inspect(context, backend, resource, fk)
        else:
            props[name] = commands.inspect(context, backend, resource, field)

    data['properties'] = props

    yield eid, data


@commands.inspect.register(Context, Sql, Model, type(None))
def inspect(
    context: Context,
    backend: Sql,
    model: Model,
    source: None,
) -> Iterator[ManifestSchema]:
    """Keep an existing model that is no longer in the resource"""
    yield model_to_schema(model)


@commands.inspect.register(Context, Sql, Resource, type(None))
def inspect(
    context: Context,
    backend: Sql,
    resource: Resource,
    source: None,
) -> Iterator[ManifestSchema]:
    if resource.prepare:
        env = SqlResource(context).init(backend.config['dsn'])
        engine = env.resolve(resource.prepare)
        engine: Engine = env.execute(engine)
        if not isinstance(engine, Engine):
            raise UnexpectedFormulaResult(
                resource,
                formula=spyna.unparse(resource.prepare),
                expected=full_class_name(SaEngine),
                received=full_class_name(engine),
            )
    else:
        engine = Engine(backend.config['dsn'])

    resource_models: Set[str] = set()
    manifest_models = {
        model.external.name: model
        for model in resource.models.values()
        if model.external and model.external.name
    }

    storage = GeoSqlStorage(
        engine=engine.create(),
        namespace=engine.schema,
    )
    package = frictionless.Package.from_storage(storage)
    with context:
        context.bind('deduplicate.model', Deduplicator)

        for i, source_ in enumerate(package.resources):
            model = manifest_models.get(source_.name)
            resource_models.add(source_.name)
            schemas = commands.inspect(context, backend, model or resource, source_)
            for eid, schema in schemas:
                if 'external' not in schema:
                    schema['external'] = {}
                schema['external']['dataset'] = resource.dataset.name
                schema['external']['resource'] = resource.name
                if model is None:
                    schema['name'] = resource.dataset.name + '/' + schema['name']
                yield eid, schema

        for source_, model in manifest_models.items():
            if source_ not in resource_models:
                for eid, schema in commands.inspect(context, backend, model, None):
                    if 'external' not in schema:
                        schema['external'] = {}
                    schema['external']['dataset'] = resource.dataset.name
                    schema['external']['resource'] = resource.name
                    yield eid, schema
