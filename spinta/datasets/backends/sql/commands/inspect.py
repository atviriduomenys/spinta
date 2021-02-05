from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import frictionless
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine as SaEngine

from spinta import commands
from spinta import spyna
from spinta.backends import Backend
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.components import Engine
from spinta.datasets.backends.sql.ufuncs.components import SqlResource
from spinta.datasets.components import Resource
from spinta.exceptions import UnexpectedFormulaResult
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.utils.imports import full_class_name
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def _ensure_list(value: Optional[Any]):
    if value is None:
        return value
    elif isinstance(value, list):
        return value
    else:
        return [value]


def _read_foreign_keys(
    model_name: str,
    resource: Optional[Resource],
    foreign_keys: List[Dict[str, Any]],
    properties: Dict[str, dict],
) -> Dict[str, Any]:
    for fk in foreign_keys:
        if len(fk['fields']) == 1:
            name = to_property_name(fk['fields'][0])
            prop = properties[name]
        else:
            name = to_property_name('_'.join(fk['fields']))
            prop = properties[name] = {}
            prop['prepare'] = ', '.join([
                to_property_name(f) for f in fk['fields']
            ])

        ref_model_name = to_model_name(fk['reference']['resource'])
        if ref_model_name:
            if resource:
                ref_model_name = f'{resource.dataset.name}/{ref_model_name}'
        else:
            # If ref_model_name is empty string, it means, this is a self
            # refrence.
            ref_model_name = model_name

        prop['type'] = 'ref'
        prop['model'] = ref_model_name
        prop['refprops'] = [
            to_property_name(f)
            for f in fk['reference']['fields']
        ]

    return properties


def _read_frictionless_field(field: frictionless.Field) -> Dict[str, Any]:
    return {
        'type': field.type,
        'external': {
            'name': field.name,
        }
    }


def _read_frictionless_resource(
    resource: Optional[Resource],
    backend: Backend,
    frictionless_resource: frictionless.Resource,
) -> Dict[str, Any]:
    schema = frictionless_resource.schema
    name = to_model_name(frictionless_resource.name)
    if resource:
        name = f'{resource.dataset.name}/{name}'
    return {
        'type': 'model',
        'name': name,
        'external': {
            'dataset': resource.dataset.name if resource else None,
            'resource': resource.name if resource else None,
            'name': frictionless_resource.name,
            'pk': [
                to_property_name(p)
                for p in _ensure_list(schema.primary_key)
            ],
        },
        'properties': _read_foreign_keys(
            name,
            resource,
            schema.foreign_keys,
            {
                to_property_name(field.name): _read_frictionless_field(field)
                for field in schema.fields
            },
        ),
    }


def _read_frictionless_package(
    resource: Optional[Resource],
    backend: Backend,
    package: frictionless.Package,
) -> Iterator[Tuple[int, Dict[str, Any]]]:
    for i, frictionless_resource in enumerate(package.resources):
        yield i, _read_frictionless_resource(
            resource,
            backend,
            frictionless_resource,
        )


@commands.inspect.register(Context, Manifest, Sql)
def inspect(context: Context, manifest: Manifest, backend: Sql):
    engine = sa.create_engine(backend.config['dsn'])
    package = frictionless.Package.from_sql(engine=engine)
    schemas = _read_frictionless_package(None, backend, package)
    load_manifest_nodes(context, manifest, schemas)


@commands.inspect.register(Context, Resource, Sql)
def inspect(context: Context, resource: Resource, backend: Sql):
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

    package = frictionless.Package.from_sql(
        engine=engine.create(),
        namespace=engine.schema,
    )
    schemas = _read_frictionless_package(resource, backend, package)
    load_manifest_nodes(
        context,
        resource.dataset.manifest,
        schemas,
        link=True,
    )
