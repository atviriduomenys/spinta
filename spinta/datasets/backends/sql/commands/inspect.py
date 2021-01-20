from typing import Any
from typing import Dict
from typing import Iterator
from typing import Tuple

import sqlalchemy as sa
from frictionless import Field
from frictionless import Package
from frictionless import Resource

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context
from spinta.datasets.backends.sql.components import Sql
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def _read_frictionless_field(field: Field):
    return {
        'type': field.type,
        'external': {
            'name': field.name,
        }
    }


def _read_frictionless_resource(
    backend: Backend,
    resource: Resource,
) -> Dict[str, Any]:
    return {
        'type': 'model',
        'name': to_model_name(resource.name),
        'backend': backend.name,
        'external': {
            'dataset': None,
            'resource': None,
            'name': resource.name,
            'pk': [],
        },
        'properties': {
            to_property_name(field.name): _read_frictionless_field(field)
            for field in resource.schema.fields
        },
    }


def _read_frictionless_package(
    backend: Backend,
    package: Package,
) -> Iterator[Tuple[int, Dict[str, Any]]]:
    for i, resource in enumerate(package.resources):
        yield i, _read_frictionless_resource(backend, resource)


@commands.inspect.register(Context, Manifest, Sql)
def inspect(context: Context, manifest: Manifest, backend: Sql):
    engine = sa.create_engine(backend.config['dsn'])
    package = Package.from_sql(engine=engine)
    schemas = _read_frictionless_package(backend, package)
    load_manifest_nodes(context, manifest, schemas)
