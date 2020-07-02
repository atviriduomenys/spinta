from __future__ import annotations

from typing import List, Dict

import sqlalchemy as sa

from sqlalchemy.engine.base import Engine

from spinta.core.ufuncs import Expr
from spinta.components import Node, MetaData, Model, Property
from spinta.manifests.components import Manifest
from spinta.backends.components import Backend
from spinta.core.enums import Access
from spinta.datasets.enums import Level
from spinta.core.enums import Access
from spinta.types.owner import Owner
from spinta.types.project import Project
from spinta.datasets.enums import Level


class Dataset(MetaData):
    """DCAT compatible metadata about an external dataset.

    DCAT (Data Catalog Vocabulary) - https://w3c.github.io/dxwg/dcat/
    """

    manifest: Manifest
    owner: Owner = None
    level: Level = 3
    access: Access = Access.private
    website: str = None
    projects: List[Project] = None
    resources: Dict[str, Resource] = None

    schema = {
        'type': {'type': 'string', 'required': True},
        'name': {'type': 'string', 'required': True},
        'path': {'type': 'string'},
        'version': {
            'type': 'object',
            'items': {
                'number': {'type': 'integer', 'required': True},
                'date': {'type': 'date', 'required': True},
            },
        },

        'manifest': {'parent': True},
        'owner': {'type': 'ref', 'ref': 'manifest.nodes.owner'},
        'level': {'type': 'integer', 'choices': Level},
        'access': {'type': 'string', 'choices': Access},
        'website': {'type': 'url'},
        'projects': {
            'type': 'array',
            'factory': list,
            'items': {
                'type': 'ref',
                'ref': 'manifest.nodes.project',
            },
        },
        'resources': {
            'type': 'object',
            'values': {
                'type': 'comp',
                'group': 'external',
                'ctype': 'resource',
            }
        },
    }


class ExternalBackend(Backend):
    engine: Engine = None
    schema: sa.MetaData = None


class External(Node):
    pass


class Resource(External):
    title: str
    description: str
    dataset: Dataset
    backend: ExternalBackend = None

    schema = {
        'type': {'type': 'string'},
        'dataset': {'parent': True},
        'external': {'type': 'string'},
        'prepare': {'type': 'spyna'},
        'level': {
            'type': 'integer',
            'choices': Level,
            'inherit': 'dataset.level',
        },
        'access': {'type': 'string', 'choices': Access, 'inherit': 'dataset.access'},
        'title': {'type': 'string'},
        'description': {'type': 'string'},
        'backend': {'type': 'ref', 'ref': 'context.store.backends'},
    }


class Entity(External):
    model: Model
    dataset: Dataset
    resource: Resource
    source: str
    prepare: Expr
    pkeys: List[Property]

    schema = {
        'model': {'parent': True},
        'dataset': {'type': 'ref', 'ref': 'context.nodes.dataset'},
        'resource': {'type': 'ref', 'ref': 'dataset.resources'},
        'name': {'type': 'string', 'default': None},
        'prepare': {'type': 'spyna', 'default': None},
        'params': {
            'type': 'array',
            'items': {'type': 'object'},
        },
        'pk': {
            'type': 'array',
            'force': True,
            'attr': 'pkeys',
            'items': {
                'type': 'ref',
                'ref': 'model.properties',
            },
        }
    }


class Attribute(External):
    property: Property
    name: str
    prepare: Expr = None

    schema = {
        'property': {'parent': True},
        'name': {'required': True},
        'prepare': {'type': 'spyna', 'default': None},
    }
