from __future__ import annotations

from typing import Dict
from typing import List

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from spinta.backends.components import Backend
from spinta.components import MetaData
from spinta.components import Model
from spinta.components import Node
from spinta.components import Property
from spinta.core.enums import Access
from spinta.core.ufuncs import Expr
from spinta.datasets.enums import Level
from spinta.manifests.components import Manifest
from spinta.types.owner import Owner
from spinta.types.project import Project


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
    dataset: Dataset        # dataset
    resource: Resource      # resource
    model: Model            # model
    pkeys: List[Property]   # model.ref
    source: str             # model.source
    prepare: Expr           # model.prepare

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
    prop: Property          # property
    name: str               # property.source
    prepare: Expr = None    # property.prepare

    schema = {
        'prop': {'parent': True},
        'name': {'default': None},
        'prepare': {'type': 'spyna', 'default': None},
    }
