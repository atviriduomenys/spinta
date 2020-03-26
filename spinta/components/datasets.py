from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

import pathlib

from spinta.core.ufuncs import Expr
from spinta.core.access import Access
from spinta.core.datasets import Sector, Level
from spinta.components.backends import Backend
from spinta.components.manifests import Node, Manifest, MetaData

if TYPE_CHECKING:
    from spinta.components.datasets import Model, Property


class Owner(MetaData):
    """Dataset or project owner."""

    sector: Sector = None
    logo: pathlib.Path = None

    schema = {
        'sector': {'type': 'string', 'choices': Sector},
        'logo': {'type': 'path'},
    }


class Impact(Node):
    project: Project
    year: int
    users: int = None
    revenue: int = None
    employees: int = None

    schema = {
        'project': {'parent': True},
        'year': {'type': 'integer', 'required': True},
        'users': {'type': 'integer'},
        'revenue': {'type': 'number'},
        'employees': {'type': 'integer'},
    }


class Project(MetaData):
    manifest: Manifest
    owner: Owner = None
    coderepo: str = None
    website: str = None
    impact: Impact = None

    schema = {
        'manifest': {'parent': True},
        'owner': {'type': 'ref', 'ref': 'manifest.nodes.owner'},
        'coderepo': {'type': 'url'},
        'website': {'type': 'url'},
        'impact': {
            'type': 'array',
            'factory': list,
            'items': {
                'type': 'comp',
                'group': 'datasets',
                'ctype': 'impact',
            },
        },
    }


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


class External(Node):
    title: str
    description: str

    schema = {
        'title': {'type': 'string'},
        'description': {'type': 'string'},
    }


class Resource(External):
    dataset: Dataset
    backend: Backend = None

    schema = {
        'dataset': {'parent': True},
        'type': {
            'type': 'ref',
            'ref': 'context.config.components.backends',
            'resolve': False,
        },
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
        'source': {'type': 'string', 'default': None},
        'prepare': {'type': 'spyna', 'default': None},
        'pkeys': {
            'type': 'array',
            'force': True,
            'items': {
                'type': 'ref',
                'ref': 'model.properties',
            },
        }
    }


class Attribute(External):
    property: Property
    source: str
    prepare: Expr

    schema = {
        'property': {'parent': True},
        'source': {'type': 'string', 'default': None},
        'prepare': {'type': 'spyna', 'default': None},
    }
