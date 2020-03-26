from __future__ import annotations

from typing import Dict, List, Any

from spinta.core.access import Access
from spinta.core.ufuncs import Expr
from spinta.core.datasets import Level
from spinta.components.backends import Backend
from spinta.components.manifests import Node, Manifest, MetaData, Version
from spinta.components.datasets import Entity, Attribute


class Namespace(Node):
    parent: Namespace


class Base(Node):
    model: Model
    pkeys: List[Property]

    schema = {
        'model': {'type': 'string'},
        'pkey': {'type': 'array', 'force': True},
    }


class Model(MetaData):
    parent: Manifest
    unique: list
    base: Base
    backend: Backend
    version: Version = None
    properties: Dict[str, Property]
    flatprops: Dict[str, Property]
    leafprops: Dict[str, Property]
    external: Entity = None

    schema = {
        'manifest': {'parent': True},
        'base': {'type': 'object'},
        'endpoint': {'type': 'string'},

        # URI of this model in a known vocabulary
        'uri': {'type': 'string'},

        'level': {'type': 'string', 'inherit': True, 'choices': Level},
        'access': {'type': 'string', 'inherit': True, 'choices': Access},

        # Metadata about external data source
        'external': {
            'type': 'comp',
            'group': 'datasets',
            'cname': 'entity',
        },

        'properties': {
            'type': 'object',
            'factory': dict,
            'values': {
                'type': 'comp',
                'group': 'nodes',
                'ctype': 'property',
                'name': True,
                'mixed': {
                    'type': 'comp',
                    'group': 'types',
                    'attr': 'dtype',
                },
            },
        },
    }


class Property(Node):
    required: bool = False
    unique: bool = False
    prepare: Expr = None
    default: Any = None
    nullable: bool = True
    prepare: Expr = None
    uri: str = None
    choices: List[Any] = None
    hidden: bool = False
    inlist: bool
    external: Attribute = None

    schema = {
        'model': {'parent': True},

        'title': {'type': 'string'},
        'description': {'type': 'string'},

        # URI of this model in a known vocabulary
        'uri': {'type': 'uri', 'default': None},

        # Maturity level
        'level': {'type': 'string', 'inherit': True, 'choices': Level},

        # Validation
        'required': {'type': 'bool', 'default': False},
        'prepare': {'type': 'spyna', 'default': None},
        'default': {},
        'choices': {'type': 'array'},

        # Integrity
        'unique': {'type': 'bool', 'default': False},
        'nullable': {'type': 'bool', 'default': False},

        # Access
        'hidden': {'type': 'bool', 'default': False},
        'access': {'type': 'string', 'inherit': True, 'choices': Access},

        # Datasets
        'external': {
            'type': 'comp',
            'group': 'datasets',
            'cname': 'attribute',
        },
    }

    def __repr__(self):
        module = type(self).__module__
        klass = type(self).__name__
        prop = self.name
        dtype = self.dtype.name
        return f'<{module}.{klass}(name={prop!r}, type={dtype!r})>'
