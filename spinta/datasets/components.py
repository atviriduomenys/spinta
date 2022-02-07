from __future__ import annotations

from typing import Dict
from typing import List
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from spinta.backends.components import Backend
from spinta.components import EntryId
from spinta.components import Namespace
from spinta.dimensions.lang.components import LangData
from spinta.components import MetaData
from spinta.components import Model
from spinta.components import Node
from spinta.components import Property
from spinta.core.enums import Access
from spinta.core.ufuncs import Expr
from spinta.datasets.enums import Level
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.types.owner import Owner
from spinta.types.project import Project


class DatasetGiven:
    access: str = None


class Dataset(MetaData):
    """DCAT compatible metadata about an external dataset.

    DCAT (Data Catalog Vocabulary) - https://w3c.github.io/dxwg/dcat/
    """

    id: str
    manifest: Manifest
    owner: Owner = None
    level: Level = 3
    access: Access = Access.private
    website: str = None
    projects: List[Project] = None
    resources: Dict[str, Resource] = None
    title: str
    description: str
    given: DatasetGiven
    lang: LangData = None
    ns: Namespace = None
    prefixes: Dict[str, UriPrefix]

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

    def __init__(self):
        self.given = DatasetGiven()
        self.prefixes = {}


class ExternalBackend(Backend):
    engine: Engine = None
    schema: sa.MetaData = None


class External(Node):
    pass


class ResourceGiven:
    access: Access = None


class Resource(External):
    eid: EntryId = None
    title: str
    description: str
    dataset: Dataset
    backend: ExternalBackend = None
    level: Level
    access: Access
    external: str
    prepare: str
    models: Dict[str, Model]
    given: ResourceGiven
    lang: LangData = None

    schema = {
        'type': {'type': 'string'},
        'dataset': {'parent': True},
        'prepare': {'type': 'spyna'},

        # Backend name specified in `ref` column, points to previously defined
        # backend or to a configured stored backend.
        'backend': {
            'type': 'ref',
            'ref': 'context.store.backends',
        },

        # If `backend` is not given via `ref` column, then in `source` column,
        # explicit backend dsn can be given. Only one `ref` or `source` can be
        # given.
        'external': {
            'type': 'string'
        },

        'level': {
            'type': 'integer',
            'choices': Level,
            'inherit': 'dataset.level',
        },
        'access': {
            'type': 'string',
            'choices': Access,
            'inherit': 'dataset.access',
        },
        'title': {'type': 'string'},
        'description': {'type': 'string'},
    }

    def __init__(self):
        self.given = ResourceGiven()


class Param:
    name: str
    sources: List[str]
    formulas: List[Expr]


class Entity(External):
    # XXX: Here `dataset` and `resource` first are resolved as `str` and then
    #      linked with `Dataset` and `Resource` instances. That is not a good
    #      thing and should be refactored into a `RawEntity` with `str` types
    #      which then is converted into `Entity` with `Dataset` and `Resource`
    #      types.
    dataset: Dataset                # dataset
    resource: Optional[Resource]    # resource
    model: Model                    # model
    pkeys: List[Property]           # model.ref
    # This is set to True if primary key is not given. Anf if primary key is not
    # given, then `pkeys` will be set to all properties.
    unknown_primary_key: bool = False
    name: str                       # model.source
    prepare: Expr                   # model.prepare
    params: List[Param]

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
