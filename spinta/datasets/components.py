from __future__ import annotations

from typing import Dict, Any
from typing import List
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from spinta.backends.components import Backend
from spinta.components import EntryId, ExtraMetaData
from spinta.components import Namespace
from spinta.dimensions.comments.components import Comment
from spinta.dimensions.lang.components import LangData
from spinta.components import MetaData
from spinta.components import Model
from spinta.components import Property
from spinta.core.enums import Access, Level
from spinta.core.ufuncs import Expr
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.utils.schema import NA


class DatasetGiven:
    access: str = None
    name: str = None


class Dataset(MetaData):
    """DCAT compatible metadata about an external dataset.

    DCAT (Data Catalog Vocabulary) - https://w3c.github.io/dxwg/dcat/
    """

    manifest: Manifest
    level: Level = 3
    access: Access = Access.private
    website: str = None
    resources: Dict[str, Resource] = None
    source: Optional[str] = None  # metadata source
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
        'source': {'type': 'string'},
        'given_name': {'type': 'string', 'default': None},
    }

    def __init__(self):
        self.given = DatasetGiven()
        self.prefixes = {}


class ExternalBackend(Backend):
    engine: Engine = None
    schema: sa.MetaData = None


class External(ExtraMetaData):
    pass


class ResourceGiven:
    access: Access = None
    name: str = None


class Resource(External):
    eid: EntryId = None
    title: str
    description: str
    dataset: Dataset
    backend: ExternalBackend = None
    level: Level
    access: Access
    external: str
    prepare: Expr
    models: Dict[str, Model]
    given: ResourceGiven
    lang: LangData = None
    comments: List[Comment] = None
    params: List[Param]
    source_params: set

    schema = {
        'type': {'type': 'string'},
        'dataset': {'parent': True},
        'prepare': {'type': 'spyna', 'default': NA},

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
        'params': {'type': 'object'},
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
        'comments': {},
        'lang': {'type': 'object'},
        'given_name': {'type': 'string', 'default': None},
    }

    def __init__(self):
        self.given = ResourceGiven()
        self.params = {}
        self.source_params = set()


class Param(ExtraMetaData):
    name: str
    title: str
    description: str
    # Formatted values
    sources: List[Any]
    formulas: List[Expr]
    dependencies: set

    # Given values
    source: List[Any]
    prepare: List[Any]

    schema = {
        'type': {'type': 'string'},
        'title': {'type': 'string'},
        'description': {'type': 'string'},
        'name': {'type': 'string'},
        'source': {'type': 'list'},
        'prepare': {'type': 'list'}
    }

    def __init__(self):
        self.dependencies = set()


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
        'prepare': {'type': 'spyna', 'default': NA},
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
    prepare: Expr = NA    # property.prepare

    schema = {
        'prop': {'parent': True},
        'name': {'default': None},
        'prepare': {'type': 'spyna', 'default': NA},
    }
