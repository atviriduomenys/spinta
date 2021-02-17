from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Final
from typing import List
from typing import Literal
from typing import Optional
from typing import TypedDict

from spinta.manifests.components import Manifest


class TabularManifest(Manifest):
    pass


ID: Final = 'id'
DATASET: Final = 'dataset'
RESOURCE: Final = 'resource'
BASE: Final = 'base'
MODEL: Final = 'model'
PROPERTY: Final = 'property'
TYPE: Final = 'type'
REF: Final = 'ref'
SOURCE: Final = 'source'
PREPARE: Final = 'prepare'
LEVEL: Final = 'level'
ACCESS: Final = 'access'
URI: Final = 'uri'
TITLE: Final = 'title'
DESCRIPTION: Final = 'description'
ManifestColumn = Literal[
    'id',
    'dataset',
    'resource',
    'base',
    'model',
    'property',
    'type',
    'ref',
    'source',
    'prepare',
    'level',
    'access',
    'uri',
    'title',
    'description',
]
MANIFEST_COLUMNS: List[ManifestColumn] = [
    ID,
    DATASET,
    RESOURCE,
    BASE,
    MODEL,
    PROPERTY,
    TYPE,
    REF,
    SOURCE,
    PREPARE,
    LEVEL,
    ACCESS,
    URI,
    TITLE,
    DESCRIPTION,
]

ManifestRow = Dict[ManifestColumn, str]


class ManifestTableRow(TypedDict, total=False):
    type: str
    backends: Dict[str, BackendRow]


class DatasetRow(TypedDict, total=False):
    type: str
    id: str
    name: str
    level: str
    access: str
    title: str
    description: str
    resources: Dict[str, ResourceRow]


class ResourceRow(ManifestRow):
    backend: str
    external: str


class BackendRow(TypedDict, total=False):
    type: str
    name: str
    dsn: str
    title: str
    description: str


class BaseRow(TypedDict, total=False):
    model: str
    pk: str


class ModelRow(TypedDict, total=False):
    type: str
    id: str
    name: str
    base: Optional[str]
    level: str
    access: str
    title: str
    description: str
    properties: Dict[str, PropertyRow]
    external: ModelExternalRow
    backend: str


class ModelExternalRow(TypedDict, total=False):
    dataset: str
    resource: str
    pk: List[str]
    name: str
    prepare: Dict[str, Any]


class EnumRow(TypedDict, total=False):
    name: str
    source: str
    prepare: Optional[Dict[str, Any]]
    access: str
    title: str
    description: str


class PropertyRow(TypedDict, total=False):
    type: str
    prepare: Optional[Dict[str, Any]]
    level: str
    access: str
    uri: str
    title: str
    description: str
    model: str
    refprops: List[str]
    external: PropertyExternalRow
    enums: Dict[str, Dict[str, EnumRow]]


class PropertyExternalRow(TypedDict, total=False):
    name: str
    prepare: Optional[Dict[str, Any]]


class PrefixRow(TypedDict, total=False):
    id: str
    eid: str
    type: str
    name: str
    uri: str
    title: str
    description: str
