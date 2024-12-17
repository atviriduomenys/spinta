from __future__ import annotations

from enum import Enum
from typing import Any
from typing import Dict
from typing import Final
from typing import IO
from typing import List
from typing import Literal
from typing import Optional
from typing import TypedDict

from spinta.components import PrepareGiven
from spinta.dimensions.lang.components import LangData
from spinta.manifests.components import Manifest


class TabularFormat(Enum):
    CSV = 'csv'
    ASCII = 'ascii'
    XLSX = 'xlsx'
    GSHEETS = 'gsheets'


class TabularManifest(Manifest):
    format: TabularFormat = None
    path: str = None


class CsvManifest(TabularManifest):
    type = 'csv'
    format: TabularFormat = TabularFormat.CSV
    file: IO[str] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.csv')


class AsciiManifest(TabularManifest):
    type = 'ascii'
    format: TabularFormat = TabularFormat.ASCII
    file: IO[str] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.txt')


class XlsxManifest(TabularManifest):
    type = 'xlsx'
    format: TabularFormat = TabularFormat.XLSX

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.xlsx')


class GsheetsManifest(TabularManifest):
    type = 'gsheets'
    format: TabularFormat = TabularFormat.GSHEETS

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.startswith('https://docs.google.com/spreadsheets/')


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
    lang: LangData
    source: str
    given_name: str


class ResourceRow(ManifestRow):
    id: str
    backend: str
    external: str
    lang: LangData
    given_name: str


class BackendRow(TypedDict, total=False):
    id: str
    type: str
    name: str
    dsn: str
    title: str
    description: str


class BaseRow(TypedDict, total=False):
    id: str
    name: str
    model: str
    pk: List[str]
    lang: LangData
    level: str


class ParamRow(TypedDict):
    id: str
    name: str                   # param name
    source: List[str]           # list of `self` for prepare formulas
    prepare: List[Any]          # list of formulas
    title: str
    description: str


class ModelExtraData(TypedDict):
    params: List[ParamRow]


class ModelRow(TypedDict, total=False):
    type: str
    id: str
    name: str
    base: Optional[dict]
    level: str
    access: str
    title: str
    description: str
    properties: Dict[str, PropertyRow]
    external: ModelExternalRow
    backend: str
    lang: LangData
    data: ModelExtraData
    given_name: str


class ModelExternalRow(TypedDict, total=False):
    dataset: str
    resource: str
    pk: List[str]
    name: str
    prepare: Dict[str, Any]


class EnumRow(TypedDict, total=False):
    id: str
    name: str
    source: str
    prepare: Optional[Dict[str, Any]]
    access: str
    title: str
    description: str
    lang: LangData
    level: str


class PropertyRow(TypedDict, total=False):
    id: str
    type: str
    type_args: List[str]
    prepare: Optional[Dict[str, Any]]
    level: str
    access: str
    uri: str
    title: str
    description: str
    model: str
    refprops: List[str]
    external: PropertyExternalRow
    enum: str
    enums: Dict[str, Dict[str, EnumRow]]
    lang: LangData
    units: str
    required: bool
    unique: bool
    given_name: str
    prepare_given: List[PrepareGiven]
    explicitly_given: bool


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


class CommentRow(TypedDict, total=False):
    id: str
    parent: str
    author: str
    access: str
    # TODO: should be datetime
    created: str
    comment: str


class CommentData(TypedDict, total=False):
    comments: Optional[List[CommentRow]]
