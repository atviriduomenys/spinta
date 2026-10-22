from __future__ import annotations

from enum import Enum
from typing import IO, Any, Final, Literal, TypedDict

from spinta.components import PrepareGiven
from spinta.dimensions.lang.components import LangData
from spinta.manifests.components import Manifest


class TabularFormat(Enum):
    CSV = "csv"
    ASCII = "ascii"
    XLSX = "xlsx"
    GSHEETS = "gsheets"


class TabularManifest(Manifest):
    format: TabularFormat = None
    path: str = None


class CsvManifest(TabularManifest):
    type = "csv"
    format: TabularFormat = TabularFormat.CSV
    file: IO[str] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".csv")


class AsciiManifest(TabularManifest):
    type = "ascii"
    format: TabularFormat = TabularFormat.ASCII
    file: IO[str] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".txt")


class XlsxManifest(TabularManifest):
    type = "xlsx"
    format: TabularFormat = TabularFormat.XLSX

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".xlsx")


class GsheetsManifest(TabularManifest):
    type = "gsheets"
    format: TabularFormat = TabularFormat.GSHEETS

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.startswith("https://docs.google.com/spreadsheets/")


ID: Final = "id"
DATASET: Final = "dataset"
RESOURCE: Final = "resource"
BASE: Final = "base"
MODEL: Final = "model"
PROPERTY: Final = "property"
TYPE: Final = "type"
REF: Final = "ref"
SOURCE: Final = "source"
SOURCE_TYPE: Final = "source.type"
PREPARE: Final = "prepare"
LEVEL: Final = "level"
ACCESS: Final = "access"
URI: Final = "uri"
TITLE: Final = "title"
DESCRIPTION: Final = "description"
STATUS: Final = "status"
VISIBILITY: Final = "visibility"
ELI: Final = "eli"
COUNT: Final = "count"
ORIGIN: Final = "origin"

ManifestColumn = Literal[
    "id",
    "dataset",
    "resource",
    "base",
    "model",
    "property",
    "type",
    "ref",
    "source",
    "source.type",
    "prepare",
    "origin",
    "count",
    "level",
    "status",
    "visibility",
    "access",
    "uri",
    "eli",
    "title",
    "description",
]
MANIFEST_COLUMNS: list[ManifestColumn] = [
    ID,
    DATASET,
    RESOURCE,
    BASE,
    MODEL,
    PROPERTY,
    TYPE,
    REF,
    SOURCE,
    SOURCE_TYPE,
    PREPARE,
    ORIGIN,
    COUNT,
    LEVEL,
    STATUS,
    VISIBILITY,
    ACCESS,
    URI,
    ELI,
    TITLE,
    DESCRIPTION,
]

ManifestRow = dict[ManifestColumn, str]


class ManifestTableRow(TypedDict, total=False):
    type: str
    backends: dict[str, BackendRow]


class DatasetRow(TypedDict, total=False):
    type: str
    id: str
    name: str
    level: str
    access: str
    title: str
    description: str
    resources: dict[str, ResourceRow]
    lang: LangData
    source: str
    given_name: str


class ResourceRow(ManifestRow):
    id: str
    backend: str
    external: str
    lang: LangData
    given_name: str
    count: int
    source_type: str


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
    pk: list[str]
    lang: LangData
    level: str


class ParamRow(TypedDict):
    id: str
    name: str  # param name
    source: list[str]  # list of `self` for prepare formulas
    prepare: list[Any]  # list of formulas
    title: str
    description: str


class ModelExtraData(TypedDict):
    params: list[ParamRow]


class ModelRow(TypedDict, total=False):
    type: str
    id: str
    name: str
    base: dict | None
    level: str
    access: str
    title: str
    description: str
    properties: dict[str, PropertyRow]
    scopes: dict[str, ScopeRow]
    external: ModelExternalRow
    backend: str
    lang: LangData
    data: ModelExtraData
    given_name: str
    status: str
    visibility: str
    eli: str
    count: int
    origin: str


class ModelExternalRow(TypedDict, total=False):
    dataset: str
    resource: str
    pk: list[str]
    name: str
    prepare: dict[str, Any]
    type: str


class EnumRow(TypedDict, total=False):
    id: str
    name: str
    source: str
    prepare: dict[str, Any] | None
    access: str
    title: str
    description: str
    lang: LangData
    level: str
    status: str
    visibility: str
    eli: str
    count: int


class PropertyRow(TypedDict, total=False):
    id: str
    type: str
    type_args: list[str]
    prepare: dict[str, Any] | None
    level: str
    access: str
    uri: str
    title: str
    description: str
    model: str
    refprops: list[str]
    external: PropertyExternalRow
    enum: str
    enums: dict[str, dict[str, EnumRow]]
    lang: LangData
    units: str
    required: bool
    unique: bool
    given_name: str
    prepare_given: list[PrepareGiven]
    explicitly_given: bool
    status: str
    visibility: str
    eli: str
    count: int
    origin: str


class PropertyExternalRow(TypedDict, total=False):
    name: str
    prepare: dict[str, Any] | None
    type: str


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
    prepare: str
    level: str
    uri: str


class CommentData(TypedDict, total=False):
    comments: list[CommentRow] | None


class ScopeRow(TypedDict, total=False):
    id: str
    name: str
    prepare: dict[str, Any]  # formula
    access: str
    level: str
    status: str
    visibility: str
    count: int
    eli: str
    uri: str
    title: str
    description: str
