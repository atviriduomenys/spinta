from enum import Enum

DATASET = [
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


class DataTypeEnum(Enum):
    STRING = "string"
    TEXT = "text"
    REF = "ref"
    BACKREF = "backref"
    ARRAY = "array"
    GENERIC = "generic"
    INHERIT = "inherit"
    _OBJECT = "object"  # Internal type
    _PARTIAL = "partial"  # Internal type
    _ARRAY_BACKREF = "array_backref"  # Internal type
    _PARTIAL_ARRAY = "partial_array"  # Internal type
