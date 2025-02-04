from enum import Enum

DATASET = [
    'id',
    'dataset',
    'resource',
    'base',
    'model',
    'property',
    'type',
    'ref',
    'source',
    'source.type',
    'prepare',
    'level',
    'access',
    'uri',
    'title',
    'description',
    'status',
    'visibility',
    'eli',
    'count',
    'origin'
]


class DataTypeEnum(Enum):
    STRING = 'string'
    TEXT = 'text'
    REF = 'ref'
    BACKREF = 'backref'
    ARRAY = 'array'
    GENERIC = 'generic'
    _OBJECT = 'object'                # Internal type
    _PARTIAL = 'partial'              # Internal type
    _ARRAY_BACKREF = 'array_backref'  # Internal type
    _PARTIAL_ARRAY = 'partial_array'  # Internal type
