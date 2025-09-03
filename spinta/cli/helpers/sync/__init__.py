from enum import Enum

CONTENT_TYPE_TEXT_CSV = "text/csv"
IDENTIFIER = "_id"


class ContentType(Enum):
    BYTES = "bytes"
    CSV = "csv"
