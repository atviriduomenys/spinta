from typing import TypedDict
from typing import Optional
from typing import List


class ObjectData(TypedDict):
    _type: str
    _id: str
    _revision: str


class FileObjectData(ObjectData):
    _content: Optional[bytes]           # File content
    _content_type: Optional[str]        # File mime content type
    _size: Optional[int]                # File size in bytes

    # Following properties are internal metadata, should not be exposed.
    _bsize: Optional[int]               # Single block size in bytes
    _blocks: Optional[List[str]]        # List of block ids
