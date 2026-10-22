from typing import TypedDict


class ObjectData(TypedDict):
    _type: str
    _id: str
    _revision: str


class FileObjectData(ObjectData):
    _content: bytes | None  # File content
    _content_type: str | None  # File mime content type
    _size: int | None  # File size in bytes

    # Following properties are internal metadata, should not be exposed.
    _bsize: int | None  # Single block size in bytes
    _blocks: list[str] | None  # List of block ids
