from typing import TypedDict


class FileData(TypedDict, total=False):
    _id: str  # File name
    _content_type: str  # Media type (or MIME-type)
    _content: str  # base64 encoded string of file content
