import enum
from collections.abc import Iterator
from typing import Any, NamedTuple, TypedDict

import sqlalchemy as sa

from spinta.components import Model

PUSH_SESSION_ID = "push.session_id"


class PushOperation(enum.Enum):
    INSERT = "insert"
    PATCH = "patch"
    DELETE = "delete"

    # Used for rows that do not need any changes (mainly used to know which rows need `session_id` updates
    UNCHANGED = "unchanged"


class State(NamedTuple):
    engine: sa.engine.Engine
    metadata: sa.MetaData


class PushRow:
    model: Model | None
    data: dict[str, Any]
    # SHA1 checksum of data generated with _get_data_rev.
    checksum: str | None
    # True if data has already been sent.
    saved: bool = False
    # If push request received an error
    error: bool = False
    # Row operation
    op: PushOperation | None = None
    # If True, need to push data immediately
    push: bool = False
    # If True, need to include in request
    send: bool = True

    def __init__(
        self,
        model: Model | None,
        data: dict[str, Any],
        checksum: str | None = None,
        saved: bool = False,
        error: bool = False,
        op: PushOperation | None = None,
        push: bool = False,
        send: bool = True,
    ):
        self.model = model
        self.data = data
        self.checksum = checksum
        self.saved = saved
        self.error = error
        self.op = op
        self.push = push
        self.send = send


PushRows = Iterator[PushRow]


class ErrorContext(TypedDict):
    # Manifest name
    manifest: str

    # Dataset name
    dataset: str

    # Absolute model name (but does not start with /)
    model: str

    # Property name
    property: str

    # Property data type
    type: str

    # Model name in data source
    entity: str

    # Property name in data source
    attribute: str

    # Full class path of component involved in the error.
    component: str

    # Line number or other location (depends on manifest) node in manifest
    # involved in the error.
    schema: str

    # External object id
    id: str


class Error(TypedDict):
    # Error type
    type: str

    # Error code (exception class name)
    code: str

    # Error context.
    context: ErrorContext

    # Message template.
    tempalte: str

    # Message compiled from templates and context.
    message: str


class Saved(NamedTuple):
    revision: str
    checksum: str


PUSH_NOW = PushRow(
    model=None,
    data={"_type": None},
    push=True,
    send=False,
)
