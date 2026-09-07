from typing import Any, Callable, Dict, NamedTuple, Optional, TypedDict

import sqlalchemy as sa

from spinta.components import Model, pagination_enabled
from spinta.utils.sqlite import SqliteMigratableDb

PUSH_STATE_DB = "push.state"
PAGE_TYPE_MAPPING = {
    "string": sa.Text,
    "date": sa.Date,
    "datetime": sa.DateTime,
    "time": sa.Time,
    "integer": sa.Integer,
    "number": sa.Numeric,
}


class PushState(SqliteMigratableDb):
    engine: sa.engine.Engine
    metadata: sa.MetaData

    pagination_table_name: str = "_page"

    def __init__(self, dsn: str):
        super().__init__(dsn)

        self.metatable_templates[self.pagination_table_name] = lambda metadata: sa.Table(
            "_page",
            metadata,
            sa.Column("model", sa.Text, primary_key=True),
            sa.Column("property", sa.Text),
            sa.Column("value", sa.Text),
        )

    def _default_table_template(
        self, name: str, model: Model | None = None, **kwargs
    ) -> Callable[[sa.MetaData], sa.Table]:
        if model is None:
            raise Exception("DEFAULT TABLE TEMPLATE FOR STATE DB REQUIRES model property to be given")

        pagination_cols = []
        if pagination_enabled(model):
            for prop in model.page.keys.values():
                _type = PAGE_TYPE_MAPPING.get(prop.dtype.name, sa.Text)
                pagination_cols.append(sa.Column(f"page.{prop.name}", _type, index=True))

        return lambda metadata: sa.Table(
            name,
            metadata,
            sa.Column("id", sa.Unicode, primary_key=True),
            sa.Column("checksum", sa.Unicode),
            sa.Column("revision", sa.Unicode),
            sa.Column("pushed", sa.DateTime),
            sa.Column("error", sa.Boolean),
            sa.Column("data", sa.Text),
            sa.Column("session_id", sa.Text, index=True),
            *pagination_cols,
        )


class PushRow:
    model: Optional[Model]
    data: Dict[str, Any]
    # SHA1 checksum of data generated with _get_data_rev.
    checksum: Optional[str]
    # True if data has already been sent.
    saved: bool = False
    # If push request received an error
    error: bool = False
    # Row operation
    op: Optional[str] = None
    # If True, need to push data immediately
    push: bool = False
    # If True, need to include in request
    send: bool = True

    def __init__(
        self,
        model: Optional[Model],
        data: Dict[str, Any],
        checksum: str = None,
        saved: bool = False,
        error: bool = False,
        op: str = None,
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
