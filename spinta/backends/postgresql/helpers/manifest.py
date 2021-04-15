from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa

from spinta.components import Context

if TYPE_CHECKING:
    from spinta.backends.postgresql.components import PostgreSQL


def read_manifest_schemas(context: Context, backend: PostgreSQL, conn):
    meta = sa.MetaData(backend.engine)
    table = sa.Table('_schema', meta, autoload_with=backend.engine)
    query = sa.select([table.c._id, table.c.schema])
    i = 0
    for i, row in enumerate(conn.execute(query), 1):
        yield (
            row[table.c._id],
            row[table.c.schema],
        )
    if i == 0:
        raise Exception(
            "Empty _schema table, nothing is loaded into manifest."
        )
