from types import AsyncGeneratorType

import psycopg2
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Model, Context, Property
from spinta.exceptions import RedirectFeatureMissing


@commands.create_redirect_entry.register(Context, (Model, Property), PostgreSQL)
async def create_redirect_entry(
    context: Context,
    node: (Model, Property),
    backend: PostgreSQL,
    *,
    dstream: AsyncGeneratorType,
) -> None:
    transaction = context.get("transaction")
    connection = transaction.connection
    table = backend.get_table(node, TableType.REDIRECT)
    async for data in dstream:
        if not data.patch:
            yield data
            continue
        qry = insert(table).values(_id=data.saved.get("_id"), redirect=data.patch.get("_id"))
        qry = qry.on_conflict_do_update(index_elements=[table.c._id], set_=dict(redirect=data.patch.get("_id")))
        connection.execute(qry)
        yield data


@commands.redirect.register(Context, PostgreSQL, Model, str)
def redirect(context: Context, backend: PostgreSQL, model: Model, id_: str):
    try:
        with backend.begin() as conn:
            table_identifier = get_table_identifier(model, TableType.REDIRECT)
            result = conn.execute(
                f"SELECT redirect FROM {table_identifier.pg_escaped_qualified_name} WHERE _id = '{id_}' LIMIT 1"
            ).scalar()
            return result
    except sa.exc.ProgrammingError as e:
        if isinstance(e.orig, psycopg2.errors.UndefinedTable):
            raise RedirectFeatureMissing(model)
        raise e
