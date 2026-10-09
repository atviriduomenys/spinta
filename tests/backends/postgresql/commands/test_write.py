from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.commands import write
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context, DataItem, Model
from spinta.core.enums import Action


async def _stream(*items):
    for item in items:
        yield item


def _make_context(connection):
    context = Context("test")
    context.set("transaction", SimpleNamespace(connection=connection, id="txn"))
    context.set("config", SimpleNamespace(max_error_count_on_insert=10))
    return context


def _make_backend(model, table, redirect_table):
    backend = PostgreSQL()
    backend.schema = sa.MetaData()
    backend.tables = {
        model.basename: table,
        f"{model.basename}{TableType.REDIRECT.value}": redirect_table,
    }
    return backend


def _make_tables():
    metadata = sa.MetaData()
    table = sa.Table(
        "test_model",
        metadata,
        sa.Column("_id", sa.String, primary_key=True),
        sa.Column("_revision", sa.String),
        sa.Column("_txn", sa.String),
        sa.Column("_created", sa.DateTime),
    )
    redirect_table = sa.Table(
        "test_model_redirect",
        metadata,
        sa.Column("_id", sa.String, primary_key=True),
        sa.Column("redirect", sa.String),
    )
    return table, redirect_table


def _make_data():
    model = Model()
    model.name = "test/model"
    model.ns = None
    model.properties = {}
    data = DataItem(model=model, action=Action.INSERT)
    data.patch = {
        "_id": "id",
        "_revision": "rev",
    }
    return model, data


@pytest.mark.asyncio
async def test_postgresql_insert_ignores_duplicate_ids(monkeypatch):
    table, redirect_table = _make_tables()
    model, data = _make_data()
    connection = Mock()
    connection.begin_nested.return_value = Mock()
    connection.execute.return_value = SimpleNamespace(rowcount=0)

    monkeypatch.setattr(write, "utcnow", lambda: "now")
    after_write = Mock()
    monkeypatch.setattr(commands, "after_write", after_write)

    context = _make_context(connection)
    backend = _make_backend(model, table, redirect_table)

    result = [
        item
        async for item in commands.insert(
            context,
            model,
            backend,
            dstream=_stream(data),
        )
    ]

    assert result == [data]
    assert data.error is None
    query, params = connection.execute.call_args.args
    assert params == {
        "_id": "id",
        "_revision": "rev",
        "_txn": "txn",
        "_created": "now",
    }
    assert "ON CONFLICT (_id) DO NOTHING" in str(query.compile(dialect=postgresql.dialect()))
    after_write.assert_not_called()
    assert connection.execute.call_count == 1
    connection.begin_nested.return_value.commit.assert_called()


@pytest.mark.asyncio
async def test_postgresql_insert_runs_after_write_for_inserted_rows(monkeypatch):
    table, redirect_table = _make_tables()
    model, data = _make_data()
    connection = Mock()
    connection.begin_nested.return_value = Mock()
    connection.execute.return_value = SimpleNamespace(rowcount=1)

    monkeypatch.setattr(write, "utcnow", lambda: "now")
    after_write = Mock()
    monkeypatch.setattr(commands, "after_write", after_write)

    context = _make_context(connection)
    backend = _make_backend(model, table, redirect_table)

    result = [
        item
        async for item in commands.insert(
            context,
            model,
            backend,
            dstream=_stream(data),
        )
    ]

    assert result == [data]
    after_write.assert_called_once_with(context, model, backend, data=data)
    assert connection.execute.call_count == 2
    delete_query = connection.execute.call_args_list[1].args[0]
    compiled_delete = str(delete_query.compile(dialect=postgresql.dialect()))
    assert "DELETE FROM test_model_redirect" in compiled_delete
    assert "WHERE test_model_redirect._id" in compiled_delete
    connection.begin_nested.return_value.commit.assert_called()
