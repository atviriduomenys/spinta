from decimal import Decimal

import pytest
import sqlalchemy as sa

from spinta import commands
from spinta.auth import AdminToken
from spinta.core.config import RawConfig
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.sql.ufuncs.result.components import SqlResultBuilder
from spinta.exceptions import UnableToCast
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.utils import create_empty_backend
from spinta.ufuncs.resultbuilder.helpers import get_row_value


@pytest.mark.parametrize(
    "value",
    [
        1.0,
        Decimal(1.0),
    ],
)
def test_cast_integer(rc: RawConfig, value):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | prepare | level | title
    example                  |         |         |       |
      |   |   | Data         |         |         |       |
      |   |   |   | value    | integer |         |       |
    """,
    )
    dtype = commands.get_model(context, manifest, "example/Data").properties["value"].dtype
    env = SqlResultBuilder(context)
    env.call("cast", dtype, value)


@pytest.mark.parametrize(
    "value",
    [
        1.1,
        Decimal(1.1),
    ],
)
def test_cast_integer_error(rc: RawConfig, value):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | prepare | level | title
    example                  |         |         |       |
      |   |   | Data         |         |         |       |
      |   |   |   | value    | integer |         |       |
    """,
    )
    dtype = commands.get_model(context, manifest, "example/Data").properties["value"].dtype
    env = SqlResultBuilder(context)
    with pytest.raises(UnableToCast) as e:
        env.call("cast", dtype, value)
    assert e.value.message == "Unable to cast 1.1 to integer type."


def test_point(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type     | ref | source | prepare     | level
    example                  |          |     |        |             |
      |   |   | Data         |          | id  | data   |             | 4
      |   |   |   | id       | integer  |     | id     |             | 4
      |   |   |   | x        | number   |     | x      |             | 3
      |   |   |   | y        | number   |     | y      |             | 3
      |   |   |   | point    | geometry |     |        | point(x, y) | 3
    """,
    )

    context.set("auth.token", AdminToken())

    model_name = "example/Data"
    model = commands.get_model(context, manifest, model_name)

    backend = create_empty_backend(context, "sql")
    backend.schema = sa.MetaData()
    backend.tables = {}

    env = backend.query_builder_class(context)
    env.update(model=model)

    table = backend.tables[model_name] = sa.Table(
        "data",
        backend.schema,
        sa.Column("id", sa.Integer),
        sa.Column("x", sa.Float),
        sa.Column("y", sa.Float),
    )

    env = env.init(backend, table)
    expr = env.resolve(Expr("select"))
    expr = env.execute(expr)
    env.build(expr)

    row = [
        1,  # id
        4,  # x
        2,  # y
    ]
    sel = env.selected["point"]
    val = get_row_value(context, backend, row, sel)

    env = SqlResultBuilder(context).init(val, sel.prop, row)
    val = env.resolve(sel.prep)
    assert val == "POINT (4 2)"
