import pytest

import sqlalchemy as sa

from spinta.backends.postgresql.files import DatabaseFile


def _db():
    engine = sa.create_engine("sqlite://")
    metadata = sa.MetaData(engine)
    table = sa.Table(
        "blocks",
        metadata,
        sa.Column("_id", sa.String, primary_key=True),
        sa.Column("_block", sa.LargeBinary),
    )
    metadata.create_all()
    return engine, table


def test_files():
    engine, table = _db()
    with engine.connect() as conn:
        with DatabaseFile(conn, table, mode="w", bsize=8) as f:
            f.write(b"foo bar baz")
            f.seek(0)
            assert f.read() == b"foo bar baz"
            assert f.size == 11

        with DatabaseFile(conn, table, f.size, f.blocks, f.bsize, mode="r") as f:
            assert f.read() == b"foo bar baz"
            assert f.size == 11

        with DatabaseFile(conn, table, f.size, f.blocks, f.bsize, mode="w") as f:
            f.write(b"xyz")
            assert f.read() == b" bar baz"
            assert f.size == 11

        with DatabaseFile(conn, table, f.size, f.blocks, f.bsize, mode="r") as f:
            assert f.read() == b"xyz bar baz"
            assert f.size == 11

        with DatabaseFile(conn, table, f.size, f.blocks, f.bsize, mode="a") as f:
            f.write(b" test")
            assert f.read() == b""
            assert f.size == 16

        with DatabaseFile(conn, table, f.size, f.blocks, f.bsize, mode="r") as f:
            assert f.read() == b"xyz bar baz test"
            assert f.size == 16


@pytest.mark.parametrize("data", [b"abcdefghi"[:i] for i in range(10)])
def test_write_mode(postgresql, data):
    engine, table = _db()

    with engine.connect() as conn:
        with DatabaseFile(conn, table, mode="w", bsize=3) as f:
            assert f.write(data) == len(data)
            assert f.size == len(data)

        with DatabaseFile(conn, table, f.size, f.blocks, f.bsize, mode="r") as f:
            assert f.read() == data
            assert f.size == len(data)


@pytest.mark.parametrize(
    "pos,size,data",
    [
        (0, 2, b"ab"),
        (1, 2, b"bc"),
        (2, 2, b"cd"),
        (2, 2, b"cd"),
        (5, 2, b"fg"),
        (7, 2, b"hi"),
        (8, 2, b"i"),
        (9, 2, b""),
    ],
)
def test_read_mode(postgresql, pos, size, data):
    engine, table = _db()

    with engine.connect() as conn:
        with DatabaseFile(conn, table, mode="w", bsize=3) as f:
            f.write(b"abcdefghi")

        with DatabaseFile(conn, table, f.size, f.blocks, f.bsize, mode="r") as f:
            assert f.seek(pos) == pos
            assert f.read(size) == data
