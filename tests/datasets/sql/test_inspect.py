import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.sql.helpers import _get_column_type
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.datasets import Sqlite


def test_char_type():
    column = {"name": "test", "type": mysql.CHAR()}
    table = "test"

    assert _get_column_type(column, table) == "string"


def test_tinyblob_type():
    column = {"name": "test", "type": mysql.TINYBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_blob_type():
    column = {"name": "test", "type": mysql.BLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_mediumblob_type():
    column = {"name": "test", "type": mysql.MEDIUMBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_longblob_type():
    column = {"name": "test", "type": mysql.LONGBLOB()}
    table = "test"

    assert _get_column_type(column, table) == "binary"


def test_inspect_blob_types(
    context: Context,
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path,
    sqlite: Sqlite,
):
    """
    Test binary/BLOB type inspection via CLI (issue #1484).

    This integration test verifies the full inspect workflow generates
    correct manifests with binary types. Unit tests above verify that
    MySQL-specific BLOB types (TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB)
    are correctly mapped.
    """
    # Setup database with binary columns and real binary data
    sqlite.init(
        {
            "PROVIDERS": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("LOGO", sa.LargeBinary),
                sa.Column("ICON", sa.LargeBinary),
                sa.Column("DOCUMENT", sa.LargeBinary),
            ],
        }
    )

    sqlite.write(
        "PROVIDERS",
        [
            {
                "ID": 1,
                "NAME": "Test Provider",
                "LOGO": b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR",
                "ICON": b"\xff\xd8\xff",
                "DOCUMENT": b"%PDF-1.4",
            },
        ],
    )

    # Run inspect via CLI
    output_file = tmp_path / "manifest.csv"
    result = cli.invoke(rc, ["inspect", "-r", "sql", sqlite.dsn, "-o", str(output_file)])

    assert result.exit_code == 0

    # Validate generated manifest contains our table with binary types
    manifest_content = output_file.read_text()

    # Verify Providers table was inspected
    assert "Providers" in manifest_content
    assert "PROVIDERS" in manifest_content

    # Verify all binary columns are correctly mapped to binary type
    assert ",logo,binary," in manifest_content
    assert ",icon,binary," in manifest_content
    assert ",document,binary," in manifest_content

    # Verify other columns are correctly mapped
    assert ",id,integer," in manifest_content
    assert ",name,string," in manifest_content
