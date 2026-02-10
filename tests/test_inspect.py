import json
import os
import pathlib
import tempfile
from pathlib import Path

import sqlalchemy as sa

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.datasets.inspect.components import PriorityKey
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.config import configure
from spinta.testing.context import create_test_context
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import compare_manifest, load_manifest_and_context
from spinta.testing.tabular import create_tabular_manifest
import sqlalchemy_utils as su


@pytest.fixture()
def sqlite_new():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Sqlite("sqlite:///" + os.path.join(tmpdir, "new.sqlite"))


@pytest.fixture()
def rc_new(rc, tmp_path: pathlib.Path):
    # Need to have a clean slate, ignoring testing context manifests
    path = f"{tmp_path}/manifest.csv"
    context = create_test_context(rc)
    create_tabular_manifest(
        context,
        path,
        striptable("""
     d | r | b | m | property   | type    | ref     | source     | prepare
    """),
    )
    return rc.fork(
        {
            "manifests": {
                "default": {
                    "type": "tabular",
                    "path": str(path),
                    "backend": "default",
                    "keymap": "default",
                    "mode": "external",
                },
            },
            "backends": {
                "default": {
                    "type": "memory",
                },
            },
        }
    )


def test_inspect(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CODE", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_ID", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )

    cli.invoke(rc_new, ["inspect", sqlite.dsn, "-o", tmp_path / "result.csv"])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    assert (
        manifest
        == """
    d | r | b | m | property   | type    | ref     | source     | prepare
    db_sqlite                  |         |         |            |
      | resource1              | sql     |         | sqlite     |
                               |         |         |            |
      |   |   | City           |         |         | CITY       |
      |   |   |   | country_id | ref     | Country | COUNTRY_ID |
      |   |   |   | name       | string  |         | NAME       |
                               |         |         |            |
      |   |   | Country        |         | id      | COUNTRY    |
      |   |   |   | code       | string  |         | CODE       |
      |   |   |   | id         | integer |         | ID         |
      |   |   |   | name       | string  |         | NAME       |
    """
    )


def test_inspect_from_manifest_table(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
        }
    )
    context = create_test_context(rc_new)
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        f"""
    d | r | m | property     | type   | ref | source | access
    db_sqlite                |        |     |        |
      | resource1            | sql    |   | {sqlite.dsn} |
    """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            tmp_path / "result.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    assert (
        manifest
        == """
    d | r | b | m | property  | type    | ref | source  | prepare
    db_sqlite                 |         |     |         |
      | resource1             | sql     |     | sqlite  |
                              |         |     |         |
      |   |   | Country       |         | id  | COUNTRY |
      |   |   |   | id        | integer |     | ID      |
      |   |   |   | name      | string  |     | NAME    |
    """
    )


def test_inspect_format(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CODE", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_ID", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            tmp_path / "manifest.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "manifest.csv")
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
    d | r | b | m | property   | type    | ref     | source     | prepare
    db_sqlite                  |         |         |            |
      | resource1              | sql     |         | sqlite     |
                               |         |         |            |
      |   |   | City           |         |         | CITY       |
      |   |   |   | country_id | ref     | Country | COUNTRY_ID |
      |   |   |   | name       | string  |         | NAME       |
                               |         |         |            |
      |   |   | Country        |         | id      | COUNTRY    |
      |   |   |   | code       | string  |         | CODE       |
      |   |   |   | id         | integer |         | ID         |
      |   |   |   | name       | string  |         | NAME       |
    """,
        context,
    )
    assert a == b


def test_inspect_cyclic_refs(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CAPITAL", sa.Integer, sa.ForeignKey("CITY.ID")),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_ID", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )

    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            tmp_path / "manifest.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "manifest.csv")
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    assert (
        manifest
        == """
    d | r | b | m | property   | type    | ref     | source     | prepare
    db_sqlite                  |         |         |            |
      | resource1              | sql     |         | sqlite     |
                               |         |         |            |
      |   |   | City           |         | id      | CITY       |
      |   |   |   | country_id | ref     | Country | COUNTRY_ID |
      |   |   |   | id         | integer |         | ID         |
      |   |   |   | name       | string  |         | NAME       |
                               |         |         |            |
      |   |   | Country        |         | id      | COUNTRY    |
      |   |   |   | capital    | ref     | City    | CAPITAL    |
      |   |   |   | id         | integer |         | ID         |
      |   |   |   | name       | string  |         | NAME       |
    """
    )


def test_inspect_self_refs(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "CATEGORY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("PARENT_ID", sa.Integer, sa.ForeignKey("CATEGORY.ID")),
            ],
        }
    )
    rc_new = rc_new.fork({"manifests": {"default": {}}})
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            tmp_path / "manifest.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "manifest.csv")
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    assert (
        manifest
        == """
    d | r | b | m | property  | type    | ref      | source    | prepare
    db_sqlite                 |         |          |           |
      | resource1             | sql     |          | sqlite    |
                              |         |          |           |
      |   |   | Category      |         | id       | CATEGORY  |
      |   |   |   | id        | integer |          | ID        |
      |   |   |   | name      | string  |          | NAME      |
      |   |   |   | parent_id | ref     | Category | PARENT_ID |
    """
    )


@pytest.mark.skip(reason="sqldump not fully implemented")
def test_inspect_oracle_sqldump_stdin(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sqldump",
            "-",
            "-o",
            tmp_path / "manifest.csv",
        ],
        input="""
    --------------------------------------------------------
    --  DDL for Table COUNTRY
    --------------------------------------------------------

    CREATE TABLE "GEO"."COUNTRY" (
      "ID" NUMBER(19,0),
      "NAME" VARCHAR2(255 CHAR)
    ) SEGMENT CREATION IMMEDIATE
    PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
    NOCOMPRESS LOGGING
    STORAGE(
      INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
      PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
      BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
    )
    TABLESPACE "GEO_PORTAL_V2" ;

    --------------------------------------------------------
    --  DDL for Table COUNTRY
    --------------------------------------------------------

    CREATE TABLE "GEO"."CITY" (
      "ID" NUMBER(19,0),
      "NAME" VARCHAR2(255 CHAR)
    ) SEGMENT CREATION IMMEDIATE
    PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
    NOCOMPRESS LOGGING
    STORAGE(
      INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
      PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
      BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
    )
    TABLESPACE "GEO_PORTAL_V2" ;

    """,
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "manifest.csv")
    assert (
        manifest
        == """
    id | d | r | b | m | property | type    | ref | source  | prepare | level | access | uri | title | description
       | datasets/gov/example     |         |     |         |         |       |        |     |       |
       |   | resource1            | sqldump |     | -       |         |       |        |     |       |
       |                          |         |     |         |         |       |        |     |       |
       |   |   |   | Country      |         |     | COUNTRY |         |       |        |     |       |
       |                          |         |     |         |         |       |        |     |       |
       |   |   |   | City         |         |     | CITY    |         |       |        |     |       |
    """
    )


@pytest.mark.skip(reason="sqldump not fully implemented")
def test_inspect_oracle_sqldump_file_with_formula(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    (tmp_path / "dump.sql").write_text(
        """
    -- Šalys
    CREATE TABLE "GEO"."COUNTRY" (
      "ID" NUMBER(19,0),
      "NAME" VARCHAR2(255 CHAR)
    );
    """,
        encoding="iso-8859-4",
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sqldump",
            tmp_path / "dump.sql",
            "-f",
            'file(self, encoding: "iso-8859-4")',
            "-o",
            tmp_path / "manifest.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "manifest.csv")
    dataset = commands.get_dataset(context, manifest, "datasets/gov/example")
    dataset.resources["resource1"].external = "dump.sql"
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref | source   | prepare
    datasets/gov/example     |         |     |          |
      | resource1            | sqldump |     | dump.sql | file(self, encoding: 'iso-8859-4')
                             |         |     |          |
      |   |   | Country      |         |     | COUNTRY  |
    """
    )


def test_inspect_with_schema(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "CITY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
    d | r | m | property | type | source       | prepare
    dataset              |      |              |
      | schema           | sql  | {sqlite.dsn} | connect(self, schema: null)
    """,
    )

    cli.invoke(rc_new, ["inspect", tmp_path / "manifest.csv", "-o", tmp_path / "result.csv"])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    commands.get_dataset(context, manifest, "dataset").resources["schema"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
    d | r | b | m | property | type    | ref | source | prepare
    dataset                  |         |     |        |
      | schema               | sql     |     | sqlite | connect(self, schema: null)
                             |         |     |        |
      |   |   | City         |         | id  | CITY   |
      |   |   |   | id       | integer |     | ID     |
      |   |   |   | name     | string  |     | NAME   |
    """,
        context,
    )
    assert a == b


def test_inspect_update_existing_manifest(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )

    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
    d | r | m | property | type    | ref | source | prepare | access  | title
    datasets/gov/example |         |     |        |         |         | Example
      | schema           | sql     | sql |        |         |         |
                         |         |     |        |         |         |
      |   | City         |         | id  | CITY   | id > 1  |         | City
      |   |   | id       | integer |     | ID     |         | private |
      |   |   | name     | string  |     | NAME   | strip() | open    | City name
    """,
    )

    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    a, b = compare_manifest(
        manifest,
        """
    d | r | b | m | property | type    | ref     | source  | prepare | access  | title
    datasets/gov/example     |         |         |         |         |         | Example
      | schema               | sql     | sql     |         |         |         |
                             |         |         |         |         |         |
      |   |   | City         |         | id      | CITY    | id>1    |         | City
      |   |   |   | id       | integer |         | ID      |         | private |
      |   |   |   | name     | string  |         | NAME    | strip() | open    | City name
      |   |   |   | country  | ref     | Country | COUNTRY |         |         |
                             |         |         |         |         |         |
      |   |   | Country      |         | id      | COUNTRY |         |         |
      |   |   |   | id       | integer |         | ID      |         |         |
      |   |   |   | name     | string  |         | NAME    |         |         |
    """,
        context,
    )
    assert a == b


def test_inspect_update_existing_ref_manifest_priority(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )

    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
    d | r | m | property | type    | ref | source  | prepare | access  | title
    datasets/gov/example |         |     |         |         |         | Example
      | schema           | sql     | sql |         |         |         |
                         |         |     |         |         |         |
      |   | Country      |         | id  | COUNTRY |         |         | Country
      |   |   | id       | integer |     | ID      |         | private | Primary key
      |   |   | name     | string  |     | NAME    |         | open    | Country name
                         |         |     |         |         |         |
      |   | City         |         | id  | CITY    | id > 1  |         | City
      |   |   | id       | integer |     | ID      |         | private |
      |   |   | name     | string  |     | NAME    | strip() | open    | City name
      |   |   | country  | integer |     | COUNTRY |         | open    | Country id
    """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    a, b = compare_manifest(
        manifest,
        """
    d | r | b | m | property | type    | ref     | source  | prepare | access  | title
    datasets/gov/example     |         |         |         |         |         | Example
      | schema               | sql     | sql     |         |         |         |
                             |         |         |         |         |         |
      |   |   | Country      |         | id      | COUNTRY |         |         | Country
      |   |   |   | id       | integer |         | ID      |         | private | Primary key
      |   |   |   | name     | string  |         | NAME    |         | open    | Country name
                             |         |         |         |         |         |
      |   |   | City         |         | id      | CITY    | id>1    |         | City
      |   |   |   | id       | integer |         | ID      |         | private |
      |   |   |   | name     | string  |         | NAME    | strip() | open    | City name
      |   |   |   | country  | integer |         | COUNTRY |         | open    | Country id
    """,
        context,
    )
    assert a == b


def test_inspect_update_existing_ref_external_priority(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )

    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
    d | r | m | property | type    | ref | source  | prepare | access  | title
    datasets/gov/example |         |     |         |         |         | Example
      | schema           | sql     | sql |         |         |         |
                         |         |     |         |         |         |
      |   | Country      |         | id  | COUNTRY |         |         | Country
      |   |   | id       | integer |     | ID      |         | private | Primary key
      |   |   | name     | string  |     | NAME    |         | open    | Country name
                         |         |     |         |         |         |
      |   | City         |         | id  | CITY    | id > 1  |         | City
      |   |   | id       | integer |     | ID      |         | private |
      |   |   | name     | string  |     | NAME    | strip() | open    | City name
      |   |   | country  | integer |     | COUNTRY |         | open    | Country id
    """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-p",
            "external",
            "-o",
            tmp_path / "result.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    a, b = compare_manifest(
        manifest,
        """
    d | r | b | m | property | type    | ref     | source  | prepare | access  | title
    datasets/gov/example     |         |         |         |         |         | Example
      | schema               | sql     | sql     |         |         |         |
                             |         |         |         |         |         |
      |   |   | Country      |         | id      | COUNTRY |         |         | Country
      |   |   |   | id       | integer |         | ID      |         | private | Primary key
      |   |   |   | name     | string  |         | NAME    |         | open    | Country name
                             |         |         |         |         |         |
      |   |   | City         |         | id      | CITY    | id>1    |         | City
      |   |   |   | id       | integer |         | ID      |         | private |
      |   |   |   | name     | string  |         | NAME    | strip() | open    | City name
      |   |   |   | country  | ref     | Country | COUNTRY |         | open    | Country id
    """,
        context,
    )
    assert a == b


def test_inspect_with_empty_config_dir(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    # Change config dir
    (tmp_path / "config").mkdir()
    rc_new = rc_new.fork(
        {
            "config_path": tmp_path / "config",
        }
    )

    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            tmp_path / "result.csv",
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref | source
    db_sqlite                |         |     |
      | resource1            | sql     |     | sqlite
                             |         |     |
      |   |   | Country      |         | id  | COUNTRY
      |   |   |   | id       | integer |     | ID
      |   |   |   | name     | string  |     | NAME
    """
    )


def test_inspect_duplicate_table_names(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "__COUNTRY": [sa.Column("NAME", sa.Text)],
            "_COUNTRY": [sa.Column("NAME", sa.Text)],
            "COUNTRY": [sa.Column("NAME", sa.Text)],
        }
    )

    result_file_path = tmp_path / "result.csv"
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            result_file_path,
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref | source
    db_sqlite                |         |     |
      | resource1            | sql     |     | sqlite
                             |         |     |
      |   |   | Country      |         |     | COUNTRY
      |   |   |   | name     | string  |     | NAME
                             |         |     |
      |   |   | Country1     |         |     | _COUNTRY
      |   |   |   | name     | string  |     | NAME
                             |         |     |
      |   |   | Country2     |         |     | __COUNTRY
      |   |   |   | name     | string  |     | NAME
    """
    )


def test_inspect_duplicate_column_names(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("__NAME", sa.Text),
                sa.Column("_NAME", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            result_file_path,
        ],
    )

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref | source
    db_sqlite                |         |     |
      | resource1            | sql     |     | sqlite
                             |         |     |
      |   |   | Country      |         |     | COUNTRY
      |   |   |   | name_2   | string  |     | NAME
      |   |   |   | name_1   | string  |     | _NAME
      |   |   |   | name     | string  |     | __NAME
    """
    )


def test_inspect_existing_duplicate_table_names(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "__COUNTRY": [sa.Column("NAME", sa.Text)],
            "_COUNTRY": [sa.Column("NAME", sa.Text)],
            "COUNTRY": [sa.Column("NAME", sa.Text)],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         | id  |         |         |         | Country
         |   |   | id       | integer |     |         |         | private | Primary key
         |   |   | name     | string  |     |         |         | open    | Country name
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref | source    | prepare | access  | title
       datasets/gov/example |         |     |           |         |         | Example
         | schema           | sql     | sql |           |         |         |
                            |         |     |           |         |         |
         |   | Country      |         | id  |           |         |         | Country
         |   |   | id       | integer |     |           |         | private | Primary key
         |   |   | name     | string  |     |           |         | open    | Country name
                            |         |     |           |         |         |
         |   | Country1     |         |     | COUNTRY   |         |         |
         |   |   | name     | string  |     | NAME      |         |         |
                            |         |     |           |         |         |
         |   | Country11    |         |     | _COUNTRY  |         |         |
         |   |   | name     | string  |     | NAME      |         |         |
                            |         |     |           |         |         |
         |   | Country2     |         |     | __COUNTRY |         |         |
         |   |   | name     | string  |     | NAME      |         |         |
       """,
        context,
    )
    assert a == b


def test_inspect_existing_duplicate_column_names(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("__NAME", sa.Text),
                sa.Column("_NAME", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     |         |         | open    | Country name
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     |         |         | open    | Country name
         |   |   | name_2   | string  |     | NAME    |         |         |
         |   |   | name_1   | string  |     | _NAME   |         |         |
         |   |   | name_3   | string  |     | __NAME  |         |         |
       """,
        context,
    )
    assert a == b


def test_inspect_insert_new_dataset(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
                            |         |     |         |         |         |
         |   | Country      |         |     |         |         |         | Country
         |   |   | name     | string  |     |         |         | open    | Country name
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
                            |         |     |         |         |         |
         |   | Country      |         |     |         |         |         | Country
         |   |   | name     | string  |     |         |         | open    | Country name
       db_sqlite            |         |     |         |         |         |
         | resource1        | sql     |     | sqlite  |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
""",
        context,
    )
    assert a == b


def test_inspect_delete_model_source(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | City         |         |     | CITY    |         |         | City
         |   |   | name     | string  |     | NAME    |         | open    | City name
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | City         |         |     |         |         |         | City
         |   |   | name     | string  |     |         |         | open    | City name
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
""",
        context,
    )
    assert a == b


def test_inspect_delete_property_source(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     | NAME    |         | open    | Country name
         |   |   | code     | string  |     | CODE    |         | open    | Country code
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     | NAME    |         | open    | Country name
         |   |   | code     | string  |     |         |         | open    | Country code
""",
        context,
    )
    assert a == b


def test_inspect_multiple_resources_all_new(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite, sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    sqlite_new.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         | schema_1         | sql     |     | {sqlite_new.dsn} |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema"].external = "sqlite"
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema_1"].external = "sqlite_new"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref | source     | prepare | access  | title
       datasets/gov/example |         |     |            |         |         | Example
         | schema           | sql     |     | sqlite     |         |         |
                            |         |     |            |         |         |
         |   | Country      |         |     | COUNTRY    |         |         |
         |   |   | name     | string  |     | NAME       |         |         |
         | schema_1         | sql     |     | sqlite_new |         |         |
                            |         |     |            |         |         |
         |   | Country1     |         |     | COUNTRY    |         |         |
         |   |   | code     | string  |     | CODE       |         |         |

""",
        context,
    )
    assert a == b


def test_inspect_multiple_resources_specific(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite, sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    sqlite_new.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
            ],
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
         | schema_1         | sql     |     | {sqlite_new.dsn} |         |         |
                            |         |     |         |         |         |
         |   | Country1     |         |     | COUNTRY |         |         |
         |   |   | code     | string  |     | CODE    |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite_new.dsn,
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema"].external = "sqlite"
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema_1"].external = "sqlite_new"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property  | type    | ref       | source     | prepare | access  | title
       datasets/gov/example  |         |           |            |         |         | Example
         | schema            | sql     |           | sqlite     |         |         |
                             |         |           |            |         |         |
         |   | Country       |         |           | COUNTRY    |         |         |
         |   |   | name      | string  |           | NAME       |         |         |
         | schema_1          | sql     |           | sqlite_new |         |         |
                             |         |           |            |         |         |
         |   | Country1      |         |           | COUNTRY    |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | continent | ref     | Continent | CONTINENT  |         |         |
                             |         |           |            |         |         |
         |   | Continent     |         | id        | CONTINENT  |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | id        | integer |           | ID         |         |         |
         |   |   | name      | string  |           | NAME       |         |         |

""",
        context,
    )
    assert a == b


def test_inspect_multiple_resources_advanced(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite, sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    sqlite_new.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
            ],
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         |   | Location     |         |     |         |         |         |
         |   |   | name     | string  |     |         |         |         |
         |   |   | type     | integer |     |         |         |         |
                            |         |     |         |         |         |
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | New          |         |     |         |         |         |
         |   |   | name     | string  |     |         |         |         |
                            |         |     |         |         |         |
         |   | NewRemoved   |         |     | NEWREMOVED |         |         |
         |   |   | name     | string  |     | NAME |         |         |
                            |         |     |         |         |         |
         |   | City         |         |     | CITY |         |         |
         |   |   | name     | string  |     | NAME |         |         |
         |   |   | removed  | string  |     | REMOVED |         |         |
                            |         |     |         |         |         |
         | /          |      |     |  |         |         |
         |   | InBetween    |         |     |         |         |         |
         |   |   | name     | string  |     |         |         |         |
         |   |   | type     | integer |     |         |         |         |

                            |         |     |         |         |         |
         | schema_1         | sql     |     | {sqlite_new.dsn} |         |         |
                            |         |     |         |         |         |
         | /          |      |     |  |         |         |
         |   | AtEnd        |         |     |         |         |         |
         |   |   | name     | string  |     |         |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema"].external = "sqlite"
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema_1"].external = "sqlite_new"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property  | type    | ref       | source     | prepare | access  | title
       datasets/gov/example  |         |           |            |         |         | Example
                             |         |           |            |         |         |
         |   | Location      |         |           |            |         |         |
         |   |   | name      | string  |           |            |         |         |
         |   |   | type      | integer |           |            |         |         |
         | schema            | sql     |           | sqlite     |         |         |
                             |         |           |            |         |         |
         |   | New           |         |           |            |         |         |
         |   |   | name      | string  |           |            |         |         |
                             |         |           |            |         |         |
         |   | NewRemoved    |         |           |            |         |         |
         |   |   | name      | string  |           |            |         |         |
                             |         |           |            |         |         |
         |   | City          |         |           | CITY       |         |         |
         |   |   | name      | string  |           | NAME       |         |         |
         |   |   | removed   | string  |           |            |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
                             |         |           |            |         |         |
         |   | Country       |         |           | COUNTRY    |         |         |
         |   |   | name      | string  |           | NAME       |         |         |
         | /                 |         |           |            |         |         |
                             |         |           |            |         |         |
         |   | InBetween     |         |           |            |         |         |
         |   |   | name      | string  |           |            |         |         |
         |   |   | type      | integer |           |            |         |         |
         | /                 |         |           |            |         |         |
                             |         |           |            |         |         |
         |   | AtEnd         |         |           |            |         |         |
         |   |   | name      | string  |           |            |         |         |
         | schema_1          | sql     |           | sqlite_new |         |         |
                             |         |           |            |         |         |
         |   | Continent     |         | id        | CONTINENT  |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | id        | integer |           | ID         |         |         |
         |   |   | name      | string  |           | NAME       |         |         |
                             |         |           |            |         |         |
         |   | Country1      |         |           | COUNTRY    |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | continent | ref     | Continent | CONTINENT  |         |         |

""",
        context,
    )
    assert a == b


def test_inspect_multiple_datasets(rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
                            |         |     |         |         |         |
       datasets/gov/loc     |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema"].external = "sqlite"
    commands.get_dataset(context, manifest, "datasets/gov/loc").resources["schema"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | sqlite  |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
                            |         |     |         |         |         |
         |   | City         |         |     | CITY    |         |         |
         |   |   | code     | string  |     | CODE    |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
       datasets/gov/loc     |         |     |         |         |         | Example
         | schema           | sql     |     | sqlite  |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
                            |         |     |         |         |         |
         |   | City         |         |     | CITY    |         |         |
         |   |   | code     | string  |     | CODE    |         |         |
         |   |   | name     | string  |     | NAME    |         |         |


""",
        context,
    )
    assert a == b


def test_inspect_multiple_datasets_advanced_manifest_priority(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
            ],
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | NewCountry   |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
                            |         |     |         |         |         |
       datasets/gov/loc     |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | CODE    |         |         |
                            |         |     |         |         |         |
         |   | NewContinent      |         |     | CONTINENT |         |         |
         |   |   | name     | string  |     | TEST    |         |         |
         |   |   | new_id     | string  |     | ID    |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema"].external = "sqlite"
    commands.get_dataset(context, manifest, "datasets/gov/loc").resources["schema"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property  | type    | ref          | source    | prepare | access  | title
       datasets/gov/example  |         |              |           |         |         | Example
         | schema            | sql     |              | sqlite    |         |         |
                             |         |              |           |         |         |
         |   | NewCountry    |         |              | COUNTRY   |         |         |
         |   |   | name      | string  |              |           |         |         |
         |   |   | code      | string  |              | CODE      |         |         |
         |   |   | continent | ref     | Continent    | CONTINENT |         |         |
                             |         |              |           |         |         |
         |   | Continent     |         | id           | CONTINENT |         |         |
         |   |   | code      | string  |              | CODE      |         |         |
         |   |   | id        | integer |              | ID        |         |         |
         |   |   | name      | string  |              | NAME      |         |         |
       datasets/gov/loc      |         |              |           |         |         | Example
         | schema            | sql     |              | sqlite    |         |         |
                             |         |              |           |         |         |
         |   | Country       |         |              | COUNTRY   |         |         |
         |   |   | name      | string  |              | CODE      |         |         |
         |   |   | continent | ref     | NewContinent | CONTINENT |         |         |
                             |         |              |           |         |         |
         |   | NewContinent  |         | new_id       | CONTINENT |         |         |
         |   |   | name      | string  |              |           |         |         |
         |   |   | new_id    | string  |              | ID        |         |         |
         |   |   | code      | string  |              | CODE      |         |         |
         |   |   | name_1    | string  |              | NAME      |         |         |
""",
        context,
    )
    assert a == b


def test_inspect_multiple_datasets_advanced_external_priority(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
            ],
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | NewCountry   |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
                            |         |     |         |         |         |
       datasets/gov/loc     |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | CODE    |         |         |
                            |         |     |         |         |         |
         |   | NewContinent      |         |     | CONTINENT |         |         |
         |   |   | name     | string  |     | TEST    |         |         |
         |   |   | new_id     | string  |     | ID    |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-p",
            "external",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["schema"].external = "sqlite"
    commands.get_dataset(context, manifest, "datasets/gov/loc").resources["schema"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property  | type    | ref          | source    | prepare | access  | title
       datasets/gov/example  |         |              |           |         |         | Example
         | schema            | sql     |              | sqlite    |         |         |
                             |         |              |           |         |         |
         |   | NewCountry    |         |              | COUNTRY   |         |         |
         |   |   | name      | string  |              |           |         |         |
         |   |   | code      | string  |              | CODE      |         |         |
         |   |   | continent | ref     | Continent    | CONTINENT |         |         |
                             |         |              |           |         |         |
         |   | Continent     |         | id           | CONTINENT |         |         |
         |   |   | code      | string  |              | CODE      |         |         |
         |   |   | id        | integer |              | ID        |         |         |
         |   |   | name      | string  |              | NAME      |         |         |
       datasets/gov/loc      |         |              |           |         |         | Example
         | schema            | sql     |              | sqlite    |         |         |
                             |         |              |           |         |         |
         |   | Country       |         |              | COUNTRY   |         |         |
         |   |   | name      | string  |              | CODE      |         |         |
         |   |   | continent | ref     | NewContinent | CONTINENT |         |         |
                             |         |              |           |         |         |
         |   | NewContinent  |         | new_id       | CONTINENT |         |         |
         |   |   | name      | string  |              |           |         |         |
         |   |   | new_id    | integer |              | ID        |         |         |
         |   |   | code      | string  |              | CODE      |         |         |
         |   |   | name_1    | string  |              | NAME      |         |         |
""",
        context,
    )
    assert a == b


def test_inspect_multiple_datasets_different_resources(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite, sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
            ],
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )
    sqlite_new.init(
        {
            "CAR": [
                sa.Column("NAME", sa.Text),
                sa.Column("ENGINE", sa.Integer, sa.ForeignKey("ENGINE.ID")),
            ],
            "ENGINE": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/loc |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
       datasets/gov/car     |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite_new.dsn} |         |         |
                            |         |     |         |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/car").resources["schema"].external = "sqlite_new"
    commands.get_dataset(context, manifest, "datasets/gov/loc").resources["schema"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property  | type    | ref       | source     | prepare | access  | title
       datasets/gov/loc      |         |           |            |         |         | Example
         | schema            | sql     |           | sqlite     |         |         |
                             |         |           |            |         |         |
         |   | Continent     |         | id        | CONTINENT  |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | id        | integer |           | ID         |         |         |
         |   |   | name      | string  |           | NAME       |         |         |
                             |         |           |            |         |         |
         |   | Country       |         |           | COUNTRY    |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | continent | ref     | Continent | CONTINENT  |         |         |
       datasets/gov/car      |         |           |            |         |         | Example
         | schema            | sql     |           | sqlite_new |         |         |
                             |         |           |            |         |         |
         |   | Car           |         |           | CAR        |         |         |
         |   |   | engine    | ref     | Engine    | ENGINE     |         |         |
         |   |   | name      | string  |           | NAME       |         |         |
                             |         |           |            |         |         |
         |   | Engine        |         | id        | ENGINE     |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | id        | integer |           | ID         |         |         |
         |   |   | name      | string  |           | NAME       |         |         |

""",
        context,
    )
    assert a == b


def test_inspect_multiple_datasets_different_resources_specific(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite, sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
            ],
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )
    sqlite_new.init(
        {
            "CAR": [
                sa.Column("NAME", sa.Text),
                sa.Column("ENGINE", sa.Integer, sa.ForeignKey("ENGINE.ID")),
            ],
            "ENGINE": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/loc |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | NewContinent    |         | id  | CONTINENT |         |         |
         |   |   | code     | string  |     | CODE    |         |         |
         |   |   | id       | integer |     | ID      |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
       datasets/gov/car     |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite_new.dsn} |         |         |
                            |         |     |         |         |         |
         |   | NewCar          |         |     | CAR     |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
         |   |   | motor     | string  |     | MOTOR    |         |         |
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-r",
            "sql",
            sqlite_new.dsn,
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/car").resources["schema"].external = "sqlite_new"
    commands.get_dataset(context, manifest, "datasets/gov/loc").resources["schema"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref    | source     | prepare | access  | title
       datasets/gov/loc     |         |        |            |         |         | Example
         | schema           | sql     |        | sqlite     |         |         |
                            |         |        |            |         |         |
         |   | NewContinent |         | id     | CONTINENT  |         |         |
         |   |   | code     | string  |        | CODE       |         |         |
         |   |   | id       | integer |        | ID         |         |         |
         |   |   | name     | string  |        | NAME       |         |         |
       datasets/gov/car     |         |        |            |         |         | Example
         | schema           | sql     |        | sqlite_new |         |         |
                            |         |        |            |         |         |
         |   | NewCar       |         |        | CAR        |         |         |
         |   |   | name     | string  |        | NAME       |         |         |
         |   |   | motor    | string  |        |            |         |         |
         |   |   | engine   | ref     | Engine | ENGINE     |         |         |
                            |         |        |            |         |         |
         |   | Engine       |         | id     | ENGINE     |         |         |
         |   |   | code     | string  |        | CODE       |         |         |
         |   |   | id       | integer |        | ID         |         |         |
         |   |   | name     | string  |        | NAME       |         |         |
""",
        context,
    )
    assert a == b


def test_inspect_with_views(rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, sqlite: Sqlite):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("CODE", sa.Text),
                sa.Column("CONTINENT", sa.Integer, sa.ForeignKey("CONTINENT.ID")),
            ],
            "CONTINENT": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
                sa.Column("CODE", sa.Text),
            ],
        }
    )
    sqlite.engine.execute("""
        CREATE VIEW TestView
        AS SELECT a.CODE, a.CONTINENT, b.NAME FROM COUNTRY a, CONTINENT b
        WHERE a.CODE = b.CODE;
    """)

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            sqlite.dsn,
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"
    commands.get_dataset(context, manifest, "db_sqlite/views").resources["resource1"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property  | type    | ref       | source     | prepare | access  | title
       db_sqlite             |         |           |            |         |         |
         | resource1         | sql     |           | sqlite     |         |         |
                             |         |           |            |         |         |
         |   | Continent     |         | id        | CONTINENT  |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | id        | integer |           | ID         |         |         |
         |   |   | name      | string  |           | NAME       |         |         |
                             |         |           |            |         |         |
         |   | Country       |         |           | COUNTRY    |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | continent | ref     | Continent | CONTINENT  |         |         |
       db_sqlite/views       |         |           |            |         |         |
         | resource1         | sql     |           | sqlite     |         |         |
                             |         |           |            |         |         |
         |   | TestView      |         |           | TestView   |         |         |
         |   |   | code      | string  |           | CODE       |         |         |
         |   |   | continent | integer |           | CONTINENT  |         |         |
         |   |   | name      | string  |           | NAME       |         |         |

""",
        context,
    )
    assert a == b


@pytest.mark.skip(reason="Requires #440 task")
def test_inspect_with_manifest_backends(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        sqlite,
        tmp_path / "manifest.csv",
        f"""
       d | r | m | property | type    | ref  | source       | prepare | access  | title
         | test             | sql     |      | {sqlite.dsn} |         |         |
                            |         |      |              |         |         |
       datasets/gov/example |         |      |              |         |         | Example
         | schema           | sql     | test |              |         |         |
                            |         |      |              |         |         |
         |   | Country      |         |      | COUNTRY      |         |         | Country
         |   |   | name     | string  |      | NAME         |         | open    | Country name
         |   |   | code     | string  |      | CODE         |         | open    | Country code
       """,
    )
    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/gov/example").resources["test"].external = "sqlite"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property | type    | ref  | source  | prepare | access  | title
       datasets/gov/example |         |      |         |         |         | Example
         | test             | sql     |      | sqlite  |         |         |
                            |         |      |         |         |         |
         | schema           | sql     | test |         |         |         |
                            |         |      |         |         |         |
         |   | Country      |         |      | COUNTRY |         |         | Country
         |   |   | name     | string  |      | NAME    |         | open    | Country name
         |   |   | code     | string  |      |         |         | open    | Country code
""",
        context,
    )
    assert a == b


def test_inspect_json_model_ref_change(rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    json_manifest = [
        {
            "name": "Lithuania",
            "code": "LT",
            "location": {"latitude": 54.5, "longitude": 12.6},
            "cities": [
                {"name": "Vilnius", "weather": {"temperature": 24.7, "wind_speed": 12.4}},
                {"name": "Kaunas", "weather": {"temperature": 29.7, "wind_speed": 11.4}},
            ],
        },
        {"name": "Latvia", "code": "LV", "cities": [{"name": "Riga"}]},
    ]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))
    context = create_test_context(rc_new)

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc_new = configure(
        context,
        rc_new,
        None,
        tmp_path / "manifest.csv",
        f"""
           d | r | m      | property            | type                   | ref    | source              
           datasets/json/inspect                |                        |        |
             | resource                         | dask/json              |        | {path}
                                                |                        |        |
             |   | Pos    |                     |                        | code   | .
             |   |        | name                | string required unique |        | name
             |   |        | code                | string required unique |        | code
             |   |        | location_latitude   | number unique          |        | location.latitude
             |   |        | location_longitude  | number unique          |        | location.longitude
                                                |                        |        |
             |   | Cities |                     |                        |        | cities
             |   |        | name                | string required unique |        | name
             |   |        | weather_temperature | number unique          |        | weather.temperature
             |   |        | weather_wind_speed  | number unique          |        | weather.wind_speed
           """,
    )

    cli.invoke(
        rc_new,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "datasets/json/inspect").resources["resource"].external = "resource.json"
    a, b = compare_manifest(
        manifest,
        """
        d | r | m | property            | type                   | ref  | source
        datasets/json/inspect           |                        |      |
          | resource                    | dask/json              |      | resource.json
                                        |                        |      |
          |   | Pos                     |                        | code | .
          |   |   | name                | string required unique |      | name
          |   |   | code                | string required        |      | code
          |   |   | location_latitude   | number unique          |      | location.latitude
          |   |   | location_longitude  | number unique          |      | location.longitude
                                        |                        |      |
          |   | Cities                  |                        |      | cities
          |   |   | name                | string required unique |      | name
          |   |   | weather_temperature | number unique          |      | weather.temperature
          |   |   | weather_wind_speed  | number unique          |      | weather.wind_speed
          |   |   | parent              | ref                    | Pos  | ..
    """,
        context,
    )
    assert a == b


def test_inspect_xml_model_ref_change(rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    xml = """
    <countries>
        <country name="Lithuania" code="LT">
            <location latitude="54.5" longitude="12.6"/>
            <city name="Vilnius">
                <weather>
                    <temperature>24.7</temperature>
                    <wind_speed>12.4</wind_speed>
                </weather>
            </city>
            <city name="Kaunas">
                <weather>
                    <temperature>29.7</temperature>
                    <wind_speed>11.4</wind_speed>
                </weather>
            </city>
        </country>
        <country name="Latvia" code="LV">
            <city name="Riga"/>
            <city name="Test"/>
        </country>
    </countries>
"""
    path = tmp_path / "manifest.xml"
    path.write_text(xml)
    context = create_test_context(rc)

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    rc = configure(
        context,
        rc,
        None,
        tmp_path / "manifest.csv",
        f"""
           d | r | m | property            | type                   | ref    | source              
           datasets/xml/inspect            |                        |        |
             | resource                    | dask/xml               |        | {path}
                                           |                        |        |
             |   | Country                 |                        | code   | /countries/country
             |   |   | name                | string required unique |        | @name
             |   |   | code                | string required unique |        | @code
             |   |   | location_latitude   | number unique          |        | location/@latitude
             |   |   | location_longitude  | number unique          |        | location/@longitude
                                           |                        |        |
             |   | City                    |                        |        | /countries/country/city
             |   |   | name                | string required unique |        | @name
             |   |   | weather_temperature | number unique          |        | weather/temperature
             |   |   | weather_wind_speed  | number unique          |        | weather/wind_speed
           """,
    )

    cli.invoke(
        rc,
        [
            "inspect",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc, result_file_path)
    commands.get_dataset(context, manifest, "datasets/xml/inspect").resources["resource"].external = "resource.xml"
    a, b = compare_manifest(
        manifest,
        """
        d | r | m | property            | type                   | ref     | source
        datasets/xml/inspect            |                        |         |
          | resource                    | dask/xml               |         | resource.xml
                                        |                        |         |
          |   | Country                 |                        | code    | /countries/country
          |   |   | name                | string required unique |         | @name
          |   |   | code                | string required        |         | @code
          |   |   | location_latitude   | number unique          |         | location/@latitude
          |   |   | location_longitude  | number unique          |         | location/@longitude
                                        |                        |         |
          |   | City                    |                        |         | /countries/country/city
          |   |   | name                | string required unique |         | @name
          |   |   | weather_temperature | number unique          |         | weather/temperature
          |   |   | weather_wind_speed  | number unique          |         | weather/wind_speed
          |   |   | country             | ref                    | Country | ..
    """,
        context,
    )
    assert a == b


def test_inspect_with_postgresql_schema(rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, postgresql):
    db = f"{postgresql}/inspect_schema"
    if su.database_exists(db):
        su.drop_database(db)
    su.create_database(db)
    engine = sa.create_engine(db)
    with engine.connect() as conn:
        engine.execute(sa.schema.CreateSchema("test_schema"))
        meta = sa.MetaData(conn, schema="test_schema")
        sa.Table("cities", meta, sa.Column("id", sa.Integer), sa.Column("name", sa.Text))
        meta.create_all()

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    # Default schema
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            db,
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "inspect_schema/test_schema").resources["resource1"].external = "postgresql"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property       | type    | ref       | source             | prepare | access  | title
       inspect_schema/test_schema |         |           |                    |         |         |
         | resource1              | sql     |           | postgresql         |         |         |
                                  |         |           |                    |         |         |
         |   | Cities             |         |           | test_schema.cities |         |         |
         |   |   | id             | integer |           | id                 |         |         |
         |   |   | name           | string  |           | name               |         |         |
""",
        context,
    )
    assert a == b

    # Configure Spinta.
    # Test schema
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            db,
            "-f",
            "connect(schema: test_schema)",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "inspect_schema").resources["resource1"].external = "postgresql"
    a, b = compare_manifest(
        manifest,
        """
       d | r | m | property  | type    | ref       | source     | prepare                      | access  | title
       inspect_schema        |         |           |            |                              |         |
         | resource1         | sql     |           | postgresql | connect(schema: test_schema) |         |
                             |         |           |            |                              |         |
         |   | Cities        |         |           | cities     |                              |         |
         |   |   | id        | integer |           | id         |                              |         |
         |   |   | name      | string  |           | name       |                              |         |
""",
        context,
    )
    assert a == b

    if su.database_exists(db):
        su.drop_database(db)


def test_inspect_with_postgresql_multi_schema_references(
    rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path, postgresql
):
    db = f"{postgresql}/inspect_schema"
    if su.database_exists(db):
        su.drop_database(db)
    su.create_database(db)
    engine = sa.create_engine(db)
    with engine.connect() as conn:
        engine.execute(sa.schema.CreateSchema("users"))
        engine.execute(sa.schema.CreateSchema("finances"))
        meta = sa.MetaData(conn)
        sa.Table(
            "Client", meta, sa.Column("id", sa.Integer, primary_key=True), sa.Column("name", sa.Text), schema="users"
        )
        sa.Table(
            "Record",
            meta,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("name", sa.Text),
            sa.Column("client", sa.Integer),
            sa.ForeignKeyConstraint(["client"], ["users.Client.id"]),
            schema="finances",
        )
        meta.create_all()

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.
    # Default schema
    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "sql",
            db,
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "inspect_schema/users").resources["resource1"].external = "postgresql"
    commands.get_dataset(context, manifest, "inspect_schema/finances").resources["resource1"].external = "postgresql"
    a, b = compare_manifest(
        manifest,
        """
        d | r | m | property    | type    | ref                          | source          | prepare | access  | title
        inspect_schema/finances |         |                              |                 |         |         |
          | resource1           | sql     |                              | postgresql      |         |         |
                                |         |                              |                 |         |         |
          |   | Record          |         | id                           | finances.Record |         |         |
          |   |   | client      | ref     | /inspect_schema/users/Client | client          |         |         |
          |   |   | id          | integer |                              | id              |         |         |
          |   |   | name        | string  |                              | name            |         |         |
        inspect_schema/users    |         |                              |                 |         |         |
          | resource1           | sql     |                              | postgresql      |         |         |
                                |         |                              |                 |         |         |
          |   | Client          |         | id                           | users.Client    |         |         |
          |   |   | id          | integer |                              | id              |         |         |
          |   |   | name        | string  |                              | name            |         |         |
""",
        context,
    )
    assert a == b
    if su.database_exists(db):
        su.drop_database(db)


def test_inspect_json_blank_node(rc_new: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    json_manifest = {"id": 5, "test": 10}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))

    result_file_path = tmp_path / "result.csv"
    # Configure Spinta.

    cli.invoke(
        rc_new,
        [
            "inspect",
            "-r",
            "dask/json",
            path,
            "-o",
            tmp_path / "result.csv",
        ],
    )
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "resource.json"
    a, b = compare_manifest(
        manifest,
        """
d | r | m | property | type                    | ref    | source
dataset              |                         |        |
  | resource         | dask/json               |        | resource.json
                     |                         |        |
  |   | Model1       |                         |        | .
  |   |   | id       | integer required unique |        | id
  |   |   | test     | binary required unique  |        | test
    """,
        context,
    )
    assert a == b


def test_priority_key_eq():
    old = PriorityKey()
    new = PriorityKey()
    assert old != new

    old = PriorityKey(_id="5")
    new = PriorityKey(source="5")
    assert old != new

    old = PriorityKey(_id="5")
    new = PriorityKey(_id="5")
    assert old == new

    old = PriorityKey(_id="5", name="test")
    new = PriorityKey(_id="2", name="test")
    assert old == new

    old = PriorityKey(_id="5", name="test")
    new = PriorityKey(_id="2", name="testas")
    assert old != new

    old = PriorityKey(_id="5", name="test", source="asd")
    new = PriorityKey(_id="2", name="testas", source="asd")
    assert old == new

    old = PriorityKey(name="test", source="asd")
    new = PriorityKey(name="testas", source="asd")
    assert old == new

    old = PriorityKey(name="test", source="asd")
    new = PriorityKey(name="testas", source="asds")
    assert old != new

    old = PriorityKey(name="test", source="asd")
    new = PriorityKey(name="test", source="asds")
    assert old == new

    old = PriorityKey(source="asd")
    new = PriorityKey(source="asds")
    assert old != new

    old = PriorityKey(source="asd")
    new = PriorityKey(source="asd")
    assert old == new

    old = PriorityKey(source=tuple(["asd"]))
    new = PriorityKey(source=tuple(["asd"]))
    assert old == new

    old = PriorityKey(source=("asd", "new"))
    new = PriorityKey(source=tuple(["asd"]))
    assert old == new

    old = PriorityKey(source=tuple(["asd"]))
    new = PriorityKey(source=("asd", "new"))
    assert old == new

    old = PriorityKey(source=tuple(["zxc"]))
    new = PriorityKey(source=("asd", "new"))
    assert old != new

    old = PriorityKey(source=("asd", "new"))
    new = PriorityKey(source=tuple(["asd"]))
    assert old in [new]


def test_inspect_blob_types(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    """
    Test binary/BLOB type inspection via CLI (issue #1484).

    This functional test verifies the full inspect workflow generates
    correct manifests with binary types. Unit tests in
    tests/datasets/sql/test_inspect.py verify that MySQL-specific BLOB
    types (TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB) are correctly mapped.
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
    cli.invoke(rc_new, ["inspect", sqlite.dsn, "-o", tmp_path / "result.csv"])

    # Check what was detected
    context, manifest = load_manifest_and_context(rc_new, tmp_path / "result.csv")
    commands.get_dataset(context, manifest, "db_sqlite").resources["resource1"].external = "sqlite"

    # Verify binary columns are correctly mapped
    assert (
        manifest
        == """
    id | d | r | b | m | property  | type    | ref | source    | source.type | prepare | origin | count | level | status  | visibility | access | uri | eli | title | description
       | db_sqlite                 |         |     |           |             |         |        |       |       |         |            |        |     |     |       |
       |   | resource1             | sql     |     | sqlite    |             |         |        |       |       |         |            |        |     |     |       |
       |                           |         |     |           |             |         |        |       |       |         |            |        |     |     |       |
       |   |   |   | Providers     |         | id  | PROVIDERS |             |         |        |       |       | develop | private    |        |     |     |       |
       |   |   |   |   | document  | binary  |     | DOCUMENT  |             |         |        |       |       | develop | private    |        |     |     |       |
       |   |   |   |   | icon      | binary  |     | ICON      |             |         |        |       |       | develop | private    |        |     |     |       |
       |   |   |   |   | id        | integer |     | ID        |             |         |        |       |       | develop | private    |        |     |     |       |
       |   |   |   |   | logo      | binary  |     | LOGO      |             |         |        |       |       | develop | private    |        |     |     |       |
       |   |   |   |   | name      | string  |     | NAME      |             |         |        |       |       | develop | private    |        |     |     |       |
    """
    )
