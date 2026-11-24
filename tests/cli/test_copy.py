import csv
import io
import json
from pathlib import Path

from pytest import mark

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.lib import Spinta
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest
from spinta.testing.tabular import create_tabular_manifest


def test_copy(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    rc = rc.fork({"default_access_level": "protected"})
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access
    datasets/gov/example     |        |         |             |         |
      | data                 | sql    |         |             |         |
                             |        |         |             |         |
      |   |   | Country      |        | code    | salis       |         |
      |   |   |   | code     | string |         | kodas       |         | public
      |   |   |   | name     | string |         | pavadinimas |         | open
      |   |   |   | driving  | string |         | vairavimas  |         | open
                             | enum   |         | l           | 'left'  | open
                             |        |         | r           | 'right' | open
                             |        |         |             |         |
      |   |   | City         |        | name    | miestas     |         |
      |   |   |   | name     | string |         | pavadinimas |         | open
      |   |   |   | country  | ref    | Country | salis       |         | open
                             |        |         |             |         |
      |   |   | Capital      |        | name    | miestas     |         |
      |   |   |   | name     | string |         | pavadinimas |         |
      |   |   |   | country  | ref    | Country | salis       |         |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref     | source | prepare | access
    datasets/gov/example     |        |         |        |         |
                             |        |         |        |         |
      |   |   | Country      |        |         |        |         |
      |   |   |   | name     | string |         |        |         | open
      |   |   |   | driving  | string |         |        |         | open
                             | enum   |         |        | 'left'  | open
                             |        |         |        | 'right' | open
                             |        |         |        |         |
      |   |   | City         |        | name    |        |         |
      |   |   |   | name     | string |         |        |         | open
      |   |   |   | country  | ref    | Country |        |         | open
    """
    )


def test_copy_enum_0(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref     | source      | prepare | access
    datasets/gov/example     |         |         |             |         |
      | data                 | sql     |         |             |         |
                             |         |         |             |         |
      |   |   | Country      |         | name    | salis       |         |
      |   |   |   | name     | string  |         | pavadinimas |         | open
      |   |   |   | driving  | integer |         | vairavimas  |         | open
                             | enum    |         | l           | 0       | open
                             |         |         | r           | 1       | open
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref     | source | prepare | access
    datasets/gov/example     |         |         |        |         |
                             |         |         |        |         |
      |   |   | Country      |         | name    |        |         |
      |   |   |   | name     | string  |         |        |         | open
      |   |   |   | driving  | integer |         |        |         | open
                             | enum    |         |        | 0       | open
                             |         |         |        | 1       | open
    """
    )


def test_copy_global_enum(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref       | source      | prepare | access
    datasets/gov/example     |         |           |             |         |
                             | enum    | direction | l           | 0       |
                             |         |           | r           | 1       |
      | data                 | sql     |           |             |         |
                             |         |           |             |         |
      |   |   | Country      |         | name      | salis       |         |
      |   |   |   | name     | string  |           | pavadinimas |         | open
      |   |   |   | driving  | integer | direction | vairavimas  |         | open
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "open",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref       | source | prepare | access
    datasets/gov/example     |         |           |        |         |
                             | enum    | direction |        | 0       |
                             |         |           |        | 1       |
                             |         |           |        |         |
      |   |   | Country      |         | name      |        |         |
      |   |   |   | name     | string  |           |        |         | open
      |   |   |   | driving  | integer | direction |        |         | open
    """
    )


@mark.parametrize("type", ["dask/xml", "sql/sqlite"])
def test_copy_with_filters_and_externals(context: Context, rc, cli, tmp_path, type: str):
    rc = rc.fork({"default_access_level": "protected"})
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable(f"""
    d | r | b | m | property | type   | ref     | source      | prepare   | access
    datasets/gov/example     |        |         |             |           |
      | data                 | {type} |         |             |           |
                             |        |         |             |           |
      |   |   | Country      |        | code    | salis       | code='lt' |
      |   |   |   | code     | string |         | kodas       |           | private
      |   |   |   | name     | string |         | pavadinimas |           | open
      |   |   |   | driving  | string |         | vairavimas  |           | open
      |   |   |   |          | enum   |         | l           | 'left'    | private
      |   |   |   |          |        |         | r           | 'right'   | open
                             |        |         |             |           |
      |   |   | City         |        | name    | miestas     |           |
      |   |   |   | name     | string |         | pavadinimas |           | open
      |   |   |   | country  | ref    | Country | salis       |           | open
                             |        |         |             |           |
      |   |   | Capital      |        | name    | miestas     |           |
      |   |   |   | name     | string |         | pavadinimas |           |
      |   |   |   | country  | ref    | Country | salis       |           |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--access",
            "open",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == f"""
    d | r | b | m | property | type   | ref     | source      | prepare   | level | access
    datasets/gov/example     |        |         |             |           |       |
      | data                 | {type} |         |             |           |       |
                             |        |         |             |           |       |
      |   |   | Country      |        |         | salis       | code='lt' |       |
      |   |   |   | name     | string |         | pavadinimas |           |       | open
      |   |   |   | driving  | string |         | vairavimas  |           |       | open
                             | enum   |         | r           | 'right'   |       | open
                             |        |         |             |           |       |
      |   |   | City         |        | name    | miestas     |           |       |
      |   |   |   | name     | string |         | pavadinimas |           |       | open
      |   |   |   | country  | ref    | Country | salis       |           |       | open
    """
    )


def test_copy_and_format_names(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property    | type    | ref                        | source      | prepare                  | level  | access     | title
    datasets/gov/example        |         |                            |             |                          |        |            | Example dataset
      | data                    | sql     |                            |             |                          |        |            |
                                |         |                            |             |                          |        |            |
      |   |   | COUNTRY         |         | countryCode                | Šalis       | countryCode='lt'         |        |            | Countries
      |   |   |   | countryCode | string  |                            | Kodas       | lower()                  | 3      | private    | Country code
      |   |   |   | name        | string  |                            | Pavadinimas |                          | 3      | open       |
      |   |   |   | population  | integer |                            | Populiacija |                          | 4      | protected  |
      |   |   |   | id          | integer |                            |             | countryCode, name        | 2      | public     |
                                |         |                            |             |                          |        |            |
      |   |   | CITY            |         | name                       | Miestas     |                          |        |            | Cities
      |   |   |   | name        | string  |                            | Pavadinimas |                          | 1      |            |
      |   |   |   | countryName | string  |                            | Šalis       |                          | 4      |            |
      |   |   |   | countryCode | string  |                            | Kodas       |                          | 4      |            |
      |   |   |   | country     | ref     | COUNTRY[countryCode, name] |             | countryCode, countryName | 4      |            |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--format-names",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property     | type    | ref                         | source      | prepare                    | level  | access    | title
    datasets/gov/example         |         |                             |             |                            |        |           | Example dataset
      | data                     | sql     |                             |             |                            |        |           |
                                 |         |                             |             |                            |        |           |
      |   |   | Country          |         | country_code                | Šalis       | country_code='lt'          |        |           | Countries
      |   |   |   | country_code | string  |                             | Kodas       | lower()                    | 3      | private   | Country code
      |   |   |   | name         | string  |                             | Pavadinimas |                            | 3      | open      |
      |   |   |   | population   | integer |                             | Populiacija |                            | 4      | protected |
      |   |   |   | id           | integer |                             |             | country_code, name         | 2      | public    |
                                 |         |                             |             |                            |        |           |
      |   |   | City             |         | name                        | Miestas     |                            |        |           | Cities
      |   |   |   | name         | string  |                             | Pavadinimas |                            | 1      |           |
      |   |   |   | country_name | string  |                             | Šalis       |                            | 4      |           |
      |   |   |   | country_code | string  |                             | Kodas       |                            | 4      |           |
      |   |   |   | country      | ref     | Country[country_code, name] |             | country_code, country_name | 4      |           |
    """
    )


def test_copy_and_format_names_for_ref(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property     | type   | ref       | prepare
    datasets/gov/example         |        |           |
      | data                     | sql    |           |
                                 |        |           |
      |   |   | CONTINENT        |        |           | name_id='eu'
      |   |   |   | name_id      | string |           |
                                 |        |           |
      |   |   | COUNTRY          |        |           | continent_id.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | continent_id | ref    | CONTINENT |
                                 |        |           |
      |   |   | CITY             |        |           | country_id.continent_id.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | country_id   | ref    | COUNTRY   |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--format-names",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property     | type   | ref       | prepare
    datasets/gov/example         |        |           |
      | data                     | sql    |           |
                                 |        |           |
      |   |   | Continent        |        |           | name_id='eu'
      |   |   |   | name_id      | string |           |
                                 |        |           |
      |   |   | Country          |        |           | continent.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | continent    | ref    | Continent |
                                 |        |           |
      |   |   | City             |        |           | country.continent.name_id='eu'
      |   |   |   | name_id      | string |           |
      |   |   |   | country      | ref    | Country   |
    """
    )


def test_copy_and_format_names_with_formulas(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | prepare
    datasets/gov/example     |        |
      | data                 | sql    |
                             |        |
      |   |   | City         |        | NAME!=['a', 'b']\\|NAME='c'
      |   |   |   | NAME     | string |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--format-names",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | prepare
    datasets/gov/example     |        |
      | data                 | sql    |
                             |        |
      |   |   | City         |        | name!=['a', 'b']\\|name='c'
      |   |   |   | name     | string |
    """
    )


def test_copy_to_stdout(context: Context, rc, cli, tmp_path):
    manifest = striptable("""
    d | r | b | m | property | type
    datasets/gov/example     |
      | data                 | sql
                             |
      |   |   | City         |
      |   |   |   | name     | string
    """)
    create_tabular_manifest(context, tmp_path / "manifest.csv", manifest)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-c",
            "d,r,b,m,p,type",
            tmp_path / "manifest.csv",
        ],
    )

    assert result.stdout.strip() == manifest


def test_copy_order_by_access(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property   | type    | ref        | source | prepare  | access
    datasets/gov/example       |         |            |        |          |
      | data                   | sql     |            |        |          |
                               |         |            |        |          |
      |   |   | Continent      |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | private
      |   |   |   | population | string  |            |        |          | private
                               |         |            |        |          |
      |   |   | Country        |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | protected
      |   |   |   | continent  | ref     | Continent  |        |          | protected
      |   |   |   | population | string  |            |        |          | protected
      |   |   |   | driving    | string  |            |        |          | open
                               | enum    |            | l      | 'left'   | private
                               |         |            | r      | 'right'  | open
                               |         |            | c      | 'center' | public
                               |         |            |        |          |
      |   |   | City           |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | open
      |   |   |   | country    | ref     | Country    |        |          | public
      |   |   |   | population | string  |            |        |          | open
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            "--order-by",
            "access",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property   | type    | ref        | source | prepare  | access
    datasets/gov/example       |         |            |        |          |
      | data                   | sql     |            |        |          |
                               |         |            |        |          |
      |   |   | Country        |         |            |        |          |
      |   |   |   | driving    | string  |            |        |          | open
                               | enum    |            | r      | 'right'  | open
                               |         |            | c      | 'center' | public
                               |         |            | l      | 'left'   | private
      |   |   |   | name       | string  |            |        |          | protected
      |   |   |   | continent  | ref     | Continent  |        |          | protected
      |   |   |   | population | string  |            |        |          | protected
                               |         |            |        |          |
      |   |   | City           |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | open
      |   |   |   | population | string  |            |        |          | open
      |   |   |   | country    | ref     | Country    |        |          | public
                               |         |            |        |          |
      |   |   | Continent      |         |            |        |          |
      |   |   |   | name       | string  |            |        |          | private
      |   |   |   | population | string  |            |        |          | private
    """
    )


def test_copy_rename_duplicates(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property   | type
    datasets/gov/example       |
      | data                   | sql
                               |
      |   |   | City           |
      |   |   |   | name       | string
                               |
      |   |   | City           |
      |   |   |   | name       | string
                               |
      |   |   | City           |
      |   |   |   | name       | string
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--rename-duplicates",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property   | type
    datasets/gov/example       |
      | data                   | sql
                               |
      |   |   | City           |
      |   |   |   | name       | string
                               |
      |   |   | City_1         |
      |   |   |   | name       | string
                               |
      |   |   | City_2         |
      |   |   |   | name       | string
    """
    )


def test_enum_ref(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        """
    d | r | b | m | property | type    | ref     | source      | prepare | access | title
                             | enum    | sex     |             | 1       |        | Male
                             |         |         |             | 2       |        | Female
                             |         |         |             |         |        |
    datasets/gov/example     |         |         |             |         |        |
      | data                 | sql     |         |             |         |        |
                             |         |         |             |         |        |
      |   |   | Report1      |         | id      | REPORT1     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
                             |         |         |             |         |        |
      |   |   | Report2      |         | id      | REPORT2     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
    """,
    )

    cli.invoke(
        rc,
        [
            "copy",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref     | source      | prepare | access | title
                             | enum    | sex     |             | 1       |        | Male
                             |         |         |             | 2       |        | Female
                             |         |         |             |         |        |
    datasets/gov/example     |         |         |             |         |        |
      | data                 | sql     |         |             |         |        |
                             |         |         |             |         |        |
      |   |   | Report1      |         | id      | REPORT1     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
                             |         |         |             |         |        |
      |   |   | Report2      |         | id      | REPORT2     |         |        |
      |   |   |   | id       | integer |         | ID          |         | open   |
      |   |   |   | sex      | integer | sex     | SEX         |         | open   |
      |   |   |   | name     | string  |         | NAME        |         | open   |
    """
    )


def test_copy_status(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access | status
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | develop
      |   |   |   | code     | string |         | kodas       |         | public | develop
      |   |   |   | name     | string |         | pavadinimas |         | open   | completed
      |   |   |   | driving  | string |         | vairavimas  |         | open   | discont
                             | enum   |         | l           | 'left'  | open   | completed
                             |        |         | r           | 'right' | open   | discont
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        | completed
      |   |   |   | name     | string |         | pavadinimas |         | open   | deprecated
      |   |   |   | country  | ref    | Country | salis       |         | open   | withdrawn
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref     | source      | prepare | access | status
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | develop
      |   |   |   | code     | string |         | kodas       |         | public | develop
      |   |   |   | name     | string |         | pavadinimas |         | open   | completed
      |   |   |   | driving  | string |         | vairavimas  |         | open   | discont
                             | enum   |         | l           | 'left'  | open   | completed
                             |        |         | r           | 'right' | open   | discont
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        | completed
      |   |   |   | name     | string |         | pavadinimas |         | open   | deprecated
      |   |   |   | country  | ref    | Country | salis       |         | open   | withdrawn
    """
    )


def test_copy_visibility(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access | visibility
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | public
      |   |   |   | code     | string |         | kodas       |         | public | public
      |   |   |   | name     | string |         | pavadinimas |         | open   | package
      |   |   |   | driving  | string |         | vairavimas  |         | open   | protected
                             | enum   |         | l           | 'left'  | open   | public
                             |        |         | r           | 'right' | open   | package
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        |
      |   |   |   | name     | string |         | pavadinimas |         | open   | private
      |   |   |   | country  | ref    | Country | salis       |         | open   | private
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref     | source      | prepare | access | visibility
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | public
      |   |   |   | code     | string |         | kodas       |         | public | public
      |   |   |   | name     | string |         | pavadinimas |         | open   | package
      |   |   |   | driving  | string |         | vairavimas  |         | open   | protected
                             | enum   |         | l           | 'left'  | open   | public
                             |        |         | r           | 'right' | open   | package
                             |        |         |             |         |        |
      |   |   | City         |        | name    | miestas     |         |        |
      |   |   |   | name     | string |         | pavadinimas |         | open   | private
      |   |   |   | country  | ref    | Country | salis       |         | open   | private
    """
    )


def test_copy_eli(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access | eli
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | https://example.com/law/1
      |   |   |   | code     | string |         | kodas       |         | public | https://example.com/law/2
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   | https://example.com/law/3
                             | enum   |         | l           | 'left'  | open   | https://example.com/law/4
                             |        |         | r           | 'right' | open   | https://example.com/law/5
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref     | source      | prepare | access | eli
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | https://example.com/law/1
      |   |   |   | code     | string |         | kodas       |         | public | https://example.com/law/2
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   | https://example.com/law/3
                             | enum   |         | l           | 'left'  | open   | https://example.com/law/4
                             |        |         | r           | 'right' | open   | https://example.com/law/5
    """
    )


def test_copy_count(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access | count
    datasets/gov/example     |        |         |             |         |        | 4
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | 5 
      |   |   |   | code     | string |         | kodas       |         | public | 6
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   | 7
                             | enum   |         | l           | 'left'  | open   | 8
                             |        |         | r           | 'right' | open   | 9
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref     | source      | prepare | access | count
    datasets/gov/example     |        |         |             |         |        | 4
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | 5
      |   |   |   | code     | string |         | kodas       |         | public | 6
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   | 7
                             | enum   |         | l           | 'left'  | open   | 8
                             |        |         | r           | 'right' | open   | 9
    """
    )


def test_copy_origin(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access | origin
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        |
      |   |   |   | code     | string |         | kodas       |         | public |
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   |
                             | enum   |         | l           | 'left'  | open   |
                             |        |         | r           | 'right' | open   |
    datasets/gov/example2    |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |        
                             |        |         |             |         |        |              
      |   |   | Country      |        | code    | salis       |         |        | datasets/gov/example/Country
      |   |   |   | code     | string |         | kodas       |         | public | code
      |   |   |   | name     | string |         | pavadinimas |         | open   | name
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref     | source      | prepare | access | origin
    datasets/gov/example     |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        |
      |   |   |   | code     | string |         | kodas       |         | public |
      |   |   |   | name     | string |         | pavadinimas |         | open   |
      |   |   |   | driving  | string |         | vairavimas  |         | open   |
                             | enum   |         | l           | 'left'  | open   |
                             |        |         | r           | 'right' | open   |
    datasets/gov/example2    |        |         |             |         |        |
      | data                 | sql    |         |             |         |        |
                             |        |         |             |         |        |
      |   |   | Country      |        | code    | salis       |         |        | datasets/gov/example/Country
      |   |   |   | code     | string |         | kodas       |         | public | code
      |   |   |   | name     | string |         | pavadinimas |         | open   | name
    """
    )


def test_copy_source_type_resource(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         | sqlite://   | sqlite
                             |         |         |             |
      |   |   | Country      |         | code    | salis       |
      |   |   |   | code     | integer |         | kodas       |
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | driving  | string  |         | vairavimas  |
      |   |   |   |          | enum    |         | l           |
      |   |   |   |          |         |         | r           |
                             |         |         |             |
      |   |   | City         |         | name    | miestas     |
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | country  | ref     | Country | salis       |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         | sqlite://   | sqlite
                             |         |         |             |
      |   |   | Country      |         | code    | salis       |
      |   |   |   | code     | integer |         | kodas       |
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | driving  | string  |         | vairavimas  |
                             | enum    |         | l           |
                             |         |         | r           |
                             |         |         |             |
      |   |   | City         |         | name    | miestas     |
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | country  | ref     | Country | salis       |
    """
    )


def test_copy_source_type_model(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         |             |
                             |         |         |             |
      |   |   | Country      |         | code    | salis       | table
      |   |   |   | code     | integer |         | kodas       |
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | driving  | string  |         | vairavimas  |
      |   |   |   |          | enum    |         | l           |
      |   |   |   |          |         |         | r           |  
                             |         |         |             |
      |   |   | City         |         | name    | miestas     | view materialized
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | country  | ref     | Country | salis       |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         |             |
                             |         |         |             |
      |   |   | Country      |         | code    | salis       | table
      |   |   |   | code     | integer |         | kodas       |
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | driving  | string  |         | vairavimas  |
                             | enum    |         | l           |
                             |         |         | r           |
                             |         |         |             |
      |   |   | City         |         | name    | miestas     | view materialized
      |   |   |   | name     | string  |         | pavadinimas |
      |   |   |   | country  | ref     | Country | salis       |
    """
    )


def test_copy_source_type_property(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref     | source      | source.type  
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         | sqlite://   |
                             |         |         |             |
      |   |   | Country      |         | code    | salis       |
      |   |   |   | code     | integer |         | kodas       | integer 
      |   |   |   | name     | string  |         | pavadinimas | varchar(255) 
      |   |   |   | driving  | string  |         | vairavimas  | varchar(1)
      |   |   |   |          | enum    |         | l           |
      |   |   |   |          |         |         | r           |  
                             |         |         |             |
      |   |   | City         |         | name    | miestas     |
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | country  | ref     | Country | salis       | test
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         | sqlite://   |
                             |         |         |             |
      |   |   | Country      |         | code    | salis       |
      |   |   |   | code     | integer |         | kodas       | integer
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | driving  | string  |         | vairavimas  | varchar(1)
                             | enum    |         | l           |
                             |         |         | r           |
                             |         |         |             |
      |   |   | City         |         | name    | miestas     |
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | country  | ref     | Country | salis       | test
    """
    )


def test_copy_source_type_resource_model_property(context: Context, rc, cli, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         | sqlite://   | sqlite
                             |         |         |             |
      |   |   | Country      |         | code    | salis       | table
      |   |   |   | code     | integer |         | kodas       | test
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | driving  | string  |         | vairavimas  | varchar(1)
      |   |   |   |          | enum    |         | l           |
      |   |   |   |          |         |         | r           |  
                             |         |         |             |
      |   |   | City         |         | name    | miestas     | view materialized
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | country  | ref     | Country | salis       | integer
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref     | source      | source.type
    datasets/gov/example     |         |         |             |
      | data                 | sql     |         | sqlite://   | sqlite
                             |         |         |             |
      |   |   | Country      |         | code    | salis       | table
      |   |   |   | code     | integer |         | kodas       | test
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | driving  | string  |         | vairavimas  | varchar(1)
                             | enum    |         | l           |
                             |         |         | r           |
                             |         |         |             |
      |   |   | City         |         | name    | miestas     | view materialized
      |   |   |   | name     | string  |         | pavadinimas | varchar(255)
      |   |   |   | country  | ref     | Country | salis       | integer
    """
    )


def test_enum_function_noop_copy(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    primary_manifest = """
    d | r | b | m | property | type    | ref | source       | prepare
    dataset1                 |         |     |              |
      | resource1            | sql     |     |              |
                             |         |     |              |
      |   |   | City         |         | id  | CITIES       |
      |   |   |   | id       | integer |     | ID           |
      |   |   |   | country  | string  |     | COUNTRY_CODE |
                             | enum    |     | lt           |
                             |         |     | ee           |
                             |         |     | lv           |
                             |         |     |              | noop()
    """
    create_tabular_manifest(context, tmp_path / "manifest.csv", primary_manifest)

    cli.invoke(
        rc,
        [
            "copy",
            tmp_path / "manifest.csv",
            "-o",
            tmp_path / "result.csv",
        ],
    )
    manifest = load_manifest(rc, tmp_path / "result.csv")

    assert manifest == primary_manifest


def test_copy_with_resource_no_models(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source
    datasets/gov/example     |        |         |
      | empty_resource       | sql    |         |
      | data                 | sql    |         |
                             |        |         |
      |   |   | Country      |        | code    | salis
      |   |   |   | code     | string |         | kodas
      |   |   |   | name     | string |         | pavadinimas
      |   |   |   | driving  | string |         | vairavimas
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref     | source
    datasets/gov/example     |        |         |
      | empty_resource       | sql    |         |
      | data                 | sql    |         |
                             |        |         |
      |   |   | Country      |        | code    | salis
      |   |   |   | code     | string |         | kodas
      |   |   |   | name     | string |         | pavadinimas
      |   |   |   | driving  | string |         | vairavimas
    """
    )


def test_copy_property_with_underscore(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    # _id, _created, _updated, _label, _revision are allowed property names
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type   | ref     | source      | prepare | access
    datasets/gov/example      |        |         |             |         |
      | data                  | sql    |         |             |         |
                              |        |         |             |         |
      |   |   | Country       |        | code    | salis       |         |
      |   |   |   | _id       | string |         |             |         | open
      |   |   |   | code      | string |         | kodas       |         | public
      |   |   |   | name      | string |         | pavadinimas |         | open
      |   |   |   | _updated  | string |         | atnaujinta  |         | open
      |   |   |   | _created  | string |         | sukurta     |         | open
      |   |   |   | _label    | string |         | zyme        |         | public
      |   |   |   | _revision | string |         | versija     |         | public
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "public",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property  | type   | ref     | source | prepare | access
    datasets/gov/example      |        |         |        |         |
                              |        |         |        |         |
      |   |   | Country       |        | code    |        |         |
      |   |   |   | _id       | string |         |        |         | open
      |   |   |   | _revision | string |         |        |         | public
      |   |   |   | _created  | string |         |        |         | open
      |   |   |   | code      | string |         |        |         | public
      |   |   |   | name      | string |         |        |         | open
      |   |   |   | _updated  | string |         |        |         | open
      |   |   |   | _label    | string |         |        |         | public
    """
    )


def test_copy_property_with_underscore_reserved(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    # _op is a reserved property name for internal use only
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property   | type   | source | access
    datasets/gov/example       |        |        |
      |   |   | Country        |        | salis  |
      |   |   |   | code       | string | kodas  | public
      |   |   |   | _label     | string | zyme   | public
      |   |   |   | _op        | string | op     | public
    """),
    )
    result = cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "public",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "_op" in result.stderr


def test_copy_property_with_underscore_wrong(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    # _* property names are not allowed in the manifest except for the allowed ones
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property   | type   | source | access
    datasets/gov/example       |        |        |
      |   |   | Country        |        | salis  |
      |   |   |   | code       | string | kodas  | public
      |   |   |   | _test123   | string | test   | public
    """),
    )
    result = cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "public",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )
    assert result.exit_code == 1
    assert "_test" in result.stderr


def test_copy_undeclared_ref_transforms_to_object(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """When a ref points to an undeclared model, copy should output the property
    as object (level=2) and append a restore comment row."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref              | access
    example                  |        |                  |
                             |        |                  |
      |   |   | City         |        |                  |
      |   |   |   | name     | string |                  | private
      |   |   |   | country  | ref    | example2/Country | private
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "private",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref  | source | prepare                                | level | access
    example                  |         |      |        |                                        |       |
                             |         |      |        |                                        |       |
      |   |   | City         |         |      |        |                                        |       |
      |   |   |   | name     | string  |      |        |                                        |       | private
      |   |   |   | country  | object  |      |        |                                        | 2     | private
                             | comment | type |        | update(type:"ref", ref:"example2/Country") | 4     |
    """
    )


def test_copy_scope(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref    | source | prepare           | level | status  | visibility | access | uri                       | eli                       | count | title      | description
    datasets/gov/example     |        |        |        |                   |       |         |            |        |                           |                           |       |            |
                             |        |        |        |                   |       |         |            |        |                           |                           |       |            |
      |   |   | Country      |        | code   | salis  |                   |       |         |            |        |                           |                           |       |            |
                             | scope  | codes  |        | select(code)      | 3     | develop | public     | public | https://example.org/s/ltu | http://data.europa.eu/eli | 100   | Code scope | Scope for codes
      |   |   |   | code     | string |        | kodas  |                   |       |         |            | public |                           |                           |       |            |
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref    | source | prepare           | level | status  | visibility | access | uri                       | eli                       | count | title      | description
    datasets/gov/example     |        |        |        |                   |       |         |            |        |                           |                           |       |            |
                             |        |        |        |                   |       |         |            |        |                           |                           |       |            |
      |   |   | Country      |        | code   | salis  |                   |       |         |            |        |                           |                           |       |            |
                             | scope  | codes  |        | select(code)      | 3     | develop | public     | public | https://example.org/s/ltu | http://data.europa.eu/eli | 100   | Code scope | Scope for codes
      |   |   |   | code     | string |        | kodas  |                   |       |         |            | public |                           |                           |       |            |
    """
    )


def test_copy_scope_invalid_name(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref  | source | prepare             | access
    datasets/gov/example     |        |      |        |                     |
      |   |   | Country      |        | code | salis  |                     |
      |   |   |   |          | scope  | LTU  |        | code='lt'           | public
      |   |   |   | code     | string |      | kodas  |                     | public
    """),
    )

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "LTU" in result.stderr


def test_copy_scope_no_model_defined(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref  | source | prepare             | access
    datasets/gov/example     |        |      |        |                     |
                             | scope  | ltu  |        | country.code='lt'   |
    """),
    )

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "must be defined on a concrete model" in result.stderr


def test_copy_multiple_scopes(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref  | source | prepare                | access
    datasets/gov/example     |        |      |        |                        |
      |   |   | Country      |        | code | salis  |                        |
      |   |   |   |          | scope  | ltu  |        | code='lt'              | public
      |   |   |   |          | scope  | eu   |        | region='EU'            | public
      |   |   |   | code     | string |      | kodas  |                        | public
      |   |   |   | region   | string |      | region |                        | public
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type   | ref  | source | prepare                | access
    datasets/gov/example     |        |      |        |                        |
                             |        |      |        |                        |
      |   |   | Country      |        | code | salis  |                        |
                             | scope  | ltu  |        | code='lt'              | public
                             | scope  | eu   |        | region='EU'            | public
      |   |   |   | code     | string |      | kodas  |                        | public
      |   |   |   | region   | string |      | region |                        | public
    """
    )


def test_copy_undeclared_base_drops_and_comments(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """When a model declares an undeclared base, copy should output the model
    without that base and append a restore comment row."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b                              | m       | property | type   | ref | access
    example                                |         |          |        |     |
                                           |         |          |        |     |
      |   | dataset/gov/vssa/is/ds/Address |         |          |        |     |
      |   |                                | Country |          |        |     |
      |   |                                |         | name     | string |     | private
    """),
    )
    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "--access",
            "private",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )
    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | model    | property | type    | ref    | source | prepare                                        | level | access
    example                  |         |        |        |                                                |       |
                             |         |        |        |                                                |       |
      |   |   | Country      |         |        |        |                                                |       |
                             | comment | base   |        | insert(base:"dataset/gov/vssa/is/ds/Address")  |       |
      |   |   |   | name     | string  |        |        |                                                |       | private
    """
    )


def test_copy_to_simple_json(context: Context, rc, cli: SpintaCliRunner, tmp_path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref     | source      | prepare | access
    datasets/gov/example     |        |         |             |         |
      | data                 | sql    |         |             |         |
                             |        |         |             |         |
      |   |   | Country      |        | code    | salis       |         |
      |   |   |   | code     | string |         | kodas       |         | public
      |   |   |   | name     | string |         | pavadinimas |         | open
    """),
    )

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            tmp_path / "result.simple.json",
            tmp_path / "manifest.csv",
        ],
    )

    doc = json.loads((tmp_path / "result.simple.json").read_text(encoding="utf-8"))
    assert list(doc["manifest"].values())[0]["errors"] == []
    assert "datasets/gov/example/Country" in doc["model"]
    assert "datasets/gov/example/Country/name" in doc["property"]

    # The `simple` file reads back as a manifest.
    cli.invoke(
        rc,
        [
            "copy",
            "--no-source",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "result.simple.json",
        ],
    )
    text = (tmp_path / "result.csv").read_text(encoding="utf-8")
    assert "datasets/gov/example" in text
    assert "Country" in text
    assert ",name,string," in text


PARITY_TABLE = """
    d | r | b | m | property | type   | ref     | source      | prepare | access
    datasets/gov/example     |        |         |             |         |
      | data                 | sql    |         |             |         |
                             |        |         |             |         |
      |   |   | Country      |        | code    | salis       |         |
      |   |   |   | code     | string |         | kodas       |         | public
      |   |   |   | name     | string |         | pavadinimas |         | open
      |   |   |   | status   | string |         | status      |         | open
                             |        |         |             |         |
      |   |   | City         |        | name    | miestas     |         |
      |   |   |   | name     | string |         | pavadinimas |         | open
      |   |   |   | country  | ref    | Country | salis       |         | open
"""


def _parity_manifest(context, tmp_path):
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(PARITY_TABLE))
    return tmp_path / "manifest.csv"


def test_copy_csv_output_is_the_library_output(context: Context, rc, cli, tmp_path):
    # `spinta copy` and `Spinta.copy` do the same thing (D14): the file is the bytes the library returns.
    manifest = _parity_manifest(context, tmp_path)
    cli.invoke(rc, ["copy", "--access", "open", "--no-source", "-o", tmp_path / "out.csv", manifest])

    spinta = Spinta._from_context(context)
    expected = spinta.copy(spinta.copy(str(manifest)), "csv", access="open", source=False)
    assert (tmp_path / "out.csv").read_bytes() == expected


def test_copy_table_output_is_the_library_output(context: Context, rc, cli, tmp_path):
    manifest = _parity_manifest(context, tmp_path)
    result = cli.invoke(rc, ["copy", "-c", "d,r,b,m,p,type,ref", manifest])

    spinta = Spinta._from_context(context)
    expected = spinta.copy(spinta.copy(str(manifest)), "ascii", columns=["d", "r", "b", "m", "p", "type", "ref"])
    assert result.stdout == expected + "\n"


def test_copy_mermaid_output_is_the_library_output(context: Context, rc, cli, tmp_path):
    manifest = _parity_manifest(context, tmp_path)
    cli.invoke(rc, ["copy", "-o", tmp_path / "out.mmd", manifest])

    spinta = Spinta._from_context(context)
    expected = spinta.copy(spinta.copy(str(manifest)), "mermaid")
    assert (tmp_path / "out.mmd").read_text(encoding="utf-8") == expected


def test_copy_rdf_output_is_the_library_output(context: Context, rc, cli, tmp_path):
    manifest = _parity_manifest(context, tmp_path)
    cli.invoke(rc, ["copy", "--access", "open", "-o", tmp_path / "out.ttl", manifest])

    spinta = Spinta._from_context(context)
    expected = spinta.copy(spinta.copy(str(manifest)), "rdf", access="open")
    assert (tmp_path / "out.ttl").read_text(encoding="utf-8") == expected


def test_copy_simple_output_is_the_library_document(context: Context, rc, cli, tmp_path):
    manifest = _parity_manifest(context, tmp_path)
    cli.invoke(rc, ["copy", "-o", tmp_path / "out.simple.json", manifest])

    doc = json.loads((tmp_path / "out.simple.json").read_text(encoding="utf-8"))
    assert doc == Spinta._from_context(context).copy(str(manifest))


def test_copy_passes_the_parsed_options_to_the_library(context: Context, rc, cli, tmp_path, monkeypatch):
    calls = []
    run = Spinta._copy_files

    def spy(self, files, format, **options):
        calls.append((format, options))
        return run(self, files, format, **options)

    monkeypatch.setattr(Spinta, "_copy_files", spy)
    manifest = _parity_manifest(context, tmp_path)
    cli.invoke(
        rc,
        ["copy", "--access", "open", "--no-source", "--order-by", "access", "-o", tmp_path / "out.csv", manifest],
    )

    [(format, options)] = calls
    assert format == "csv"
    assert options["access"] == "open"
    assert options["source"] is False
    assert options["order_by"] == "access"
    assert options["format_names"] is False


def test_copy_with_output_names_the_manifest_first(context: Context, rc, cli, tmp_path):
    # With `-o` the CLI says which manifest it loads, as it always did. Without `-o` it does not.
    manifest = _parity_manifest(context, tmp_path)

    result = cli.invoke(rc, ["copy", "-o", tmp_path / "out.csv", manifest])
    assert result.stdout.startswith("Loading ")

    result = cli.invoke(rc, ["copy", manifest])
    assert not result.stdout.startswith("Loading ")


def test_copy_invalid_name_stops_before_printing(context: Context, rc, cli, tmp_path):
    # The first error that stops the copy is raised, and no table or error is printed before it.
    (tmp_path / "manifest.csv").write_text(
        "dataset,resource,base,model,property,type,ref,source,prepare,access\n"
        "datasets/gov/bad,,,,,,,,,\n"
        ",,,Bad Model!,,,,,,\n"
        ",,,,name,string,,,,\n",
        encoding="utf-8",
    )
    result = cli.invoke(
        rc,
        ["copy", "-o", tmp_path / "result.csv", tmp_path / "manifest.csv"],
        fail=False,
    )
    assert result.exit_code == 1
    assert "Bad Model" in result.stderr
    assert "Bad Model" not in result.stdout


@mark.parametrize(
    "options, message",
    [
        (["-c", "d,r"], "`columns` does not apply to `csv` output"),
        (["-d", "datasets/gov/example"], "`dataset` does not apply to `csv` output"),
    ],
)
def test_copy_refuses_an_option_the_output_does_not_take(context: Context, rc, cli, tmp_path, options, message):
    # The option is refused before anything is loaded or written, not ignored (3.1).
    manifest = _parity_manifest(context, tmp_path)

    result = cli.invoke(rc, ["copy", *options, "-o", tmp_path / "out.csv", manifest], fail=False)

    assert isinstance(result.exception, ValueError)
    assert message in str(result.exception)
    assert not (tmp_path / "out.csv").exists()
    assert not result.stdout.startswith("Loading ")


def test_copy_refuses_no_source_for_mermaid(context: Context, rc, cli, tmp_path):
    manifest = _parity_manifest(context, tmp_path)

    result = cli.invoke(rc, ["copy", "--no-source", "-o", tmp_path / "out.mmd", manifest], fail=False)

    assert isinstance(result.exception, ValueError)
    assert "`source` does not apply to `mermaid` output" in str(result.exception)
    assert not (tmp_path / "out.mmd").exists()


_UNRESOLVED_REF_TABLE = striptable("""
    d | r | b | m | property | type   | ref  | source | prepare | access
    datasets/gov/example     |        |      |        |         |
      | data                 | sql    |      |        |         |
                             |        |      |        |         |
      |   |   | City         |        | name |        |         |
      |   |   |   | name     | string |      |        |         | open
      |   |   |   | country  | ref    | Nope |        |         | open
""")


def test_copy_warning_is_logged_not_counted_as_an_error(context: Context, rc, cli, tmp_path):
    # A reference to a missing model is a warning. It was logged and never counted, so the table is still
    # printed, and the exit status is 0 (3.5, rule 4).
    create_tabular_manifest(context, tmp_path / "manifest.csv", _UNRESOLVED_REF_TABLE)

    result = cli.invoke(rc, ["copy", tmp_path / "manifest.csv"])
    assert "Nope" in result.stdout
    assert "Total errors" not in result.stdout + result.stderr

    result = cli.invoke(rc, ["copy", "-o", tmp_path / "out.csv", tmp_path / "manifest.csv"])
    assert "Total errors" not in result.stdout + result.stderr
    assert (tmp_path / "out.csv").exists()


def test_copy_to_simple_json_exits_1_on_a_row_that_cannot_be_read(context: Context, rc, cli, tmp_path):
    # The row is stored in the document, the file is written, and the run exits 1 as a table output does (3.5).
    (tmp_path / "manifest.csv").write_text(
        "dataset,resource,base,model,property,type,ref,source,prepare,access\n"
        "datasets/gov/example,,,,,,,,,\n"
        ",data,sql,,,,,,,\n",
        encoding="utf-8",
    )

    result = cli.invoke(rc, ["copy", "-o", tmp_path / "out.simple.json", tmp_path / "manifest.csv"], fail=False)

    assert result.exit_code == 1
    assert "only single dimension can be used" in result.stderr
    assert "Total errors: 1" in result.stderr
    assert (tmp_path / "out.simple.json").exists()


_BASE_LOCN = striptable("""
    d    | r | b | m       | property | type    | ref | uri
    locn |   |   |         |          |         |     |
         |   |   | Country |          |         | id  | locn:Country
         |   |   |         | id       | integer |     |
         |   |   |         | code     | string  |     |
""")

_BASE_PLACES = striptable("""
    d      | r | b | m     | property | type    | ref | uri
    places |   |   |       |          |         |     |
           |   |   | Place |          |         | id  |
           |   |   |       | id       | integer |     |
""")

_BASE_AR = striptable("""
    d                            | r | b | m       | property | type    | ref | uri
    datasets/gov/rc/ar/uapi/addr |   |   |         |          |         |     |
         |   |   | Country |          |         | id  |
         |   |   |         | id       | integer |     |
         |   |   |         | code     | string  |     |
""")


def _base_manifests(context, tmp_path, ar: str = _BASE_AR) -> tuple[Path, Path, Path]:
    create_tabular_manifest(context, tmp_path / "locn.csv", _BASE_LOCN)
    create_tabular_manifest(context, tmp_path / "places.csv", _BASE_PLACES)
    create_tabular_manifest(context, tmp_path / "ar.csv", ar)
    return tmp_path / "ar.csv", tmp_path / "locn.csv", tmp_path / "places.csv"


def test_copy_base_writes_the_base_of_each_model(context: Context, rc, cli, tmp_path):
    ar, locn, _ = _base_manifests(context, tmp_path)

    result = cli.invoke(rc, ["copy", "-b", locn, "-o", tmp_path / "out.csv", ar], fail=False)

    assert result.exit_code == 0
    assert "Total errors" not in result.stdout + result.stderr
    assert "/locn/Country" in (tmp_path / "out.csv").read_text(encoding="utf-8")
    # The CLI writes what the library returns for the same arguments (D14).
    spinta = Spinta._from_context(context)
    expected = spinta.copy(spinta.copy(str(ar)), "csv", base=[str(locn)])
    assert (tmp_path / "out.csv").read_bytes() == expected


def test_copy_base_can_be_given_more_than_once(context: Context, rc, cli, tmp_path):
    ar, locn, places = _base_manifests(context, tmp_path)

    result = cli.invoke(rc, ["copy", "-b", places, "-b", locn, "-o", tmp_path / "out.csv", ar], fail=False)

    assert result.exit_code == 0
    assert "/locn/Country" in (tmp_path / "out.csv").read_text(encoding="utf-8")


def test_copy_base_prints_the_table_and_the_error_when_a_model_has_no_match(context: Context, rc, cli, tmp_path):
    # The error is printed on stderr and counted. The table is still written, and the exit status stays 0.
    ar, locn, _ = _base_manifests(context, tmp_path, ar=_BASE_AR + "\n         |   |   | Standalone |  |  |  |")

    result = cli.invoke(rc, ["copy", "-b", locn, ar], fail=False)

    assert result.exit_code == 0
    assert "Standalone" in result.stdout
    assert "has no match in the default bases" in result.stderr
    assert "Total errors: 1" in result.stderr


def test_copy_base_writes_the_output_when_a_model_has_no_match(context: Context, rc, cli, tmp_path):
    ar, locn, _ = _base_manifests(context, tmp_path, ar=_BASE_AR + "\n         |   |   | Standalone |  |  |  |")

    result = cli.invoke(rc, ["copy", "-b", locn, "-o", tmp_path / "out.simple.json", ar], fail=False)

    assert result.exit_code == 0
    assert "has no match in the default bases" in result.stderr
    doc = json.loads((tmp_path / "out.simple.json").read_text(encoding="utf-8"))
    standalone = doc["model"]["datasets/gov/rc/ar/uapi/addr/Standalone"]
    assert [error["code"] for error in standalone["errors"]] == ["DefaultBaseNotFound"]


def test_copy_base_visibility_limits_the_errors(context: Context, rc, cli, tmp_path):
    _, locn, _ = _base_manifests(context, tmp_path)
    create_tabular_manifest(
        context,
        tmp_path / "ar.csv",
        striptable("""
    d                            | r | b | m       | property | type    | ref | uri | visibility
    datasets/gov/rc/ar/uapi/addr |   |   |         |          |         |     |     |
         |   |   | Country |          |         | id  |     |
         |   |   |         | id       | integer |     |     |
         |   |   | Hidden  |          |         |     |     | private
    """),
    )
    ar = tmp_path / "ar.csv"

    result = cli.invoke(rc, ["copy", "-b", locn, "--base-visibility", "public,package", ar], fail=False)
    assert result.exit_code == 0
    assert "Total errors" not in result.stdout + result.stderr

    result = cli.invoke(rc, ["copy", "-b", locn, ar], fail=False)
    assert "Total errors: 1" in result.stderr


def test_copy_base_visibility_needs_base(context: Context, rc, cli, tmp_path):
    ar, _, _ = _base_manifests(context, tmp_path)

    result = cli.invoke(rc, ["copy", "--base-visibility", "public", "-o", tmp_path / "out.csv", ar], fail=False)

    assert isinstance(result.exception, ValueError)
    assert "needs `base`" in str(result.exception)
    assert not (tmp_path / "out.csv").exists()


def test_copy_base_refuses_an_input_it_does_not_read_yet(context: Context, rc, cli, tmp_path):
    _, locn, _ = _base_manifests(context, tmp_path)
    (tmp_path / "model.xsd").write_text("<xs:schema/>", encoding="utf-8")

    result = cli.invoke(rc, ["copy", "-b", locn, "-o", tmp_path / "out.csv", tmp_path / "model.xsd"], fail=False)

    assert isinstance(result.exception, NotImplementedError)
    assert not (tmp_path / "out.csv").exists()


def test_copy_base_options_reach_the_library(context: Context, rc, cli, tmp_path, monkeypatch):
    calls = []
    run = Spinta._copy_files

    def spy(self, files, format, **options):
        calls.append(options)
        return run(self, files, format, **options)

    monkeypatch.setattr(Spinta, "_copy_files", spy)
    ar, locn, places = _base_manifests(context, tmp_path)
    cli.invoke(
        rc,
        ["copy", "-b", str(locn), "-b", str(places), "--base-visibility", "public", "-o", tmp_path / "out.csv", ar],
    )

    [options] = calls
    assert options["base"] == [str(locn), str(places)]
    assert options["base_visibility"] == ["public"]


def _inherit_example(tmp_path):
    """The example of the inheritance: `dsa.txt` is based on the `locn` manifest of `base.txt` (ASCII)."""
    from tests.manifests.simple.test_inherit import BASE_TXT, DSA_TXT

    dsa = tmp_path / "dsa.txt"
    dsa.write_text(DSA_TXT, encoding="utf-8")
    base = tmp_path / "base.txt"
    base.write_text(BASE_TXT, encoding="utf-8")
    return dsa, base


def test_copy_inherits_the_values_of_the_base_to_csv(context: Context, rc, cli, tmp_path):
    dsa, base = _inherit_example(tmp_path)

    result = cli.invoke(rc, ["copy", str(dsa), str(base), "-o", tmp_path / "out.csv"], fail=False)

    assert result.exit_code == 0
    # The CLI writes what the library returns for the same arguments (D14).
    spinta = Spinta._from_context(context)
    expected = spinta.copy([spinta.copy(str(dsa)), spinta.copy(str(base))], "csv")
    assert (tmp_path / "out.csv").read_bytes() == expected
    dataset = None
    rows = []
    for row in csv.DictReader(io.StringIO((tmp_path / "out.csv").read_text(encoding="utf-8"))):
        dataset = row["dataset"] or dataset
        rows.append((dataset, row))
    addr = [row for name, row in rows if name == "datasets/gov/rc/ar/uapi/addr"]
    assert {row["ref"]: row["uri"] for row in addr if row["type"] == "prefix"}["locn"] == "http://www.w3.org/ns/locn#"
    [country] = [row for row in addr if row["model"] == "Country"]
    assert (country["uri"], country["title"]) == ("locn:Country", "Country")
    [identifier] = [row for row in addr if row["property"] == "id"]
    assert (identifier["uri"], identifier["title"]) == ("dct:identifier", "Identifier")


def test_copy_inherits_the_values_of_the_base_to_simple_json(context: Context, rc, cli, tmp_path):
    dsa, base = _inherit_example(tmp_path)

    result = cli.invoke(rc, ["copy", str(dsa), str(base), "-o", tmp_path / "out.simple.json"], fail=False)

    assert result.exit_code == 0
    doc = json.loads((tmp_path / "out.simple.json").read_text(encoding="utf-8"))
    country = doc["model"]["datasets/gov/rc/ar/uapi/addr/Country"]
    # `given` is as written, and `implied` has the inherited values.
    assert "title" not in country["given"]
    assert country["implied"]["title"] == "Country"
    assert country["implied"]["uri"] == "http://www.w3.org/ns/locn#Country"


def test_copy_inherits_the_values_of_the_base_to_turtle(context: Context, rc, cli, tmp_path):
    dsa, base = _inherit_example(tmp_path)

    result = cli.invoke(rc, ["copy", str(dsa), str(base), "-o", tmp_path / "out.ttl"], fail=False)

    assert result.exit_code == 0
    ttl = (tmp_path / "out.ttl").read_text(encoding="utf-8")
    assert 'sh:name "Identifier" ;\n        sh:path dct:identifier ;' in ttl
    assert 'sh:name "Country" ;\n    sh:targetClass locn:Country ;' in ttl


def test_read_for_sync_keeps_the_values_as_written(context: Context, rc, tmp_path):
    from spinta import commands
    from spinta.cli.manifest import _read_and_return_manifest
    from spinta.testing.context import create_test_context

    dsa, base = _inherit_example(tmp_path)
    own = create_test_context(rc)
    manifest = _read_and_return_manifest(own, [str(dsa), str(base)])

    country = commands.get_model(own, manifest, "datasets/gov/rc/ar/uapi/addr/Country")
    assert country.title == ""
    assert country.uri == ""


def test_copy_inherits_the_values_of_the_base_to_internal_sql(context: Context, rc, tmp_path):
    import sqlalchemy as sa

    from spinta.cli.manifest import copy_manifest
    from spinta.core.context import configure_context
    from spinta.testing.context import create_test_context

    dsa, base = _inherit_example(tmp_path)
    own = create_test_context(rc)
    own = configure_context(own, [str(dsa), str(base)], check_names=True)
    output = f"sqlite:///{tmp_path / 'manifest.db'}"

    # The internal SQL output has no CLI option, so it is written by `copy_manifest` as `copy` does for it.
    copy_manifest(own, manifests=[str(dsa), str(base)], output=output, output_type="internal_sql")

    engine = sa.create_engine(output)
    with engine.connect() as conn:
        rows = conn.execute(sa.text("select path, title, uri from _manifest where dim = 'model'")).fetchall()
    models = {row.path: (row.title, row.uri) for row in rows}
    assert models["datasets/gov/rc/ar/uapi/addr/Country"] == ("Country", "locn:Country")
