from pathlib import Path

import sqlalchemy as sa

from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.config import configure
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import compare_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import load_tabular_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.testing.workarounds import fix_s3_backend_issue


def test_inspect(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('CODE', sa.Text),
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY_ID', sa.Integer, sa.ForeignKey("COUNTRY.ID")),
        ],
    })

    # Configure Spinta.
    rc = configure(rc, None, tmpdir / 'manifest.csv', f'''
    d | r | m | property     | type   | ref | source       | access
    dataset                  |        |     |              |
      | rs                   | sql    |     | {sqlite.dsn} |
    ''')

    cli.invoke(rc, ['inspect', '-o', tmpdir / 'result.csv'])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
    manifest.datasets['dataset'].resources['rs'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property   | type    | ref     | source     | prepare | level | access    | uri | title | description
       | dataset                    |         |         |            |         |       | protected |     |       |
       |   | rs                     | sql     |         | sqlite     |         |       | protected |     |       |
       |                            |         |         |            |         |       |           |     |       |
       |   |   |   | Country        |         | id      | COUNTRY    |         |       | protected |     |       |
       |   |   |   |   | id         | integer |         | ID         |         |       | protected |     |       |
       |   |   |   |   | code       | string  |         | CODE       |         |       | protected |     |       |
       |   |   |   |   | name       | string  |         | NAME       |         |       | protected |     |       |
       |                            |         |         |            |         |       |           |     |       |
       |   |   |   | City           |         |         | CITY       |         |       | protected |     |       |
       |   |   |   |   | name       | string  |         | NAME       |         |       | protected |     |       |
       |   |   |   |   | country_id | ref     | Country | COUNTRY_ID |         |       | protected |     |       |
    ''')


def test_inspect_from_manifest_table(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
        ],
    })

    create_tabular_manifest(tmpdir / 'manifest.csv', f'''
    d | r | m | property     | type   | ref | source       | access
    dataset                  |        |     |              |
      | rs                   | sql    |     | {sqlite.dsn} |
    ''')

    cli.invoke(rc, [
        'inspect', tmpdir / 'manifest.csv',
        '-o', tmpdir / 'result.csv',
    ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
    manifest.datasets['dataset'].resources['rs'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property | type    | ref | source  | prepare | level | access    | uri | title | description
       | dataset                  |         |     |         |         |       | protected |     |       |
       |   | rs                   | sql     |     | sqlite  |         |       | protected |     |       |
       |                          |         |     |         |         |       |           |     |       |
       |   |   |   | Country      |         | id  | COUNTRY |         |       | protected |     |       |
       |   |   |   |   | id       | integer |     | ID      |         |       | protected |     |       |
       |   |   |   |   | name     | string  |     | NAME    |         |       | protected |     |       |
    ''')


def test_inspect_format(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('CODE', sa.Text),
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY_ID', sa.Integer, sa.ForeignKey("COUNTRY.ID")),
        ],
    })

    rc = fix_s3_backend_issue(rc)

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
    ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['sql'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property   | type    | ref     | source     | prepare | level | access    | uri | title | description
       | dataset                    |         |         |            |         |       | protected |     |       |
       |   | sql                    | sql     |         | sqlite     |         |       | protected |     |       |
       |                            |         |         |            |         |       |           |     |       |
       |   |   |   | Country        |         | id      | COUNTRY    |         |       | protected |     |       |
       |   |   |   |   | id         | integer |         | ID         |         |       | protected |     |       |
       |   |   |   |   | code       | string  |         | CODE       |         |       | protected |     |       |
       |   |   |   |   | name       | string  |         | NAME       |         |       | protected |     |       |
       |                            |         |         |            |         |       |           |     |       |
       |   |   |   | City           |         |         | CITY       |         |       | protected |     |       |
       |   |   |   |   | name       | string  |         | NAME       |         |       | protected |     |       |
       |   |   |   |   | country_id | ref     | Country | COUNTRY_ID |         |       | protected |     |       |
    ''')


def test_inspect_cyclic_refs(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('CAPITAL', sa.Integer, sa.ForeignKey("CITY.ID")),
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY_ID', sa.Integer, sa.ForeignKey("COUNTRY.ID")),
        ],
    })

    rc = fix_s3_backend_issue(rc)

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
              ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['sql'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property   | type    | ref     | source     | prepare | level | access    | uri | title | description
       | dataset                    |         |         |            |         |       | protected |     |       |
       |   | sql                    | sql     |         | sqlite     |         |       | protected |     |       |
       |                            |         |         |            |         |       |           |     |       |
       |   |   |   | City           |         | id      | CITY       |         |       | protected |     |       |
       |   |   |   |   | id         | integer |         | ID         |         |       | protected |     |       |
       |   |   |   |   | name       | string  |         | NAME       |         |       | protected |     |       |
       |   |   |   |   | country_id | ref     | Country | COUNTRY_ID |         |       | protected |     |       |
       |                            |         |         |            |         |       |           |     |       |
       |   |   |   | Country        |         | id      | COUNTRY    |         |       | protected |     |       |
       |   |   |   |   | id         | integer |         | ID         |         |       | protected |     |       |
       |   |   |   |   | capital    | ref     | City    | CAPITAL    |         |       | protected |     |       |
       |   |   |   |   | name       | string  |         | NAME       |         |       | protected |     |       |
    ''')


def test_inspect_self_refs(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'CATEGORY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('PARENT_ID', sa.Integer, sa.ForeignKey("CATEGORY.ID")),
        ],
    })

    rc = fix_s3_backend_issue(rc)

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
              ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['sql'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property  | type    | ref      | source    | prepare | level | access    | uri | title | description
       | dataset                   |         |          |           |         |       | protected |     |       |
       |   | sql                   | sql     |          | sqlite    |         |       | protected |     |       |
       |                           |         |          |           |         |       |           |     |       |
       |   |   |   | Category      |         | id       | CATEGORY  |         |       | protected |     |       |
       |   |   |   |   | id        | integer |          | ID        |         |       | protected |     |       |
       |   |   |   |   | name      | string  |          | NAME      |         |       | protected |     |       |
       |   |   |   |   | parent_id | ref     | Category | PARENT_ID |         |       | protected |     |       |
    ''')


def test_inspect_oracle_sqldump_stdin(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
):
    rc = fix_s3_backend_issue(rc)
    cli.invoke(rc, [
        'inspect',
        '-r', 'sqldump', '-',
        '-o', tmpdir / 'manifest.csv',
    ], input='''
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
    
    ''')

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property | type    | ref | source  | prepare | level | access    | uri | title | description
       | dataset                  |         |     |         |         |       | protected |     |       |
       |   | sqldump              | sqldump |     | -       |         |       | protected |     |       |
       |                          |         |     |         |         |       |           |     |       |
       |   |   |   | Country      |         |     | COUNTRY |         |       | protected |     |       |
       |                          |         |     |         |         |       |           |     |       |
       |   |   |   | City         |         |     | CITY    |         |       | protected |     |       |
    ''')


def test_inspect_oracle_sqldump_file_with_formula(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
):
    rc = fix_s3_backend_issue(rc)
    (tmpdir / 'dump.sql').write_text('''
    -- Šalys
    CREATE TABLE "GEO"."COUNTRY" (
      "ID" NUMBER(19,0), 
      "NAME" VARCHAR2(255 CHAR)
    );
    ''', encoding='iso-8859-4')
    cli.invoke(rc, [
        'inspect',
        '-r', 'sqldump', tmpdir / 'dump.sql',
        '-f', 'file(self, encoding: "iso-8859-4")',
        '-o', tmpdir / 'manifest.csv',
    ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['sqldump'].external = 'dump.sql'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property | type    | ref | source   | prepare                            | level | access    | uri | title | description
       | dataset                  |         |     |          |                                    |       | protected |     |       |
       |   | sqldump              | sqldump |     | dump.sql | file(self, encoding: 'iso-8859-4') |       | protected |     |       |
       |                          |         |     |          |                                    |       |           |     |       |
       |   |   |   | Country      |         |     | COUNTRY  |                                    |       | protected |     |       |
    ''')


def test_inspect_with_schema(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'CITY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
        ],
    })

    # Configure Spinta.
    rc = configure(rc, None, tmpdir / 'manifest.csv', f'''
    d | r | m | property | type | source       | prepare
    dataset              |      |              |
      | schema           | sql  | {sqlite.dsn} | connect(self, schema: null)
    ''')

    cli.invoke(rc, ['inspect', '-o', tmpdir / 'result.csv'])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'result.csv')
    manifest.datasets['dataset'].resources['schema'].external = 'sqlite'
    a, b = compare_manifest(manifest, '''
    d | r | b | m | property | type    | ref | source | prepare
    dataset                  |         |     |        |
      | schema               | sql     |     | sqlite | connect(self, schema: null)
                             |         |     |        |
      |   |   | City         |         | id  | CITY   |
      |   |   |   | id       | integer |     | ID     |
      |   |   |   | name     | string  |     | NAME   |
    ''')
    assert a == b
