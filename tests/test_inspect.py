from pathlib import Path

import sqlalchemy as sa

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.config import configure
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import compare_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest


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

    cli.invoke(rc, ['inspect', sqlite.dsn, '-o', tmpdir / 'result.csv'])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'result.csv')
    manifest.datasets['dbsqlite'].resources['resource1'].external = 'sqlite'
    assert manifest == '''
    d | r | b | m | property   | type    | ref     | source     | prepare
    dbsqlite                   |         |         |            |
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
    '''


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

    create_tabular_manifest(tmpdir / 'manifest.csv', '''
    d | r | m | property     | type   | ref | source | access
    dataset                  |        |     |        |
      | rs                   | sql    |     |        |
    ''')

    cli.invoke(rc, [
        'inspect', tmpdir / 'manifest.csv',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'result.csv',
    ])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'result.csv')
    manifest.datasets['dataset'].resources['rs'].external = 'sqlite'
    assert manifest == '''
    d | r | b | m | property | type    | ref | source  | prepare
    dataset                  |         |     |         |
      | rs                   | sql     |     | sqlite  |
                             |         |     |         |
      |   |   | Country      |         | id  | COUNTRY |
      |   |   |   | id       | integer |     | ID      |
      |   |   |   | name     | string  |     | NAME    |
    '''


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

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
    ])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'manifest.csv')
    dataset = manifest.datasets['datasets/gov/example']
    dataset.resources['resource1'].external = 'sqlite'
    assert manifest == '''
    d | r | b | m | property   | type    | ref     | source     | prepare
    datasets/gov/example       |         |         |            |
      | resource1              | sql     |         | sqlite     |
                               |         |         |            |
      |   |   | Country        |         | id      | COUNTRY    |
      |   |   |   | id         | integer |         | ID         |
      |   |   |   | code       | string  |         | CODE       |
      |   |   |   | name       | string  |         | NAME       |
                               |         |         |            |
      |   |   | City           |         |         | CITY       |
      |   |   |   | name       | string  |         | NAME       |
      |   |   |   | country_id | ref     | Country | COUNTRY_ID |
    '''


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

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
              ])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'manifest.csv')
    dataset = manifest.datasets['datasets/gov/example']
    dataset.resources['resource1'].external = 'sqlite'
    assert manifest == '''
    d | r | b | m | property   | type    | ref     | source     | prepare
    datasets/gov/example       |         |         |            |
      | resource1              | sql     |         | sqlite     |
                               |         |         |            |
      |   |   | City           |         | id      | CITY       |
      |   |   |   | id         | integer |         | ID         |
      |   |   |   | name       | string  |         | NAME       |
      |   |   |   | country_id | ref     | Country | COUNTRY_ID |
                               |         |         |            |
      |   |   | Country        |         | id      | COUNTRY    |
      |   |   |   | id         | integer |         | ID         |
      |   |   |   | capital    | ref     | City    | CAPITAL    |
      |   |   |   | name       | string  |         | NAME       |
    '''


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

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
              ])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'manifest.csv')
    dataset = manifest.datasets['datasets/gov/example']
    dataset.resources['resource1'].external = 'sqlite'
    assert manifest == '''
    d | r | b | m | property  | type    | ref      | source    | prepare
    datasets/gov/example      |         |          |           |
      | resource1             | sql     |          | sqlite    |
                              |         |          |           |
      |   |   | Category      |         | id       | CATEGORY  |
      |   |   |   | id        | integer |          | ID        |
      |   |   |   | name      | string  |          | NAME      |
      |   |   |   | parent_id | ref     | Category | PARENT_ID |
    '''


def test_inspect_oracle_sqldump_stdin(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
):
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
    manifest = load_manifest(rc, tmpdir / 'manifest.csv')
    assert manifest == '''
    id | d | r | b | m | property | type    | ref | source  | prepare | level | access | uri | title | description
       | datasets/gov/example     |         |     |         |         |       |        |     |       |
       |   | resource1            | sqldump |     | -       |         |       |        |     |       |
       |                          |         |     |         |         |       |        |     |       |
       |   |   |   | Country      |         |     | COUNTRY |         |       |        |     |       |
       |                          |         |     |         |         |       |        |     |       |
       |   |   |   | City         |         |     | CITY    |         |       |        |     |       |
    '''


def test_inspect_oracle_sqldump_file_with_formula(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmpdir: Path,
):
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
    manifest = load_manifest(rc, tmpdir / 'manifest.csv')
    dataset = manifest.datasets['datasets/gov/example']
    dataset.resources['resource1'].external = 'dump.sql'
    assert manifest == '''
    d | r | b | m | property | type    | ref | source   | prepare
    datasets/gov/example     |         |     |          |
      | resource1            | sqldump |     | dump.sql | file(self, encoding: 'iso-8859-4')
                             |         |     |          |
      |   |   | Country      |         |     | COUNTRY  |
    '''


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
    manifest = load_manifest(rc, tmpdir / 'result.csv')
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


def test_inspect_update_existing_manifest(
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
        'CITY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Integer, sa.ForeignKey("COUNTRY.ID")),
        ],
    })

    # Configure Spinta.
    rc = configure(rc, sqlite, tmpdir / 'manifest.csv', f'''
    d | r | m | property | type    | ref | source | prepare | access  | title
    datasets/gov/example |         |     |        |         |         | Example
      | schema           | sql     | sql |        |         |         | 
                         |         |     |        |         |         |
      |   | City         |         | id  | CITY   | id > 1  |         | City
      |   |   | id       | integer |     | ID     |         | private | 
      |   |   | name     | string  |     | NAME   | strip() | open    | City name
    ''')

    cli.invoke(rc, [
        'inspect',
        tmpdir / 'manifest.csv',
        '-o', tmpdir / 'result.csv',
    ])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'result.csv')
    a, b = compare_manifest(manifest, '''
    d | r | b | m | property | type    | ref     | source  | prepare | access  | title
    datasets/gov/example     |         |         |         |         |         | Example
      | schema               | sql     | sql     |         |         |         |
                             |         |         |         |         |         |
      |   |   | Country      |         | id      | COUNTRY |         |         |
      |   |   |   | id       | integer |         | ID      |         |         |
      |   |   |   | name     | string  |         | NAME    |         |         |
                             |         |         |         |         |         |
      |   |   | City         |         | id      | CITY    | id>1    |         | City
      |   |   |   | id       | integer |         | ID      |         | private |
      |   |   |   | name     | string  |         | NAME    | strip() | open    | City name
      |   |   |   | country  | ref     | Country | COUNTRY |         |         |
    ''')
    assert a == b


def test_inspect_update_existing_ref(
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
        'CITY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('COUNTRY', sa.Integer, sa.ForeignKey("COUNTRY.ID")),
        ],
    })

    # Configure Spinta.
    rc = configure(rc, sqlite, tmpdir / 'manifest.csv', f'''
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
    ''')

    cli.invoke(rc, [
        'inspect',
        tmpdir / 'manifest.csv',
        '-o', tmpdir / 'result.csv',
        ])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'result.csv')
    a, b = compare_manifest(manifest, '''
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
    ''')
    assert a == b


def test_inspect_with_empty_config_dir(
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

    # Change config dir
    (tmpdir / 'config').mkdir()
    rc = rc.fork({
        'config_path': tmpdir / 'config',
    })

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'result.csv',
    ])

    # Check what was detected.
    manifest = load_manifest(rc, tmpdir / 'result.csv')
    dataset = manifest.datasets['datasets/gov/example']
    dataset.resources['resource1'].external = 'sqlite://'
    assert manifest == '''
    d | r | b | m | property | type    | ref | source
    datasets/gov/example     |         |     |
      | resource1            | sql     |     | sqlite://
                             |         |     |
      |   |   | Country      |         | id  | COUNTRY
      |   |   |   | id       | integer |     | ID
      |   |   |   | name     | string  |     | NAME
    '''


def test_inspect_duplicate_table_names(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        '__COUNTRY': [sa.Column('NAME', sa.Text)],
        '_COUNTRY': [sa.Column('NAME', sa.Text)],
        'COUNTRY': [sa.Column('NAME', sa.Text)],
    })

    result_file_path = tmp_path / 'result.csv'
    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', result_file_path,
    ])

    # Check what was detected.
    manifest = load_manifest(rc, result_file_path)
    dataset = manifest.datasets['datasets/gov/example']
    dataset.resources['resource1'].external = 'sqlite://'
    assert manifest == '''
    d | r | b | m | property | type    | ref | source
    datasets/gov/example     |         |     |
      | resource1            | sql     |     | sqlite://
                             |         |     |
      |   |   | Country      |         |     | COUNTRY
      |   |   |   | name     | string  |     | NAME
                             |         |     |
      |   |   | Country1     |         |     | _COUNTRY
      |   |   |   | name     | string  |     | NAME
                             |         |     |
      |   |   | Country2     |         |     | __COUNTRY
      |   |   |   | name     | string  |     | NAME
    '''


def test_inspect_duplicate_column_names(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('__NAME', sa.Text),
            sa.Column('_NAME', sa.Text),
            sa.Column('NAME', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', result_file_path,
    ])

    # Check what was detected.
    manifest = load_manifest(rc, result_file_path)
    dataset = manifest.datasets['datasets/gov/example']
    dataset.resources['resource1'].external = 'sqlite://'
    assert manifest == '''
    d | r | b | m | property | type    | ref | source
    datasets/gov/example     |         |     |
      | resource1            | sql     |     | sqlite://
                             |         |     |
      |   |   | Country      |         |     | COUNTRY
      |   |   |   | name     | string  |     | __NAME
      |   |   |   | name_1   | string  |     | _NAME
      |   |   |   | name_2   | string  |     | NAME
    '''
