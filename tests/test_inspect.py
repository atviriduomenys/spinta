import json
import os
import pathlib
import tempfile
from pathlib import Path

import sqlalchemy as sa

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.datasets.inspect.helpers import PriorityKey
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.config import configure
from spinta.testing.context import create_test_context
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import compare_manifest, load_manifest_and_context
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest


@pytest.fixture()
def sqlite_new():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Sqlite('sqlite:///' + os.path.join(tmpdir, 'new.sqlite'))


@pytest.fixture()
def rc_new(rc, tmp_path: pathlib.Path):
    # Need to have a clean slate, ignoring testing context manifests
    path = f'{tmp_path}/manifest.csv'
    context = create_test_context(rc)
    create_tabular_manifest(context, path, striptable('''
     d | r | b | m | property   | type    | ref     | source     | prepare
    '''))
    return rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(path),
                'backend': 'default',
                'keymap': 'default',
                'mode': 'external',
            },
        },
        'backends': {
            'default': {
                'type': 'memory',
            },
        },
    })


def test_inspect(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
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

    cli.invoke(rc_new, ['inspect', sqlite.dsn, '-o', tmp_path / 'result.csv'])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'result.csv')
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    assert manifest == f'''
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
    '''


def test_inspect_from_manifest_table(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
        ],
    })
    context = create_test_context(rc_new)
    create_tabular_manifest(context, tmp_path / 'manifest.csv', f'''
    d | r | m | property     | type   | ref | source | access
    db_sqlite                |        |     |        |
      | resource1            | sql    |   | {sqlite.dsn} |
    ''')
    cli.invoke(rc_new, [
        'inspect', tmp_path / 'manifest.csv',
        '-r', 'sql', sqlite.dsn,
        '-o', tmp_path / 'result.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'result.csv')
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    assert manifest == f'''
    d | r | b | m | property  | type    | ref | source  | prepare
    db_sqlite                 |         |     |         |
      | resource1             | sql     |     | sqlite  |
                              |         |     |         |
      |   |   | Country       |         | id  | COUNTRY |
      |   |   |   | id        | integer |     | ID      |
      |   |   |   | name      | string  |     | NAME    |
    '''


def test_inspect_format(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
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
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmp_path / 'manifest.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'manifest.csv')
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
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
    ''', context)
    assert a == b


def test_inspect_cyclic_refs(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
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

    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmp_path / 'manifest.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'manifest.csv')
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    assert manifest == f'''
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
    '''


def test_inspect_self_refs(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
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
    rc_new = rc_new.fork({
        "manifests": {
            "default": {

            }
        }
    })
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmp_path / 'manifest.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'manifest.csv')
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    assert manifest == f'''
    d | r | b | m | property  | type    | ref      | source    | prepare
    db_sqlite                 |         |          |           |
      | resource1             | sql     |          | sqlite    |
                              |         |          |           |
      |   |   | Category      |         | id       | CATEGORY  |
      |   |   |   | id        | integer |          | ID        |
      |   |   |   | name      | string  |          | NAME      |
      |   |   |   | parent_id | ref     | Category | PARENT_ID |
    '''


@pytest.mark.skip(reason="sqldump not fully implemented")
def test_inspect_oracle_sqldump_stdin(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sqldump', '-',
        '-o', tmp_path / 'manifest.csv',
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
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'manifest.csv')
    assert manifest == '''
    id | d | r | b | m | property | type    | ref | source  | prepare | level | access | uri | title | description
       | datasets/gov/example     |         |     |         |         |       |        |     |       |
       |   | resource1            | sqldump |     | -       |         |       |        |     |       |
       |                          |         |     |         |         |       |        |     |       |
       |   |   |   | Country      |         |     | COUNTRY |         |       |        |     |       |
       |                          |         |     |         |         |       |        |     |       |
       |   |   |   | City         |         |     | CITY    |         |       |        |     |       |
    '''


@pytest.mark.skip(reason="sqldump not fully implemented")
def test_inspect_oracle_sqldump_file_with_formula(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    (tmp_path / 'dump.sql').write_text('''
    -- Šalys
    CREATE TABLE "GEO"."COUNTRY" (
      "ID" NUMBER(19,0),
      "NAME" VARCHAR2(255 CHAR)
    );
    ''', encoding='iso-8859-4')
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sqldump', tmp_path / 'dump.sql',
        '-f', 'file(self, encoding: "iso-8859-4")',
        '-o', tmp_path / 'manifest.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'manifest.csv')
    dataset = commands.get_dataset(context, manifest, 'datasets/gov/example')
    dataset.resources['resource1'].external = 'dump.sql'
    assert manifest == '''
    d | r | b | m | property | type    | ref | source   | prepare
    datasets/gov/example     |         |     |          |
      | resource1            | sqldump |     | dump.sql | file(self, encoding: 'iso-8859-4')
                             |         |     |          |
      |   |   | Country      |         |     | COUNTRY  |
    '''


def test_inspect_with_schema(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'CITY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
        ],
    })

    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
    d | r | m | property | type | source       | prepare
    dataset              |      |              |
      | schema           | sql  | {sqlite.dsn} | connect(self, schema: null)
    ''')

    cli.invoke(rc_new, ['inspect', tmp_path / 'manifest.csv', '-o', tmp_path / 'result.csv'])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'result.csv')
    commands.get_dataset(context, manifest, 'dataset').resources['schema'].external = 'sqlite'
    a, b = compare_manifest(manifest, '''
    d | r | b | m | property | type    | ref | source | prepare
    dataset                  |         |     |        |
      | schema               | sql     |     | sqlite | connect(self, schema: null)
                             |         |     |        |
      |   |   | City         |         | id  | CITY   |
      |   |   |   | id       | integer |     | ID     |
      |   |   |   | name     | string  |     | NAME   |
    ''', context)
    assert a == b


def test_inspect_update_existing_manifest(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
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
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
    d | r | m | property | type    | ref | source | prepare | access  | title
    datasets/gov/example |         |     |        |         |         | Example
      | schema           | sql     | sql |        |         |         |
                         |         |     |        |         |         |
      |   | City         |         | id  | CITY   | id > 1  |         | City
      |   |   | id       | integer |     | ID     |         | private |
      |   |   | name     | string  |     | NAME   | strip() | open    | City name
    ''')

    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'result.csv')
    a, b = compare_manifest(manifest, '''
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
    ''', context)
    assert a == b


def test_inspect_update_existing_ref_manifest_priority(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
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
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
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
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'result.csv')
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
      |   |   |   | country  | integer |         | COUNTRY |         | open    | Country id
    ''', context)
    assert a == b


def test_inspect_update_existing_ref_external_priority(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
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
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
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
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-p', 'external',
        '-o', tmp_path / 'result.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'result.csv')
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
    ''', context)
    assert a == b


def test_inspect_with_empty_config_dir(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
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
    (tmp_path / 'config').mkdir()
    rc_new = rc_new.fork({
        'config_path': tmp_path / 'config',
    })

    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmp_path / 'result.csv',
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, tmp_path / 'result.csv')
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    assert manifest == f'''
    d | r | b | m | property | type    | ref | source
    db_sqlite                |         |     |
      | resource1            | sql     |     | sqlite
                             |         |     |
      |   |   | Country      |         | id  | COUNTRY
      |   |   |   | id       | integer |     | ID
      |   |   |   | name     | string  |     | NAME
    '''


def test_inspect_duplicate_table_names(
    rc_new: RawConfig,
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
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', result_file_path,
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    assert manifest == f'''
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
    '''


def test_inspect_duplicate_column_names(
    rc_new: RawConfig,
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
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', result_file_path,
    ])

    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    assert manifest == f'''
    d | r | b | m | property | type    | ref | source
    db_sqlite                |         |     |
      | resource1            | sql     |     | sqlite
                             |         |     |
      |   |   | Country      |         |     | COUNTRY
      |   |   |   | name_2   | string  |     | NAME
      |   |   |   | name_1   | string  |     | _NAME
      |   |   |   | name     | string  |     | __NAME
    '''


def test_inspect_existing_duplicate_table_names(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        '__COUNTRY': [sa.Column('NAME', sa.Text)],
        '_COUNTRY': [sa.Column('NAME', sa.Text)],
        'COUNTRY': [sa.Column('NAME', sa.Text)],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         | id  |         |         |         | Country
         |   |   | id       | integer |     |         |         | private | Primary key
         |   |   | name     | string  |     |         |         | open    | Country name
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(manifest, f'''
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
       ''', context)
    assert a == b


def test_inspect_existing_duplicate_column_names(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('__NAME', sa.Text),
            sa.Column('_NAME', sa.Text),
            sa.Column('NAME', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     |         |         | open    | Country name
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(manifest, f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     |         |         | open    | Country name
         |   |   | name_2   | string  |     | NAME    |         |         |
         |   |   | name_1   | string  |     | _NAME   |         |         |
         |   |   | name_3   | string  |     | __NAME  |         |         |
       ''', context)
    assert a == b


def test_inspect_insert_new_dataset(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
                            |         |     |         |         |         |
         |   | Country      |         |     |         |         |         | Country
         |   |   | name     | string  |     |         |         | open    | Country name
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-r', 'sql', sqlite.dsn,
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = "sqlite"
    a, b = compare_manifest(manifest, f'''
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
''', context)
    assert a == b


def test_inspect_delete_model_source(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | City         |         |     | CITY    |         |         | City
         |   |   | name     | string  |     | NAME    |         | open    | City name
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(manifest, f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | City         |         |     |         |         |         | City
         |   |   | name     | string  |     |         |         | open    | City name
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         |
         |   |   | name     | string  |     | NAME    |         |         |
''', context)
    assert a == b


def test_inspect_delete_property_source(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     | NAME    |         | open    | Country name
         |   |   | code     | string  |     | CODE    |         | open    | Country code
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    a, b = compare_manifest(manifest, f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     | sql |         |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     | NAME    |         | open    | Country name
         |   |   | code     | string  |     |         |         | open    | Country code
''', context)
    assert a == b


def test_inspect_multiple_resources_all_new(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
    sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
    })

    sqlite_new.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         | schema_1         | sql     |     | {sqlite_new.dsn} |         |         |
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema'].external = 'sqlite'
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema_1'].external = 'sqlite_new'
    a, b = compare_manifest(manifest, f'''
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

''', context)
    assert a == b


def test_inspect_multiple_resources_specific(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
    sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    sqlite_new.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Integer, sa.ForeignKey("CONTINENT.ID")),
        ],
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
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
       ''')
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite_new.dsn,
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema'].external = 'sqlite'
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema_1'].external = 'sqlite_new'
    a, b = compare_manifest(manifest, f'''
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

''', context)
    assert a == b


def test_inspect_multiple_resources_advanced(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
    sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    sqlite_new.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Integer, sa.ForeignKey("CONTINENT.ID")),
        ],
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
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
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema'].external = 'sqlite'
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema_1'].external = 'sqlite_new'
    a, b = compare_manifest(manifest, f'''
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

''', context)
    assert a == b


def test_inspect_multiple_datasets(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
        'CITY': [
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
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
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema'].external = 'sqlite'
    commands.get_dataset(context, manifest, 'datasets/gov/loc').resources['schema'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
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


''', context)
    assert a == b


def test_inspect_multiple_datasets_advanced_manifest_priority(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Integer, sa.ForeignKey("CONTINENT.ID")),
        ],
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
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
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema'].external = 'sqlite'
    commands.get_dataset(context, manifest, 'datasets/gov/loc').resources['schema'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
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
''', context)
    assert a == b


def test_inspect_multiple_datasets_advanced_external_priority(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Integer, sa.ForeignKey("CONTINENT.ID")),
        ],
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
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
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-p', 'external',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['schema'].external = 'sqlite'
    commands.get_dataset(context, manifest, 'datasets/gov/loc').resources['schema'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
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
''', context)
    assert a == b


def test_inspect_multiple_datasets_different_resources(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
    sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Integer, sa.ForeignKey("CONTINENT.ID")),
        ],
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })
    sqlite_new.init({
        'CAR': [
            sa.Column('NAME', sa.Text),
            sa.Column('ENGINE', sa.Integer, sa.ForeignKey("ENGINE.ID")),
        ],
        'ENGINE': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/loc |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
       datasets/gov/car     |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite_new.dsn} |         |         |
                            |         |     |         |         |         |
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/car').resources['schema'].external = 'sqlite_new'
    commands.get_dataset(context, manifest, 'datasets/gov/loc').resources['schema'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
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

''', context)
    assert a == b


def test_inspect_multiple_datasets_different_resources_specific(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite,
    sqlite_new: Sqlite
):
    # Prepare source data.
    context = create_test_context(rc_new)
    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Integer, sa.ForeignKey("CONTINENT.ID")),
        ],
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })
    sqlite_new.init({
        'CAR': [
            sa.Column('NAME', sa.Text),
            sa.Column('ENGINE', sa.Integer, sa.ForeignKey("ENGINE.ID")),
        ],
        'ENGINE': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
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
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-r', 'sql', sqlite_new.dsn,
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/car').resources['schema'].external = 'sqlite_new'
    commands.get_dataset(context, manifest, 'datasets/gov/loc').resources['schema'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
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
''', context)
    assert a == b


def test_inspect_with_views(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    sqlite: Sqlite
):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('CODE', sa.Text),
            sa.Column('CONTINENT', sa.Integer, sa.ForeignKey("CONTINENT.ID")),
        ],
        'CONTINENT': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('NAME', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })
    sqlite.engine.execute('''
        CREATE VIEW TestView
        AS SELECT a.CODE, a.CONTINENT, b.NAME FROM COUNTRY a, CONTINENT b
        WHERE a.CODE = b.CODE;
    ''')

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    cli.invoke(rc_new, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'db_sqlite').resources['resource1'].external = 'sqlite'
    commands.get_dataset(context, manifest, 'db_sqlite/views').resources['resource1'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
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

''', context)
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
    sqlite.init({
        'COUNTRY': [
            sa.Column('NAME', sa.Text),
        ],
    })

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, sqlite, tmp_path / 'manifest.csv', f'''
       d | r | m | property | type    | ref  | source       | prepare | access  | title
         | test             | sql     |      | {sqlite.dsn} |         |         |
                            |         |      |              |         |         |
       datasets/gov/example |         |      |              |         |         | Example
         | schema           | sql     | test |              |         |         |
                            |         |      |              |         |         |
         |   | Country      |         |      | COUNTRY      |         |         | Country
         |   |   | name     | string  |      | NAME         |         | open    | Country name
         |   |   | code     | string  |      | CODE         |         | open    | Country code
       ''')
    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/gov/example').resources['test'].external = 'sqlite'
    a, b = compare_manifest(manifest, f'''
       d | r | m | property | type    | ref  | source  | prepare | access  | title
       datasets/gov/example |         |      |         |         |         | Example
         | test             | sql     |      | sqlite  |         |         |
                            |         |      |         |         |         |
         | schema           | sql     | test |         |         |         |
                            |         |      |         |         |         |
         |   | Country      |         |      | COUNTRY |         |         | Country
         |   |   | name     | string  |      | NAME    |         | open    | Country name
         |   |   | code     | string  |      |         |         | open    | Country code
''', context)
    assert a == b


def test_inspect_json_model_ref_change(
    rc_new: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path):
    json_manifest = [
        {
            "name": "Lithuania",
            "code": "LT",
            "location": {
                "latitude": 54.5,
                "longitude": 12.6
            },
            "cities": [
                {
                    "name": "Vilnius",
                    "weather": {
                        "temperature": 24.7,
                        "wind_speed": 12.4
                    }
                },
                {
                    "name": "Kaunas",
                    "weather": {
                        "temperature": 29.7,
                        "wind_speed": 11.4
                    }
                }
            ]
        },
        {
            "name": "Latvia",
            "code": "LV",
            "cities": [
                {
                    "name": "Riga"
                }
            ]
        }
    ]
    path = tmp_path / 'manifest.json'
    path.write_text(json.dumps(json_manifest))
    context = create_test_context(rc_new)

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc_new = configure(context, rc_new, None, tmp_path / 'manifest.csv', f'''
           d | r | m      | property            | type                   | ref    | source              
           datasets/json/inspect                |                        |        |
             | resource                         | json                   |        | {path}
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
           ''')

    cli.invoke(rc_new, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc_new, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/json/inspect').resources['resource'].external = 'resource.json'
    a, b = compare_manifest(manifest, f'''
d | r | model  | property            | type                   | ref    | source
datasets/json/inspect           |                        |        |
  | resource                    | json                   |        | resource.json
                                |                        |        |
  |   | Pos                     |                        | code   | .
  |   |   | name                | string required unique |        | name
  |   |   | code                | string required        |        | code
  |   |   | location_latitude   | number unique          |        | location.latitude
  |   |   | location_longitude  | number unique          |        | location.longitude
                                |                        |        |
  |   | Cities                  |                        |        | cities
  |   |   | name                | string required unique |        | name
  |   |   | weather_temperature | number unique          |        | weather.temperature
  |   |   | weather_wind_speed  | number unique          |        | weather.wind_speed
  |   |   | parent              | ref                    | Pos    | ..
    ''', context)
    assert a == b


def test_inspect_xml_model_ref_change(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path):
    xml = '''
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
'''
    path = tmp_path / 'manifest.xml'
    path.write_text(xml)
    context = create_test_context(rc)

    result_file_path = tmp_path / 'result.csv'
    # Configure Spinta.
    rc = configure(context, rc, None, tmp_path / 'manifest.csv', f'''
           d | r | m      | property             | type                   | ref    | source              
           datasets/xml/inspect                  |                        |        |
             | resource                          | xml                    |        | {path}
                                                 |                        |        |
             |   | Country |                     |                        | code   | /countries/country
             |   |         | name                | string required unique |        | @name
             |   |         | code                | string required unique |        | @code
             |   |         | location_latitude   | number unique          |        | location/@latitude
             |   |         | location_longitude  | number unique          |        | location/@longitude
                                                 |                        |        |
             |   | City    |                     |                        |        | /countries/country/city
             |   |         | name                | string required unique |        | @name
             |   |         | weather_temperature | number unique          |        | weather/temperature
             |   |         | weather_wind_speed  | number unique          |        | weather/wind_speed
           ''')

    cli.invoke(rc, [
        'inspect',
        tmp_path / 'manifest.csv',
        '-o', tmp_path / 'result.csv',
    ])
    # Check what was detected.
    context, manifest = load_manifest_and_context(rc, result_file_path)
    commands.get_dataset(context, manifest, 'datasets/xml/inspect').resources['resource'].external = 'resource.xml'
    a, b = compare_manifest(manifest, f'''
d | r | model  | property            | type                   | ref    | source
datasets/xml/inspect            |                        |        |
  | resource                    | xml                    |        | resource.xml
                                |                        |        |
  |   | Country                 |                        | code   | /countries/country
  |   |   | name                | string required unique |        | @name
  |   |   | code                | string required        |        | @code
  |   |   | location_latitude   | number unique          |        | location/@latitude
  |   |   | location_longitude  | number unique          |        | location/@longitude
                                |                        |        |
  |   | City                    |                        |        | /countries/country/city
  |   |   | name                | string required unique |        | @name
  |   |   | weather_temperature | number unique          |        | weather/temperature
  |   |   | weather_wind_speed  | number unique          |        | weather/wind_speed
  |   |   | country             | ref                    | Country | ..
    ''', context)
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
