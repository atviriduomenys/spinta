from pathlib import Path

import sqlalchemy as sa

from spinta.core.config import RawConfig
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.config import configure
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import load_tabular_manifest
from spinta.testing.tabular import render_tabular_manifest
from spinta.manifests.tabular.helpers import striptable


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

    cli.invoke(rc, ['inspect'])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['rs'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property   | source     | prepare | type    | ref     | level | access    | uri | title | description
       | dataset                    |            |         |         |         |       | protected |     |       |
       |   | rs                     | sqlite     |         | sql     |         |       | protected |     |       |
       |                            |            |         |         |         |       |           |     |       |
       |   |   |   | Country        | COUNTRY    |         |         | id      |       | protected |     |       |
       |   |   |   |   | id         | ID         |         | integer |         |       | protected |     |       |
       |   |   |   |   | code       | CODE       |         | string  |         |       | protected |     |       |
       |   |   |   |   | name       | NAME       |         | string  |         |       | protected |     |       |
       |                            |            |         |         |         |       |           |     |       |
       |   |   |   | City           | CITY       |         |         |         |       | protected |     |       |
       |   |   |   |   | name       | NAME       |         | string  |         |       | protected |     |       |
       |   |   |   |   | country_id | COUNTRY_ID |         | ref     | Country |       | protected |     |       |
    ''')


def _fix_s3_backend_issue(rc: RawConfig) -> RawConfig:
    # S3 backend is hardcoded globally and requires a fixture.
    # This rc override deletes S3 backend from config.
    return rc.fork({
        'backends': {
            'default': {
                'type': 'memory',
            }
        },
    })


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

    rc = _fix_s3_backend_issue(rc)

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
    ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['sql'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property   | source     | prepare | type    | ref     | level | access    | uri | title | description
       | dataset                    |            |         |         |         |       | protected |     |       |
       |   | sql                    | sqlite     |         | sql     |         |       | protected |     |       |
       |                            |            |         |         |         |       |           |     |       |
       |   |   |   | Country        | COUNTRY    |         |         | id      |       | protected |     |       |
       |   |   |   |   | id         | ID         |         | integer |         |       | protected |     |       |
       |   |   |   |   | code       | CODE       |         | string  |         |       | protected |     |       |
       |   |   |   |   | name       | NAME       |         | string  |         |       | protected |     |       |
       |                            |            |         |         |         |       |           |     |       |
       |   |   |   | City           | CITY       |         |         |         |       | protected |     |       |
       |   |   |   |   | name       | NAME       |         | string  |         |       | protected |     |       |
       |   |   |   |   | country_id | COUNTRY_ID |         | ref     | Country |       | protected |     |       |
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

    rc = _fix_s3_backend_issue(rc)

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
              ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['sql'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property   | source     | prepare | type    | ref     | level | access    | uri | title | description
       | dataset                    |            |         |         |         |       | protected |     |       |
       |   | sql                    | sqlite     |         | sql     |         |       | protected |     |       |
       |                            |            |         |         |         |       |           |     |       |
       |   |   |   | City           | CITY       |         |         | id      |       | protected |     |       |
       |   |   |   |   | id         | ID         |         | integer |         |       | protected |     |       |
       |   |   |   |   | name       | NAME       |         | string  |         |       | protected |     |       |
       |   |   |   |   | country_id | COUNTRY_ID |         | ref     | Country |       | protected |     |       |
       |                            |            |         |         |         |       |           |     |       |
       |   |   |   | Country        | COUNTRY    |         |         | id      |       | protected |     |       |
       |   |   |   |   | id         | ID         |         | integer |         |       | protected |     |       |
       |   |   |   |   | capital    | CAPITAL    |         | ref     | City    |       | protected |     |       |
       |   |   |   |   | name       | NAME       |         | string  |         |       | protected |     |       |
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

    rc = _fix_s3_backend_issue(rc)

    cli.invoke(rc, [
        'inspect',
        '-r', 'sql', sqlite.dsn,
        '-o', tmpdir / 'manifest.csv',
              ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.datasets['dataset'].resources['sql'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property  | source    | prepare | type    | ref      | level | access    | uri | title | description
       | dataset                   |           |         |         |          |       | protected |     |       |
       |   | sql                   | sqlite    |         | sql     |          |       | protected |     |       |
       |                           |           |         |         |          |       |           |     |       |
       |   |   |   | Category      | CATEGORY  |         |         | id       |       | protected |     |       |
       |   |   |   |   | id        | ID        |         | integer |          |       | protected |     |       |
       |   |   |   |   | name      | NAME      |         | string  |          |       | protected |     |       |
       |   |   |   |   | parent_id | PARENT_ID |         | ref     | Category |       | protected |     |       |
    ''')
