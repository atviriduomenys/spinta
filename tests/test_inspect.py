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

    # This was added only to remove s3.
    rc = rc.fork({
        'backends': {
            'default': {
                'type': 'memory',
            }
        },
    })

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
