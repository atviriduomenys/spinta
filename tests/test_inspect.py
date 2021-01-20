import sqlalchemy as sa

from spinta.cli.inspect import inspect
from spinta.testing.config import configure
from spinta.testing.tabular import load_tabular_manifest
from spinta.testing.tabular import render_tabular_manifest
from spinta.testing.tabular import striptable


def test_inspect(rc, cli, tmpdir, sqlite):
    # Prepare source data.
    sqlite.init({
        'COUNTRY': [
            sa.Column('ID', sa.Integer, primary_key=True),
            sa.Column('CODE', sa.Text),
            sa.Column('NAME', sa.Text),
        ],
    })

    # Configure Spinta.
    rc = configure(rc, None, tmpdir / 'manifest.csv', f'''
    d | r | m | property     | type   | ref | source       | access
    dataset                  |        |     |              |
      | rs                   | sql    |     | {sqlite.dsn} |
    ''')

    cli.invoke(rc, inspect)

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    manifest.objects['dataset']['dataset'].resources['rs'].external = 'sqlite'
    assert render_tabular_manifest(manifest) == striptable(f'''
    id | d | r | b | m | property | source  | prepare | type    | ref | level | access    | uri | title | description
       | dataset                  |         |         |         |     |       | protected |     |       |
       |   | rs                   | sqlite  |         | sql     |     |       | protected |     |       |
       |                          |         |         |         |     |       |           |     |       |
       |   |   |   | Country      | COUNTRY |         |         | id  |       | protected |     |       |
       |   |   |   |   | id       | ID      |         | integer |     |       | protected |     |       |
       |   |   |   |   | code     | CODE    |         | string  |     |       | protected |     |       |
       |   |   |   |   | name     | NAME    |         | string  |     |       | protected |     |       |
    ''')
