import sqlalchemy as sa

from spinta.testing.cli import SpintaCliRunner
from spinta.testing.config import configure
from spinta.testing.tabular import load_tabular_manifest
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import striptable


def test_detect_pii(rc, cli: SpintaCliRunner, tmpdir, sqlite):
    # Prepare source data.
    sqlite.init({
        'PERSON': [
            sa.Column('NAME', sa.Text),
            sa.Column('EMAIL', sa.Text),
            sa.Column('PHONE', sa.Text),
            sa.Column('CODE', sa.Text),
        ],
    })

    sqlite.write('PERSON', [
        {
            'NAME': "Amelija Kazlauskė",
            'EMAIL': 'amelija@example.com',
            'PHONE': '+370 675 36104',
            'CODE': '43109127482',
        },
        {
            'NAME': "Lukas Stankevičius",
            'EMAIL': 'lukas.stankevicius@example.com',
            'PHONE': '8 636 60400',
            'CODE': '32701264423',
        },
        {
            'NAME': "Emilija Petrauskaitė",
            'EMAIL': 'emilija@example.com',
            'PHONE': '0370 633 46560',
            'CODE': '46002270784',
        },
    ])

    # Configure Spinta.
    rc = configure(rc, sqlite, tmpdir / 'manifest.csv', '''
    d | r | m | property     | type   | ref     | source  | access
    datasets/ds              |        |         |         |
      | rs                   | sql    | sql     |         |
      |   | Person           |        |         | PERSON  | open
      |   |   | name         | string |         | NAME    |
      |   |   | email        | string |         | EMAIL   |
      |   |   | phone        | string |         | PHONE   |
      |   |   | nin          | string |         | CODE    |
    ''')

    # Detect person identifying information.
    cli.invoke(rc, [
        'pii', 'detect', tmpdir / 'manifest.csv',
        '-o', tmpdir / 'pii.csv',
        '--stop-on-error',
    ])

    # Check what was detected.
    manifest = load_tabular_manifest(rc, tmpdir / 'pii.csv')
    cols = [
        'dataset', 'resource', 'model', 'property', 'type', 'ref', 'source',
        'access', 'uri',
    ]
    assert render_tabular_manifest(manifest, cols) == striptable('''
    d | r | m | property | type   | ref | source | access    | uri
                         | prefix | pii |        |           | https://data.gov.lt/pii/
    datasets/ds          |        |     |        | protected |
      | rs               | sql    | sql |        | protected |
                         |        |     |        |           |
      |   | Person       |        |     | PERSON | open      |
      |   |   | name     | string |     | NAME   | open      |
      |   |   | email    | string |     | EMAIL  | open      | pii:email
      |   |   | phone    | string |     | PHONE  | open      | pii:phone
      |   |   | nin      | string |     | CODE   | open      | pii:id
    ''')

