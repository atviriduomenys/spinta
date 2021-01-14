import pathlib

import sqlalchemy as sa

from spinta.cli.pii import detect
from spinta.core.config import RawConfig
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import load_tabular_manifest
from spinta.testing.tabular import render_tabular_manifest
from spinta.testing.tabular import striptable


def configure(
    rc: RawConfig,
    db: Sqlite,
    path: pathlib.Path,  # manifest file path
    manifest: str,
) -> RawConfig:
    create_tabular_manifest(path, striptable(manifest))
    return rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(path),
                'backend': 'sql',
                'keymap': 'default',
            },
        },
        'backends': {
            'sql': {
                'type': 'sql',
                'dsn': db.dsn,
            },
        },
        # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
        'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
    })


def test_detect_pii(rc, cli, tmpdir, sqlite):
    # Prepare source data.
    sqlite.init({
        'PERSON': [
            sa.Column('NAME', sa.Text),
            sa.Column('EMAIL', sa.Text),
            sa.Column('PHONE', sa.Text),
        ],
    })

    sqlite.write('PERSON', [
        {
            'NAME': "Amelija Kazlauskė",
            'EMAIL': 'amelija@example.com',
            'PHONE': '+370 675 36104',
        },
        {
            'NAME': "Lukas Stankevičius",
            'EMAIL': 'lukas.stankevicius@example.com',
            'PHONE': '8 636 60400',
        },
        {
            'NAME': "Emilija Petrauskaitė",
            'EMAIL': 'emilija@example.com',
            'PHONE': '0370 633 46560',
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
    ''')

    # Detect person identifying information.
    cli.invoke(rc, detect, [
        str(tmpdir / 'manifest.csv'),
        '-o', str(tmpdir / 'pii.csv'),
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
    ''')

