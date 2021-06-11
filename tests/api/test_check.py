from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.tabular import convert_ascii_manifest_to_csv


def test_success(rc: RawConfig):
    app = create_test_client(rc, raise_server_exceptions=False)
    app.authmodel('', ['check'])
    csv_manifest = convert_ascii_manifest_to_csv('''
    id | d | r | b | m | property | type
       | datasets/gov/example     |
       |   |   |   | Country      |
       |   |   |   |   | name     | string
    ''')
    resp = app.post('/:check', files={
        'manifest': ('manifest.csv', csv_manifest, 'text/csv'),
    })
    assert resp.json() == {'status': 'OK'}


def test_unknown_field(rc: RawConfig):
    app = create_test_client(rc, raise_server_exceptions=False)
    app.authmodel('', ['check'])
    csv_manifest = convert_ascii_manifest_to_csv('''
    id | d | r | b | m | property | typo
       | datasets/gov/example     |
       |   |   |   | Country      |
       |   |   |   |   | name     | string
    ''')
    resp = app.post('/:check', files={
        'manifest': ('manifest.csv', csv_manifest, 'text/csv'),
    })
    assert resp.json() == {
        'errors': [
            {
                'code': 'TabularManifestError',
                'message': 'manifest.csv:1: Unknown columns: typo.',
            },
        ]
    }
