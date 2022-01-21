from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import error


def test_empty_manifest(rc: RawConfig, postgresql: str):
    rc = rc.fork({
        'default_auth_client': 'default',
    })
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | access
    ''')

    app = create_test_client(context)
    resp = app.get('/')
    assert error(resp) == 'AuthorizedClientsOnly'


def test_manifest_without_open_properties(rc: RawConfig, postgresql: str):
    rc = rc.fork({
        'default_auth_client': 'default',
    })

    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | access
    datasets/gov/vpt/new     |        |
      | resource             |        |
      |   |   | Country      |        |
      |   |   |   | name     | string |
      |   |   | City         |        |
      |   |   |   | name     | string |
    ''')

    app = create_test_client(context)
    resp = app.get('/')
    assert error(resp) == 'AuthorizedClientsOnly'


def test_manifest_with_open_properties(rc: RawConfig, postgresql: str):
    rc = rc.fork({
        'default_auth_client': 'default',
    })

    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | access
    datasets/gov/vpt/new     |        |
      | resource             |        |
      |   |   | Country      |        |
      |   |   |   | name     | string |
      |   |   | City         |        |
      |   |   |   | name     | string | open
    ''')

    app = create_test_client(context)
    resp = app.get('/')
    assert resp.json() == {
        '_data': [
            {
                'name': 'datasets/:ns',
                'title': '',
                'description': '',
            },
        ]
    }
