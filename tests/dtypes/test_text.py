from pytest import FixtureRequest

from spinta.core.config import RawConfig
from spinta.formats.html.components import Cell
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.request import render_data


def test_text(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property      | type
    backends/postgres/dtypes/text |
      |   |   | Country           |
      |   |   |   | name@lt       | text
      |   |   |   | name@en       | text
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('backends/postgres/dtypes/text/Country', [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post('/backends/postgres/dtypes/text/Country', json={
        'name': {
            'lt': "Lietuva",
            'en': "Lithuania",
        }
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get('/backends/postgres/dtypes/text/Country')
    assert listdata(resp, full=True) == [
        {
            'name': {
                'lt': "Lietuva",
                'en': "Lithuania",
            },
        }
    ]


def test_text_patch(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property      | type
    backends/postgres/dtypes/text |
      |   |   | Country           |
      |   |   |   | name@lt       | text
      |   |   |   | name@en       | text
    ''', backend=postgresql, request=request)

    app = create_test_client(context)
    app.authmodel('backends/postgres/dtypes/text/Country', [
        'insert',
        'patch',
        'getall',
    ])

    # Write data
    resp = app.post('/backends/postgres/dtypes/text/Country', json={
        'name': {
            'lt': "Lietuva",
            'en': "Lithuania",
        }
    })
    assert resp.status_code == 201

    # Patch data
    data = resp.json()
    pk = data['_id']
    rev = data['_revision']
    resp = app.patch(f'/backends/postgres/dtypes/text/Country/{pk}', json={
        '_revision': rev,
        'name': {
            'lt': "Latvija",
            'en': "Latvia",
        }
    })
    assert resp.status_code == 200

    # Read data
    resp = app.get('/backends/postgres/dtypes/text/Country')
    assert listdata(resp, full=True) == [
        {
            'name': {
                'lt': "Latvija",
                'en': "Latvia",
            },
        }
    ]


def test_html(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type | access
    example                  |      |
      |   |   | Country      |      |
      |   |   |   | name@lt  | text | open
      |   |   |   | name@en  | text | open
    ''')
    result = render_data(
        context, manifest,
        'example/Country',
        query=None,
        accept='text/html',
        data={
            '_id': '262f6c72-4284-4d26-b9b0-e282bfe46a46',
            '_revision': 'b6197bb7-3592-4cdb-a61c-5a618f44950c',
            'name': {
                'lt': 'Lietuva',
                'en': 'Lithuania',
            },
        },
    )
    assert result == {
        '_id': Cell(
            value='262f6c72',
            link='/example/Country/262f6c72-4284-4d26-b9b0-e282bfe46a46',
            color=None,
        ),
        'name.lt': Cell(value='Lietuva', link=None, color=None),
        'name.en': Cell(value='Lithuania', link=None, color=None),
    }
