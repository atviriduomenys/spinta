import base64
import hashlib
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import pytest
from _pytest.fixtures import FixtureRequest
from starlette.requests import Request

from spinta import commands
from spinta.components import Store
from spinta.formats.html.commands import _LimitIter
from spinta.formats.html.components import Cell
from spinta.formats.html.helpers import CurrentLocation
from spinta.formats.html.helpers import get_current_location
from spinta.formats.html.helpers import short_id
from spinta.components import Config
from spinta.components import Context
from spinta.components import Namespace
from spinta.components import UrlParams
from spinta.components import Version
from spinta.core.config import RawConfig
from spinta.testing.client import TestClient
from spinta.testing.client import TestClientResponse
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from spinta.utils.data import take


def _get_data_table(context: dict):
    return [tuple(context['header'])] + [
        tuple(cell['value'] for cell in row)
        for row in context['data']
    ]


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('datasets')
def test_select_with_joins(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('continent/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('country/:dataset/dependencies/:resource/continents', ['insert'])
    app.authmodel('capital/:dataset/dependencies/:resource/continents', ['insert', 'search'])

    resp = app.post('/', json={
        '_data': [
            {
                '_type': 'continent/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': sha1('1'),
                'title': 'Europe',
            },
            {
                '_type': 'country/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': sha1('2'),
                'title': 'Lithuania',
                'continent': sha1('1'),
            },
            {
                '_type': 'capital/:dataset/dependencies/:resource/continents',
                '_op': 'insert',
                '_id': sha1('3'),
                'title': 'Vilnius',
                'country': sha1('2'),
            },
        ]
    })
    assert resp.status_code == 200, resp.json()

    resp = app.get(
        '/capital/:dataset/dependencies/:resource/continents?'
        'select(_id,title,country.title,country.continent.title)&'
        'format(html)'
    )
    assert _get_data_table(resp.context) == [
        ('_id', 'title', 'country.title', 'country.continent.title'),
        (sha1('3')[:8], 'Vilnius', 'Lithuania', 'Europe'),
    ]


def test_limit_in_links(app):
    app.authmodel('country', ['search', ])
    resp = app.get('/country/:format/html?limit(1)')
    assert resp.context['formats'][0] == (
        'CSV', '/country/:format/csv?limit(1)'
    )


def _get_current_loc(context: Context, path: str):
    request = Request({
        'type': 'http',
        'method': 'GET',
        'path': path,
        'path_params': {'path': path},
        'headers': {},
    })
    params = commands.prepare(context, UrlParams(), Version(), request)
    if isinstance(params.model, Namespace):
        store: Store = context.get('store')
        model = store.manifest.models['_ns']
    else:
        model = params.model
    config: Config = context.get('config')
    return get_current_location(config, model, params)


@pytest.fixture(scope='module')
def context_current_location(rc: RawConfig) -> Context:
    return bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref          | title               | description
                             | ns     | datasets     | All datasets        | All external datasets.
                             | ns     | datasets/gov | Government datasets | All external government datasets.
    datasets/gov/vpt/new     |        |              | New data            | Data from a new database.
      | resource             |        |              |                     |
      |   |   | City         |        |              | Cities              | All cities.
      |   |   |   | name     | string |              | City name           | Name of a city.
    ''')


@pytest.mark.parametrize('path,result', [
    ('/', [
        ('🏠', '/'),
    ]),
    ('/datasets', [
        ('🏠', '/'),
        ('All datasets', None),
    ]),
    ('/datasets/gov', [
        ('🏠', '/'),
        ('All datasets', '/datasets'),
        ('Government datasets', None),
    ]),
    ('/datasets/gov/vpt', [
        ('🏠', '/'),
        ('All datasets', '/datasets'),
        ('Government datasets', '/datasets/gov'),
        ('vpt', None),
    ]),
    ('/datasets/gov/vpt/new', [
        ('🏠', '/'),
        ('All datasets', '/datasets'),
        ('Government datasets', '/datasets/gov'),
        ('vpt', '/datasets/gov/vpt'),
        ('New data', None),
    ]),
    ('/datasets/gov/vpt/new/City', [
        ('🏠', '/'),
        ('All datasets', '/datasets'),
        ('Government datasets', '/datasets/gov'),
        ('vpt', '/datasets/gov/vpt'),
        ('New data', '/datasets/gov/vpt/new'),
        ('Cities', None),
        ('Changes', '/datasets/gov/vpt/new/City/:changes'),
    ]),
    ('/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08', [
        ('🏠', '/'),
        ('All datasets', '/datasets'),
        ('Government datasets', '/datasets/gov'),
        ('vpt', '/datasets/gov/vpt'),
        ('New data', '/datasets/gov/vpt/new'),
        ('Cities', '/datasets/gov/vpt/new/City'),
        ('0edc2281', None),
        ('Changes', '/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08/:changes'),
    ]),
])
def test_current_location(
    context_current_location: Context,
    path: str,
    result: CurrentLocation,
):
    context = context_current_location
    assert _get_current_loc(context, path) == result


@pytest.fixture(scope='module')
def context_current_location_with_root(rc: RawConfig):
    rc = rc.fork({
        'root': 'datasets/gov/vpt',
    })
    return bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref          | title               | description
                             | ns     | datasets     | All datasets        | All external datasets.
                             | ns     | datasets/gov | Government datasets | All external government datasets.
    datasets/gov/vpt/new     |        |              | New data            | Data from a new database.
      | resource             |        |              |                     |
      |   |   | City         |        |              | Cities              | All cities.
      |   |   |   | name     | string |              | City name           | Name of a city.
    datasets/gov/vpt/old     |        |              | New data            | Data from a new database.
      | resource             |        |              |                     |
      |   |   | Country      |        |              | Countries           | All countries.
      |   |   |   | name     | string |              | Country name        | Name of a country.
    ''')


@pytest.mark.parametrize('path,result', [
    ('/', [
        ('🏠', '/'),
    ]),
    ('/datasets', [
        ('🏠', '/'),
    ]),
    ('/datasets/gov', [
        ('🏠', '/'),
    ]),
    ('/datasets/gov/vpt', [
        ('🏠', '/'),
    ]),
    ('/datasets/gov/vpt/new', [
        ('🏠', '/'),
        ('New data', None),
    ]),
    ('/datasets/gov/vpt/new/City', [
        ('🏠', '/'),
        ('New data', '/datasets/gov/vpt/new'),
        ('Cities', None),
        ('Changes', '/datasets/gov/vpt/new/City/:changes'),
    ]),
    ('/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08', [
        ('🏠', '/'),
        ('New data', '/datasets/gov/vpt/new'),
        ('Cities', '/datasets/gov/vpt/new/City'),
        ('0edc2281', None),
        ('Changes', '/datasets/gov/vpt/new/City/0edc2281-f372-44a7-b0f8-e8d06ad0ce08/:changes'),
    ]),
])
def test_current_location_with_root(
    context_current_location_with_root: Context,
    path: str,
    result: CurrentLocation,
):
    context = context_current_location_with_root
    assert _get_current_loc(context, path) == result


def _prep_file_type(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
) -> Tuple[TestClient, str]:
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | access
    example/html/file        |        |
      | resource             |        |
      |   |   | Country      |        |
      |   |   |   | name     | string | open
      |   |   |   | flag     | image  | open
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/html/file', [
        'insert',
        'getall',
        'getone',
        'changes',
        'search',
    ])

    # Add a file
    resp = app.post(f'/example/html/file/Country', json={
        'name': 'Lithuania',
        'flag': {
            '_id': 'flag.png',
            '_content_type': 'image/png',
            '_content': base64.b64encode(b'IMAGE').decode(),
        },
    })
    assert resp.status_code == 201, resp.json()
    data = resp.json()
    _id = data['_id']

    return app, _id


def _table(data: List[List[Cell]]) -> List[List[Dict[str, Any]]]:
    return [
        [cell.as_dict() for cell in row]
        for row in data
    ]


def _table_with_header(
    resp: TestClientResponse,
) -> List[Dict[str, Dict[str, Any]]]:
    header = resp.context['header']
    return [
        {key: cell.as_dict() for key, cell in zip(header, row)}
        for row in resp.context['data']
    ]


def test_file_type_list(
    postgresql: str,
    rc: RawConfig,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(rc, postgresql, request)
    resp = app.get('/example/html/file/Country', headers={
        'Accept': 'text/html',
    })
    assert _table(resp.context['data']) == [
        [
            {
                'link': f'/example/html/file/Country/{_id}',
                'value': short_id(_id),
            },
            {
                'value': 'Lithuania',
            },
            {
                'link': f'/example/html/file/Country/{_id}/flag',
                'value': 'flag.png',
            },
        ]
    ]


def _row(row: List[Tuple[str, Cell]]) -> List[Tuple[str, Dict[str, Any]]]:
    return [
        (name, cell.as_dict())
        for name, cell in row
    ]


def test_file_type_details(
    postgresql: str,
    rc: RawConfig,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(rc, postgresql, request)
    resp = app.get(f'/example/html/file/Country/{_id}', headers={
        'Accept': 'text/html',
    })
    assert list(map(take, _table_with_header(resp))) == [
        {
            'name': {'value': 'Lithuania'},
            'flag': {
                'value': 'flag.png',
                'link': f'/example/html/file/Country/{_id}/flag',
            },
        },
    ]


def test_file_type_changes(
    postgresql: str,
    rc: RawConfig,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(rc, postgresql, request)
    resp = app.get(f'/example/html/file/Country/:changes', headers={
        'Accept': 'text/html',
    })
    assert _table(resp.context['data'])[0][6:] == [
        {
            'value': 'Lithuania',
        },
        {
            'link': f'/example/html/file/Country/{_id}/flag',
            'value': 'flag.png',
        },
    ]


def test_file_type_changes_single_object(
    postgresql: str,
    rc: RawConfig,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(rc, postgresql, request)
    resp = app.get(f'/example/html/file/Country/{_id}/:changes', headers={
        'Accept': 'text/html',
    })
    assert _table(resp.context['data'])[0][6:] == [
        {
            'value': 'Lithuania',
        },
        {
            'value': 'flag.png',
            'link': f'/example/html/file/Country/{_id}/flag',
        },
    ]


def test_file_type_no_pk(
    postgresql: str,
    rc: RawConfig,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(rc, postgresql, request)
    resp = app.get('/example/html/file/Country?select(name, flag)', headers={
        'Accept': 'text/html',
    })
    assert _table(resp.context['data']) == [
        [
            {
                'value': 'Lithuania',
            },
            {
                'value': 'flag.png',
                'link': f'/example/html/file/Country/{_id}/flag',
            },
        ]
    ]


@pytest.mark.parametrize('limit, exhausted, result', [
    (0, False, []),
    (1, False, [1]),
    (2, False, [1, 2]),
    (3, True, [1, 2, 3]),
    (4, True, [1, 2, 3]),
    (5, True, [1, 2, 3]),
])
def test_limit_iter(limit, exhausted, result):
    it = _LimitIter(limit, iter([1, 2, 3]))
    assert list(it) == result
    assert it.exhausted is exhausted
