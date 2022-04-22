import base64
import hashlib
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest
from starlette.requests import Request

from spinta import commands
from spinta.components import Action
from spinta.components import Store
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import get_select_tree
from spinta.formats.html.commands import _build_template_context
from spinta.formats.html.commands import _LimitIter
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Color
from spinta.formats.html.components import Html
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
from spinta.testing.request import make_get_request
from spinta.testing.manifest import bootstrap_manifest
from spinta.utils.data import take
from spinta.testing.manifest import load_manifest_and_context
from spinta.manifests.components import Manifest


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
            {
                'value': 'image/png',
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
            'flag._id': {
                'value': 'flag.png',
                'link': f'/example/html/file/Country/{_id}/flag',
            },
            'flag._content_type': {'value': 'image/png'},
        },
    ]


def test_file_type_changes(
    postgresql: str,
    rc: RawConfig,
    request: FixtureRequest,
):
    app, _id = _prep_file_type(rc, postgresql, request)
    resp = app.get('/example/html/file/Country/:changes', headers={
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
        {
            'value': 'image/png',
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
        {
            'value': 'image/png',
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
            {
                'value': 'image/png',
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


@pytest.mark.parametrize('value, cell', [
    ({'_id': None}, Cell('', link=None, color=Color.null)),
    ({'_id': 'c634dbd8-416f-457d-8bda-5a6c35bbd5d6'},
     Cell('c634dbd8', link='/example/Country/c634dbd8-416f-457d-8bda-5a6c35bbd5d6')),
])
def test_prepare_ref_for_response(rc: RawConfig, value, cell):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         | name    |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         | name    |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''')
    fmt = Html()
    dtype = manifest.models['example/City'].properties['country'].dtype
    result = commands.prepare_dtype_for_response(
        context,
        fmt,
        dtype,
        value,
        data={},
        action=Action.GETALL,
        select=None,
    )
    assert list(result) == ['_id']
    assert isinstance(result['_id'], Cell)
    assert result['_id'].value == cell.value
    assert result['_id'].color == cell.color
    assert result['_id'].link == cell.link


def _build_context(
    context: Context,
    manifest: Manifest,
    url: str,
    row: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if '?' in url:
        model, query = url.split('?', 1)
    else:
        model, query = url, None

    model = manifest.models[model]
    request = make_get_request(model.name, query, {'accept': 'text/html'})
    action = Action.SEARCH if query else Action.GETALL
    params = commands.prepare(context, UrlParams(), Version(), request)

    select_tree = get_select_tree(context, action, params.select)
    prop_names = get_select_prop_names(
        context,
        model,
        model.properties,
        action,
        select_tree,
        reserved=['_type', '_id', '_revision'],
    )
    row = commands.prepare_data_for_response(
        context,
        model,
        params.fmt,
        row,
        action=action,
        select=select_tree,
        prop_names=prop_names,
    )

    rows = [row]

    ctx = _build_template_context(
        context, model,
        action,
        params,
        rows,
    )

    data = next(ctx['data'], None)
    if data is not None:
        return dict(zip(ctx['header'], data))


def test_select_id(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | City           |         | name    |
      |   |   |   | name       | string  |         | open
    ''')
    result = _build_context(context, manifest, 'example/City?select(_id)', {
        '_id': '19e4f199-93c5-40e5-b04e-a575e81ac373',
        '_revision': 'b6197bb7-3592-4cdb-a61c-5a618f44950c',
    })
    assert result == {
        '_id': Cell(
            value='19e4f199',
            link='/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373',
            color=None,
        ),
    }


def test_select_join(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''')
    result = _build_context(context, manifest, 'example/City?select(_id, country.name)', {
        '_id': '19e4f199-93c5-40e5-b04e-a575e81ac373',
        '_revision': 'b6197bb7-3592-4cdb-a61c-5a618f44950c',
        'country': {'name': 'Lithuania'},
    })
    assert result == {
        '_id': Cell(
            value='19e4f199',
            link='/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373',
            color=None,
        ),
        'country.name': Cell(value='Lithuania', link=None, color=None),
    }


def test_select_join_multiple_props(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''')
    result = _build_context(
        context, manifest,
        'example/City?select(_id, country._id, country.name)',
        {
            '_id': '19e4f199-93c5-40e5-b04e-a575e81ac373',
            '_revision': 'b6197bb7-3592-4cdb-a61c-5a618f44950c',
            'country': {
                '_id': '262f6c72-4284-4d26-b9b0-e282bfe46a46',
                'name': 'Lithuania',
            },
        },
    )
    assert result == {
        '_id': Cell(
            value='19e4f199',
            link='/example/City/19e4f199-93c5-40e5-b04e-a575e81ac373',
            color=None,
        ),
        'country._id': Cell(
            value='262f6c72',
            link='/example/Country/262f6c72-4284-4d26-b9b0-e282bfe46a46',
            color=None,
        ),
        'country.name': Cell(value='Lithuania', link=None, color=None),
    }
