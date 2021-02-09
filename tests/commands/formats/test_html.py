import hashlib
from typing import List
from typing import Tuple

import pytest
from starlette.requests import Request

from spinta import commands
from spinta.commands.formats.html import get_current_location
from spinta.components import Context
from spinta.components import Namespace
from spinta.components import UrlParams
from spinta.components import Version
from spinta.core.config import RawConfig
from spinta.manifests.components import Manifest
from spinta.testing.manifest import configure_manifest
from spinta.types.namespace import get_ns_model


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


@pytest.fixture(scope='module')
def context_city(rc: RawConfig) -> Context:
    return configure_manifest(rc, '''
    d | r | b | m | property | type   | ref          | title               | description
                             | ns     | datasets     | All datasets        | All external datasets.
                             | ns     | datasets/gov | Government datasets | All external government datasets.
    datasets/gov/vpt/new     |        |              | New data            | Data from a new database.
      | resource             |        |              |                     |
      |   |   | City         |        |              | Cities              | All cities.
      |   |   |   | name     | string |              | City name           | Name of a city.
    ''')


def _get_current_loc(context: Context, path: str):
    manifest: Manifest = context.get('store').manifest
    request = Request({
        'type': 'http',
        'method': 'GET',
        'path': path,
        'path_params': {'path': path},
        'headers': {},
    })
    params = commands.prepare(context, UrlParams(), Version(), request)
    if isinstance(params.model, Namespace):
        model = get_ns_model(context, params.model)
    else:
        model = params.model
    return get_current_location(model, params)


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
    context_city: Context,
    path: str,
    result: List[Tuple[
        str,    # Link title
        str,    # Link
    ]],
):
    assert _get_current_loc(context_city, path) == result
