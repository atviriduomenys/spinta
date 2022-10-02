import hashlib
from typing import Tuple

import pytest

from spinta.core.config import RawConfig
from spinta.testing.client import TestClient
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.data import pushdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import load_manifest
from spinta.types.namespace import sort_models_by_refs
from spinta.utils.data import take


def _create_data(app: TestClient, ns: str) -> Tuple[str, str]:
    continent = ns + '/continent'
    country = ns + '/country'
    capital = ns + '/capital'

    app.authmodel(continent, ['insert'])
    app.authmodel(country, ['insert'])
    app.authmodel(capital, ['insert'])

    eu = take('_id', pushdata(app, continent, {
        'title': 'Europe',
    }))
    lt = take('_id', pushdata(app, country, {
        'code': 'lt',
        'title': 'Lithuania',
        'continent': {'_id': eu},
    }))
    pushdata(app, capital, {
        'title': 'Vilnius',
        'country': {'_id': lt},
    })

    return eu, lt


@pytest.mark.models('datasets/backends/postgres/dataset')
def test_getall(model: str, app: TestClient):
    eu, lt = _create_data(app, model)
    app.authorize(['spinta_getall'])

    resp = app.get('/datasets/backends/postgres/dataset/:all')
    assert listdata(resp, full=True) == [
        {'title': 'Europe'},
        {'code': 'lt', 'continent': {'_id': eu}, 'title': 'Lithuania'},
        {'country': {'_id': lt}, 'title': 'Vilnius'},
    ]

    resp = app.get('/datasets/csv/:all')
    assert listdata(resp) == []


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.models('datasets/backends/postgres/dataset')
def test_getall_ns(model, app):
    _create_data(app, model)
    app.authorize(['spinta_getall'])

    resp = app.get('/datasets/backends/postgres/dataset/:ns/:all')
    assert listdata(resp, 'name') == [
        'datasets/backends/postgres/dataset/capital',
        'datasets/backends/postgres/dataset/continent',
        'datasets/backends/postgres/dataset/country',
        'datasets/backends/postgres/dataset/org',
        'datasets/backends/postgres/dataset/report',
    ]


def test_ns_titles(
    rc: RawConfig,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref          | title               | description
                             | ns     | datasets     | All datasets        | All external datasets.
                             | ns     | datasets/gov | Government datasets | All external government datasets.
    datasets/gov/vpt/new     |        |              | New data            | Data from a new database.
      | resource             |        |              |                     |
      |   |   | Country      |        |              | Countries           | All countries.
      |   |   |   | name     | string |              | Country name        | Name of a country.
      |   |   | City         |        |              | Cities              | All cities.
      |   |   |   | name     | string |              | City name           | Name of a city.
    ''')
    app = create_test_client(context, scope=['spinta_getall'])
    assert listdata(app.get('/:ns'), 'title', 'description') == [
        ("All datasets", "All external datasets."),
    ]
    assert listdata(app.get('/:ns/:all'), 'name', 'title', 'description') == [
        ('datasets/:ns', "All datasets", "All external datasets."),
        ('datasets/gov/:ns', "Government datasets", "All external government datasets."),
        ('datasets/gov/vpt/:ns', "", ""),
        ('datasets/gov/vpt/new/:ns', "New data", "Data from a new database."),
        ('datasets/gov/vpt/new/City', "Cities", "All cities."),
        ('datasets/gov/vpt/new/Country', "Countries", "All countries."),
    ]


def test_ns_titles_bare_models(
    rc: RawConfig,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property                 | type   | ref                  | title               | description
                                             | ns     | datasets             | All datasets        | All external datasets.
                                             |        | datasets/gov         | Government datasets | All external government datasets.
                                             |        | datasets/gov/vpt/new | New data            | Data from a new database.
                                             |        |                      |                     |               
      |   |   | datasets/gov/vpt/new/Country |        |                      | Countries           | All countries.
      |   |   |   | name                     | string |                      | Country name        | Name of a country.
      |   |   | datasets/gov/vpt/new/City    |        |                      | Cities              | All cities.
      |   |   |   | name                     | string |                      | City name           | Name of a city.
    ''')
    app = create_test_client(context, scope=['spinta_getall'])
    assert listdata(app.get('/:ns'), 'title', 'description') == [
        ("All datasets", "All external datasets."),
    ]
    assert listdata(app.get('/:ns/:all'), 'name', 'title', 'description') == [
        ('datasets/:ns', "All datasets", "All external datasets."),
        ('datasets/gov/:ns', "Government datasets", "All external government datasets."),
        ('datasets/gov/vpt/:ns', "", ""),
        ('datasets/gov/vpt/new/:ns', "New data", "Data from a new database."),
        ('datasets/gov/vpt/new/City', "Cities", "All cities."),
        ('datasets/gov/vpt/new/Country', "Countries", "All countries."),
    ]


def test_sort_models_by_refs(rc: RawConfig):
    manifest = load_manifest(rc, '''
    d | r | b | m | property  | type   | ref       | access
    datasets/gov/example      |        |           |
      |   |                   |        |           |
      |   |   | Continent     |        |           |
      |   |   |   | name      | string |           | open
      |   |                   |        |           |
      |   |   | Country       |        |           |
      |   |   |   | code      | string |           | open
      |   |   |   | name      | string |           | open
      |   |   |   | continent | ref    | Continent | open
      |   |                   |        |           |
      |   |   | City          |        |           |
      |   |   |   | name      | string |           | open
      |   |   |   | country   | ref    | Country   | open
    ''')

    models = sort_models_by_refs(manifest.models.values())
    names = [model.name for model in models]
    assert names == [
        'datasets/gov/example/City',
        'datasets/gov/example/Country',
        'datasets/gov/example/Continent',
    ]
