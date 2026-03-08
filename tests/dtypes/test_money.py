from typing import cast

from pytest import FixtureRequest

from spinta.backends.postgresql.components import PostgreSQL
from spinta.commands.write import dataitem_from_payload
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest

import pytest

from spinta import commands
from spinta.manifests.components import Manifest
from spinta.components import Store


def test_money(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/money             |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | amount                     | money          | EUR
    ''', backend=postgresql, request=request)

    model: str = 'backends/postgres/dtypes/money/City'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post(f'/{model}', json={
        'name': "Vilnius",
        'amount': 100
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get(f'/{model}')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'amount': 100
        }
    ]


def test_money_currency(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/money             |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | amount                     | money          | 
      |   |   |   | currency                   | string         | 
    ''', backend=postgresql, request=request)

    model: str = 'backends/postgres/dtypes/money/City'

    app = create_test_client(context)
    app.authmodel(model, [
        'insert',
        'getall',
    ])

    # Write data
    resp = app.post(f'/{model}', json={
        'name': "Vilnius",
        'amount': 100,
        'currency': 'EUR',
    })
    assert resp.status_code == 201

    # Read data
    resp = app.get(f'/{model}')
    assert listdata(resp, full=True) == [
        {
            'name': "Vilnius",
            'amount': 100,
            'currency': 'EUR',
        }
    ]


@pytest.mark.parametrize('value', [
    'EUR',
    'EEUR',
])
def test_currency_value(rc: RawConfig, postgresql: str, value: str):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property                   | type           | ref
    backends/postgres/dtypes/money             |                |
      |   |   | City                           |                |
      |   |   |   | name                       | string         |
      |   |   |   | amount                     | money          | 
      |   |   |   | currency                   | string         | 
    ''', backend=postgresql)

    store: Store = context.get('store')
    manifest: Manifest = store.manifest

    model = manifest.models['backends/postgres/dtypes/money/City']
    backend = model.backend

    load_data_for_payload = {
        '_op': 'insert',
        'currency': value,
    }

    context.set('transaction', backend.transaction(write=True))

    data = dataitem_from_payload(context, model, load_data_for_payload)
    data.given = commands.load(context, model, data.payload)

    commands.simple_data_check(context, data, model, backend)
