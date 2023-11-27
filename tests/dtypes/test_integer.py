from typing import Optional

import pytest

from spinta import commands
from spinta.backends.memory.components import Memory
from spinta.commands.write import dataitem_from_payload
from spinta.components import Store
from spinta.core.config import RawConfig
from spinta.testing.manifest import bootstrap_manifest
from spinta.manifests.components import Manifest


@pytest.mark.parametrize('value', [
    None,
    0,
    1,
    -1,
    1000,
    -1000,
])
def test_integer(rc: RawConfig, value: Optional[int]):
    context = bootstrap_manifest(rc, '''
    d | m | property     | type
    datasets/gov/example |
      | City             |
      |   | population   | integer
    ''', backend='memory')
    store: Store = context.get('store')
    manifest: Manifest = store.manifest
    backend: Memory = manifest.backend
    model = commands.get_model(context, manifest, 'datasets/gov/example/City')
    payload = {
        '_op': 'insert',
        'population': value,
    }
    context.set('transaction', backend.transaction(write=True))
    data = dataitem_from_payload(context, model, payload)
    data.given = commands.load(context, model, payload)
    commands.simple_data_check(context, data, model, backend)
    commands.complex_data_check(context, data, model, backend)
