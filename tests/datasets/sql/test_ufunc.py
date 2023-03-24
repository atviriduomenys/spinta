from decimal import Decimal

import pytest

from spinta.core.config import RawConfig
from spinta.datasets.backends.sql.ufuncs.components import SqlResultBuilder
from spinta.testing.manifest import load_manifest_and_context
from spinta.exceptions import UnableToCast


@pytest.mark.parametrize('value', [
    1.0,
    Decimal(1.0),
])
def test_cast_integer(rc: RawConfig, value):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare | level | title
    example                  |         |         |       |
      |   |   | Data         |         |         |       |
      |   |   |   | value    | integer |         |       |
    ''')
    dtype = manifest.models['example/Data'].properties['value'].dtype
    env = SqlResultBuilder(context)
    env.call('cast', dtype, value)


@pytest.mark.parametrize('value', [
    1.1,
    Decimal(1.1),
])
def test_cast_integer_error(rc: RawConfig, value):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property | type    | prepare | level | title
    example                  |         |         |       |
      |   |   | Data         |         |         |       |
      |   |   |   | value    | integer |         |       |
    ''')
    dtype = manifest.models['example/Data'].properties['value'].dtype
    env = SqlResultBuilder(context)
    with pytest.raises(UnableToCast) as e:
        env.call('cast', dtype, value)
    assert e.value.message == 'Unable to cast 1.1 to integer type.'
