from typing import List

import pytest

from spinta import commands
from spinta.auth import AdminToken
from spinta.formats.helpers import get_model_tabular_header
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.request import make_get_request
from spinta.components import Action
from spinta.components import UrlParams
from spinta.components import Version
from spinta.core.config import RawConfig


@pytest.mark.parametrize('query, header', [
    ('', ['_type', '_id', '_revision', 'name', 'country._id']),
    ('count()', ['count()']),
    ('select(_id)', ['_id']),
    ('select(_id, country)', ['_id', 'country._id']),
    ('select(_id, country._id)', ['_id', 'country._id']),
    ('select(_id, country._id, country.name)', ['_id', 'country._id', 'country.name']),
])
def test_get_model_tabular_header(rc: RawConfig, query: str, header: List[str]):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''')
    context.set('auth.token', AdminToken())
    model = manifest.models['example/City']
    request = make_get_request(model.name, query)
    params = commands.prepare(context, UrlParams(), Version(), request)
    action = Action.SEARCH if query else Action.GETALL
    assert get_model_tabular_header(context, model, action, params) == header
