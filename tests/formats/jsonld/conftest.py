import pytest

from tests.formats.jsonld.helpers import MODES


@pytest.fixture(params=MODES)
def mode(request):
    """Every RDF check runs twice: `inline` (the default, 10.16) and `url` (`jsonld_context_url` set)."""
    return request.param
