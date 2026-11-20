import pytest

from spinta.manifests.rdf import helpers


@pytest.fixture(autouse=True)
def no_remote_definitions(monkeypatch):
    """Unit tests must not dereference IRIs, tests that need it patch `_fetch_graph`."""
    monkeypatch.setattr(helpers, "_fetch_graph", lambda iri: None)
