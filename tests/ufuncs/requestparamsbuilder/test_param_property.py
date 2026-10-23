"""`eq()` on a source-less property linked to a resource param (`param()`) is a request-time value, not a row filter."""

from urllib.parse import parse_qs, urlparse

import pytest
from responses import GET, RequestsMock

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest

XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<GATVES>
  <GATVE><GAT_ID>1</GAT_ID><VARDAS_K>Vilniaus</VARDAS_K></GATVE>
  <GATVE><GAT_ID>2</GAT_ID><VARDAS_K>Kauno</VARDAS_K></GATVE>
</GATVES>
"""

ENDPOINT = "http://example.com/addresses/Street"


def _app(rc: RawConfig, responses: RequestsMock):
    responses.add(GET, ENDPOINT, body=XML, status=200, content_type="application/xml")
    return _client(rc)


def _client(rc: RawConfig):
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {ENDPOINT}     |               |
          |   |   |              | param    | client | CLIENT_NAME    | query('TEST') |
          |   |   |              | param    | type   | DATA_TYPE      | query('GAT')  |
          |   |   | Street       |          | id     | /GATVES/GATVE  |               |
          |   |   |   | id       | string   |        | GAT_ID         |               | open
          |   |   |   | name     | string   |        | VARDAS_K       |               | open
          |   |   |   | type     | string   |        |                | param()       | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall", "search"])
    return app


def _sent_query(responses: RequestsMock) -> dict:
    url = responses.calls[-1].request.url
    return parse_qs(urlparse(url).query, keep_blank_values=True)


def test_top_level_linked_eq_is_sent_as_query_param_and_not_filtered(rc: RawConfig, responses: RequestsMock):
    app = _app(rc, responses)

    resp = app.get("/example/Street?type='ADR'")

    assert resp.status_code == 200
    assert _sent_query(responses) == {"CLIENT_NAME": ["TEST"], "DATA_TYPE": ["ADR"]}
    assert listdata(resp, "id", "name", sort=False) == [("1", "Vilniaus"), ("2", "Kauno")]


@pytest.mark.parametrize(
    "query, reason",
    [
        ("type='ADR'&type='GAT'", "more than once"),
        ("or(type='ADR',name='Kauno')", "inside or()"),
        ("not(type='ADR')", "inside not()"),
    ],
)
def test_linked_property_used_more_than_once_or_nested_is_rejected(rc: RawConfig, query: str, reason: str):
    app = _client(rc)

    resp = app.get(f"/example/Street?{query}")

    assert resp.status_code == 400
    error = resp.json()["errors"][0]
    assert error["code"] == "ParamPropertyNotAllowed"
    assert reason in error["message"]
