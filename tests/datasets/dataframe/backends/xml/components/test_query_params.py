"""Resource `query()` params are sent to the data source (atviriduomenys/spinta#2066)."""

from urllib.parse import parse_qs, urlparse

import pytest
from responses import GET, RequestsMock

from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import UnlinkedQueryParam
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest

XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<GATVES>
  <GATVE><GAT_ID>1</GAT_ID><VARDAS_K>Vilniaus</VARDAS_K></GATVE>
  <GATVE><GAT_ID>2</GAT_ID><VARDAS_K>Kauno</VARDAS_K></GATVE>
</GATVES>
"""


def _mock(responses: RequestsMock, url: str) -> None:
    responses.add(GET, url, body=XML, status=200, content_type="application/xml")


def _sent_query(responses: RequestsMock) -> dict:
    """Query string of the last request sent to the data source."""
    url = responses.calls[-1].request.url
    return {k: v for k, v in parse_qs(urlparse(url).query, keep_blank_values=True).items()}


def test_query_defaults_are_sent_without_linked_properties(rc: RawConfig, responses: RequestsMock):
    endpoint = "http://example.com/addresses"
    _mock(responses, endpoint)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {endpoint}     |               |
          |   |   |              | param    | client | CLIENT_NAME    | query('TEST') |
          |   |   |              |          | type   | DATA_TYPE      | query('GAT')  |
          |   |   | Street       |          | id     | /GATVES/GATVE  |               |
          |   |   |   | id       | string   |        | GAT_ID         |               | open
          |   |   |   | name     | string   |        | VARDAS_K       |               | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall"])

    resp = app.get("/example/Street")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", sort=False) == [("1", "Vilniaus"), ("2", "Kauno")]
    assert _sent_query(responses) == {"CLIENT_NAME": ["TEST"], "DATA_TYPE": ["GAT"]}


def test_query_default_is_overridden_only_by_property_linked_with_param(rc: RawConfig, responses: RequestsMock):
    endpoint = "http://example.com/addresses/Street"
    _mock(responses, endpoint)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {endpoint}     |               |
          |   |   |              | param    | client | CLIENT_NAME    | query('TEST') |
          |   |   |              |          | type   | DATA_TYPE      | query('GAT')  |
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

    # `type` is linked to `type` param -> URL value overrides default.
    resp = app.get("/example/Street?type='ADR'")
    assert resp.status_code == 200, resp.json()
    assert _sent_query(responses) == {"CLIENT_NAME": ["TEST"], "DATA_TYPE": ["ADR"]}

    resp = app.get("/example/Street")
    assert resp.status_code == 200
    assert _sent_query(responses) == {"CLIENT_NAME": ["TEST"], "DATA_TYPE": ["GAT"]}


def test_url_value_for_param_without_linked_property_is_an_error(rc: RawConfig, responses: RequestsMock):
    # No mock: the request must not reach the data source.
    endpoint = "http://example.com/addresses/Street"

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {endpoint}     |               |
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

    calls = len(responses.calls)
    resp = app.get("/example/Street?client='HACKED'")
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "PropertyNotFound"
    assert len(responses.calls) == calls


def test_query_without_default_requires_value_from_url(rc: RawConfig, responses: RequestsMock):
    endpoint = "http://example.com/addresses/Street"
    _mock(responses, endpoint)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {endpoint}     |               |
          |   |   |              | param    | type   | DATA_TYPE      | query()       |
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

    resp = app.get("/example/Street?type='ADR'")
    assert resp.status_code == 200
    assert _sent_query(responses) == {"DATA_TYPE": ["ADR"]}

    calls = len(responses.calls)
    resp = app.get("/example/Street")
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "QueryParamNotFound"
    assert len(responses.calls) == calls


def test_header_params_are_not_sent_as_query_params(rc: RawConfig, responses: RequestsMock):
    endpoint = "http://example.com/addresses/Street"
    _mock(responses, endpoint)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {endpoint}     |               |
          |   |   |              | param    | client | CLIENT_NAME    | query('TEST') |
          |   |   |              | param    | token  | X-Token        | header('abc') |
          |   |   | Street       |          | id     | /GATVES/GATVE  |               |
          |   |   |   | id       | string   |        | GAT_ID         |               | open
          |   |   |   | name     | string   |        | VARDAS_K       |               | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall"])

    resp = app.get("/example/Street")

    assert resp.status_code == 200
    assert _sent_query(responses) == {"CLIENT_NAME": ["TEST"]}
    assert responses.calls[-1].request.headers["X-Token"] == "abc"


def test_param_with_explicit_name_links_property_to_param(rc: RawConfig, responses: RequestsMock):
    endpoint = "http://example.com/addresses/Street"
    _mock(responses, endpoint)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property    | type     | ref    | source         | prepare       | access
        example                     | dataset  |        |                |               |
          | resource                | dask/xml |        | {endpoint}     |               |
          |   |   |                 | param    | client | CLIENT_NAME    | query('TEST') |
          |   |   | Street          |          | id     | /GATVES/GATVE  |               |
          |   |   |   | id          | string   |        | GAT_ID         |               | open
          |   |   |   | name        | string   |        | VARDAS_K       |               | open
          |   |   |   | client_prop | string   |        |                | param('client') | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall", "search"])

    resp = app.get("/example/Street?client_prop='CUSTOM'")
    assert resp.status_code == 200
    assert _sent_query(responses) == {"CLIENT_NAME": ["CUSTOM"]}


def test_param_with_bind_name_links_property_to_param(rc: RawConfig, responses: RequestsMock):
    endpoint = "http://example.com/addresses/Street"
    _mock(responses, endpoint)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property    | type     | ref    | source         | prepare       | access
        example                     | dataset  |        |                |               |
          | resource                | dask/xml |        | {endpoint}     |               |
          |   |   |                 | param    | client | CLIENT_NAME    | query('TEST') |
          |   |   | Street          |          | id     | /GATVES/GATVE  |               |
          |   |   |   | id          | string   |        | GAT_ID         |               | open
          |   |   |   | name        | string   |        | VARDAS_K       |               | open
          |   |   |   | client_prop | string   |        |                | param(client) | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall", "search"])

    resp = app.get("/example/Street?client_prop='CUSTOM_BIND'")
    assert resp.status_code == 200
    assert _sent_query(responses) == {"CLIENT_NAME": ["CUSTOM_BIND"]}


def test_url_value_overrides_query_default_in_output_rows(rc: RawConfig, responses: RequestsMock):
    endpoint = "http://example.com/addresses/Street"
    rows_by_type = {
        "GAT": b"<GATVES><GATVE><GAT_ID>1</GAT_ID><VARDAS_K>Vilniaus</VARDAS_K></GATVE></GATVES>",
        "ADR": b"<GATVES><GATVE><GAT_ID>2</GAT_ID><VARDAS_K>Kauno</VARDAS_K></GATVE></GATVES>",
    }

    def callback(request):
        data_type = parse_qs(urlparse(request.url).query).get("DATA_TYPE", ["GAT"])[0]
        return (200, {"Content-Type": "application/xml"}, rows_by_type[data_type])

    responses.add_callback(GET, endpoint, callback=callback, content_type="application/xml")

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {endpoint}     |               |
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

    # No URL value -> manifest default `GAT` is used, and its rows are returned.
    resp = app.get("/example/Street")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", sort=False) == [("1", "Vilniaus")]

    # URL value overrides the `GAT` default -> `ADR` is sent and its rows are returned.
    resp = app.get("/example/Street?type='ADR'")
    assert resp.status_code == 200
    assert listdata(resp, "id", "name", sort=False) == [("2", "Kauno")]
    assert _sent_query(responses) == {"CLIENT_NAME": ["TEST"], "DATA_TYPE": ["ADR"]}


def test_query_default_is_shown_in_output_for_linked_property(rc: RawConfig, responses: RequestsMock):
    """`param()` property shows the `query()` default in output, and the URL value when given."""
    endpoint = "http://example.com/addresses/changes"
    _mock(responses, endpoint)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref    | source         | prepare             | access
        example                  | dataset  |        |                |                     |
          | resource             | dask/xml |        | {endpoint}     |                     |
          |   |   |              | param    | client | CLIENT_NAME    | query('SPINTA_TEST') |
          |   |   | Changes      |          | id     | /GATVES/GATVE  |                     |
          |   |   |   | id       | string   |        | GAT_ID         |                     | open
          |   |   |   | name     | string   |        | VARDAS_K       |                     | open
          |   |   |   | client   | string   |        |                | param()             | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Changes", ["getall", "search"])

    # No URL value -> `query()` default is sent to the source and shown in the output.
    resp = app.get("/example/Changes")
    assert resp.status_code == 200, resp.json()
    assert _sent_query(responses) == {"CLIENT_NAME": ["SPINTA_TEST"]}
    assert listdata(resp, "id", "name", "client", sort=False) == [
        ("1", "Vilniaus", "SPINTA_TEST"),
        ("2", "Kauno", "SPINTA_TEST"),
    ]

    # URL value replaces the default both in the outgoing request and in the output.
    resp = app.get("/example/Changes?client=test")
    assert resp.status_code == 200, resp.json()
    assert _sent_query(responses) == {"CLIENT_NAME": ["test"]}
    assert listdata(resp, "id", "name", "client", sort=False) == [
        ("1", "Vilniaus", "test"),
        ("2", "Kauno", "test"),
    ]


CLASH_ENDPOINT = "http://example.com/addresses/Street"


def _clash_manifest(source_query: str) -> str:
    return f"""
        d | r | b | m | property | type     | ref    | source                              | prepare       | access
        example                  | dataset  |        |                                     |               |
          | resource             | dask/xml |        | {CLASH_ENDPOINT}?{source_query}     |               |
          |   |   |              | param    | client | CLIENT_NAME                         | query('TEST') |
          |   |   | Street       |          | id     | /GATVES/GATVE                       |               |
          |   |   |   | id       | string   |        | GAT_ID                              |               | open
          |   |   |   | name     | string   |        | VARDAS_K                            |               | open
    """


def test_source_query_key_is_recorded_on_load(rc: RawConfig):
    context, manifest = prepare_manifest(rc, _clash_manifest("CLIENT_NAME=ABC"), mode=Mode.external)

    resource = commands.get_dataset(context, manifest, "example").resources["resource"]
    assert resource.url.source_query == {"CLIENT_NAME"}
    assert resource.url.query == {"CLIENT_NAME": "TEST"}


# TODO(#2075): documents current duplicate-key behaviour when the check is not enabled.
def test_clashing_key_sends_both_values_with_check_flag_off(rc: RawConfig, responses: RequestsMock):
    # `spinta check` is not run, so the clash is not blocked: the request carries the key twice.
    _mock(responses, CLASH_ENDPOINT)
    context, manifest = prepare_manifest(rc, _clash_manifest("CLIENT_NAME=ABC"), mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall"])

    resp = app.get("/example/Street")

    assert resp.status_code == 200
    query = parse_qs(urlparse(responses.calls[-1].request.url).query)
    assert query["CLIENT_NAME"] == ["ABC", "TEST"]


def test_non_clashing_source_key_is_sent_with_query_default(rc: RawConfig, responses: RequestsMock):
    _mock(responses, CLASH_ENDPOINT)
    context, manifest = prepare_manifest(rc, _clash_manifest("CODE=ABC"), mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall"])

    resp = app.get("/example/Street")

    assert resp.status_code == 200
    assert _sent_query(responses) == {"CODE": ["ABC"], "CLIENT_NAME": ["TEST"]}


def test_query_without_default_and_without_linked_property_is_an_error(rc: RawConfig):
    endpoint = "http://example.com/addresses/Street"

    # Invalid manifest: rejected by `check` on load, the data source is never reached.
    with pytest.raises(UnlinkedQueryParam):
        prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type     | ref    | source         | prepare       | access
            example                  | dataset  |        |                |               |
              | resource             | dask/xml |        | {endpoint}     |               |
              |   |   |              | param    | type   | DATA_TYPE      | query()       |
              |   |   | Street       |          | id     | /GATVES/GATVE  |               |
              |   |   |   | id       | string   |        | GAT_ID         |               | open
              |   |   |   | name     | string   |        | VARDAS_K       |               | open
            """,
            mode=Mode.external,
        )


def test_query_without_default_must_be_linked_in_every_model_of_resource(rc: RawConfig):
    endpoint = "http://example.com/addresses/Street"

    # `Other` has no link to `DATA_TYPE`, so its requests could never get a value. Rejected on load.
    with pytest.raises(UnlinkedQueryParam):
        prepare_manifest(
            rc,
            f"""
            d | r | b | m | property | type     | ref    | source         | prepare       | access
            example                  | dataset  |        |                |               |
              | resource             | dask/xml |        | {endpoint}     |               |
              |   |   |              | param    | type   | DATA_TYPE      | query()       |
              |   |   | Street       |          | id     | /GATVES/GATVE  |               |
              |   |   |   | id       | string   |        | GAT_ID         |               | open
              |   |   |   | type     | string   |        |                | param()       | open
              |   |   | Other        |          | id     | /GATVES/GATVE  |               |
              |   |   |   | id       | string   |        | GAT_ID         |               | open
            """,
            mode=Mode.external,
        )


def test_query_default_is_sent_on_placeholder_url(rc: RawConfig, responses: RequestsMock):
    # The URL has a `{path}` placeholder, filled from a resource param (no `read()`), so `parametrize_bases()` takes
    # the placeholder branch that formats the URL from `resource.params`.
    _mock(responses, "http://example.com/addresses/Street")

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type     | ref    | source                        | prepare       | access
        example                  | dataset  |        |                               |               |
          | resource             | dask/xml |        | http://example.com/{path}     |               |
          |   |   |              | param    | path   | addresses/Street              |               |
          |   |   |              | param    | client | CLIENT_NAME                   | query('TEST') |
          |   |   | Street       |          | id     | /GATVES/GATVE                 |               |
          |   |   |   | id       | string   |        | GAT_ID                        |               | open
          |   |   |   | name     | string   |        | VARDAS_K                      |               | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall"])

    resp = app.get("/example/Street")

    assert resp.status_code == 200
    assert listdata(resp, "id", "name", sort=False) == [("1", "Vilniaus"), ("2", "Kauno")]
    assert responses.calls[-1].request.url.split("?")[0] == "http://example.com/addresses/Street"
    assert _sent_query(responses) == {"CLIENT_NAME": ["TEST"]}


def test_query_default_is_sent_on_every_page_of_placeholder_url(rc: RawConfig, responses: RequestsMock):
    # `{path}` is filled by `read().next`: the first page comes from the literal param value, every next page from
    # `parametrize_bases()` with `resolved_params`. Each of them must carry the `query()` default.
    responses.add(
        GET,
        "http://example.com/pages/page0.json",
        json={"page": {"next": "http://example.com/pages/page1.json", "name": "Page 0"}},
        status=200,
    )
    responses.add(GET, "http://example.com/pages/page1.json", json={"page": {"next": None, "name": "Page 1"}})

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type      | ref    | source                                | prepare     | access
        example/json             |           |        |                                       |             |
          | resource             | dask/json |        | {path}                                |             |
          |   |   |              | param     | path   | http://example.com/pages/page0.json   |             |
          |   |   |              |           |        | Page                                  | read().next |
          |   |   |              | param     | client | CLIENT_NAME                           | query('TEST') |
          |   |   | Page         |           | name   | page                                  |             |
          |   |   |   | name     | string    |        | name                                  |             | open
          |   |   |   | next     | uri       |        | next                                  |             | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/json/Page", ["getall"])

    resp = app.get("/example/json/Page")

    assert resp.status_code == 200, resp.json()
    assert listdata(resp, "name", sort=False) == ["Page 0", "Page 1"]
    urls = {call.request.url.split("?")[0] for call in responses.calls}
    assert urls == {"http://example.com/pages/page0.json", "http://example.com/pages/page1.json"}
    for call in responses.calls:
        assert parse_qs(urlparse(call.request.url).query) == {"CLIENT_NAME": ["TEST"]}


MULTI_ROW_ENDPOINT = "http://example.com/addresses/Street"


def _multi_row_manifest(data_type_query: str = "query()", type_code_query: str = "query()") -> str:
    # One resource param `type` with two `query()` rows, each with its own outgoing query name.
    return f"""
        d | r | b | m | property | type     | ref    | source         | prepare       | access
        example                  | dataset  |        |                |               |
          | resource             | dask/xml |        | {MULTI_ROW_ENDPOINT} |         |
          |   |   |              | param    | type   | DATA_TYPE      | {data_type_query} |
          |   |   |              |          |        | TYPE_CODE      | {type_code_query} |
          |   |   | Street       |          | id     | /GATVES/GATVE  |               |
          |   |   |   | id       | string   |        | GAT_ID         |               | open
          |   |   |   | name     | string   |        | VARDAS_K       |               | open
          |   |   |   | type     | string   |        |                | param()       | open
        """


def test_every_query_row_of_a_param_is_recorded(rc: RawConfig):
    # Both rows are recorded. Before, only the last row was kept, so `DATA_TYPE` looked unlinked and the manifest was
    # rejected with UnlinkedQueryParam, although `type` provides a value for it.
    context, manifest = prepare_manifest(rc, _multi_row_manifest(), mode=Mode.external)

    resource = commands.get_dataset(context, manifest, "example").resources["resource"]
    [param] = resource.params
    assert param.query_names == ["DATA_TYPE", "TYPE_CODE"]


def test_url_value_is_sent_for_every_query_row_of_a_param(rc: RawConfig, responses: RequestsMock):
    _mock(responses, MULTI_ROW_ENDPOINT)
    context, manifest = prepare_manifest(rc, _multi_row_manifest(), mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall", "search"])

    calls = len(responses.calls)
    resp = app.get("/example/Street")
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "QueryParamNotFound"
    assert len(responses.calls) == calls

    resp = app.get("/example/Street?type='ADR'")
    assert resp.status_code == 200, resp.json()
    assert _sent_query(responses) == {"DATA_TYPE": ["ADR"], "TYPE_CODE": ["ADR"]}


def test_query_defaults_of_every_row_of_a_param_are_sent(rc: RawConfig, responses: RequestsMock):
    _mock(responses, MULTI_ROW_ENDPOINT)
    context, manifest = prepare_manifest(
        rc,
        _multi_row_manifest(data_type_query="query('GAT')", type_code_query="query('X')"),
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Street", ["getall", "search"])

    resp = app.get("/example/Street")

    assert resp.status_code == 200, resp.json()
    assert _sent_query(responses) == {"DATA_TYPE": ["GAT"], "TYPE_CODE": ["X"]}
