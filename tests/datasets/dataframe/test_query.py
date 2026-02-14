import pandas as pd
import dask.dataframe as dd
import pytest

from spinta import commands, spyna
from spinta.auth import AdminToken
from spinta.core.enums import Mode
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.helpers import get_enum_filters, get_ref_filters
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.utils import create_empty_backend
from spinta.ufuncs.helpers import merge_formulas


def _build(
    rc: RawConfig,
    manifest: str,
    model_name: str,
    *,
    data: list[dict],
    query: str = "",
) -> dd.DataFrame:
    """Build and execute a Dask query, returning the result DataFrame.

    Args:
        rc: RawConfig fixture
        manifest: Tabular manifest string
        model_name: Model name to query (e.g., "example/Country")
        data: List of dicts representing DataFrame rows
        query: Optional Spyna query string

    Returns:
        Filtered Dask DataFrame
    """
    context, manifest = load_manifest_and_context(rc, manifest, mode=Mode.external)
    context.set("auth.token", AdminToken())
    model = commands.get_model(context, manifest, model_name)
    backend = model.backend
    if backend is None or backend.name == "":
        backend = create_empty_backend(context, "dask")

    # Create Dask DataFrame from test data
    pdf = pd.DataFrame(data)
    dataframe = dd.from_pandas(pdf, npartitions=1)

    # Parse and merge query with prepare formulas
    query_expr = asttoexpr(spyna.parse(query))
    query_expr = merge_formulas(query_expr, model.external.prepare)

    # Create and initialize query builder
    builder = backend.query_builder_class(context)
    builder.update(model=model)
    env = builder.init(backend, dataframe, params={})

    # Merge enum and ref filters
    query_expr = merge_formulas(query_expr, get_enum_filters(context, model))
    query_expr = merge_formulas(query_expr, get_ref_filters(context, model))

    # Resolve and execute query
    expr = env.resolve(query_expr)
    where = env.execute(expr)
    result = env.build(where)
    return result


def test_select_all_columns(rc: RawConfig):
    """Test that all accessible columns are selected."""
    result = _build(
        rc,
        """
    d | r | b | m | property | type   | ref  | source  | access
    example                  |        |      |         |
      | data                 | dask   |      |         |
      |   |   | Country      |        | code | COUNTRY |
      |   |   |   | code     | string |      | CODE    | open
      |   |   |   | name     | string |      | NAME    | open
    """,
        "example/Country",
        data=[
            {"CODE": "lt", "NAME": "Lietuva"},
            {"CODE": "lv", "NAME": "Latvija"},
            {"CODE": "ee", "NAME": "Estija"},
        ],
    )

    # Convert to pandas for easier assertion
    pdf = result.compute()
    assert len(pdf) == 3
    assert list(pdf["CODE"]) == ["lt", "lv", "ee"]
    assert list(pdf["NAME"]) == ["Lietuva", "Latvija", "Estija"]


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_filter_with_prepare(rc: RawConfig):
    """Test filtering with prepare expression (like code='lt')."""
    result = _build(
        rc,
        """
    d | r | b | m | property | type   | ref  | source  | prepare   | access
    example                  |        |      |         |           |
      | data                 | dask   |      |         |           |
      |   |   | Country      |        | code | COUNTRY | code='lt' |
      |   |   |   | code     | string |      | CODE    |           | open
      |   |   |   | name     | string |      | NAME    |           | open
    """,
        "example/Country",
        data=[
            {"CODE": "lt", "NAME": "Lietuva"},
            {"CODE": "lv", "NAME": "Latvija"},
            {"CODE": "ee", "NAME": "Estija"},
        ],
    )

    pdf = result.compute()
    assert len(pdf) == 1
    assert list(pdf["CODE"]) == ["lt"]
    assert list(pdf["NAME"]) == ["Lietuva"]


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_basic_query_filter(rc: RawConfig):
    """Test filtering with query parameter."""
    result = _build(
        rc,
        """
    d | r | b | m | property | type   | ref  | source  | access
    example                  |        |      |         |
      | data                 | dask   |      |         |
      |   |   | Country      |        | code | COUNTRY |
      |   |   |   | code     | string |      | CODE    | open
      |   |   |   | name     | string |      | NAME    | open
    """,
        "example/Country",
        data=[
            {"CODE": "lt", "NAME": "Lietuva"},
            {"CODE": "lv", "NAME": "Latvija"},
            {"CODE": "ee", "NAME": "Estija"},
        ],
        query="code='lv'",
    )

    pdf = result.compute()
    assert len(pdf) == 1
    assert list(pdf["CODE"]) == ["lv"]
    assert list(pdf["NAME"]) == ["Latvija"]


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_filter_and_query_combined(rc: RawConfig):
    """Test that prepare and query filters are combined with AND."""
    result = _build(
        rc,
        """
    d | r | b | m | property | type   | ref  | source  | prepare   | access
    example                  |        |      |         |           |
      | data                 | dask   |      |         |           |
      |   |   | Country      |        | code | COUNTRY | code='lt' |
      |   |   |   | code     | string |      | CODE    |           | open
      |   |   |   | name     | string |      | NAME    |           | open
    """,
        "example/Country",
        data=[
            {"CODE": "lt", "NAME": "Lietuva"},
            {"CODE": "lv", "NAME": "Latvija"},
            {"CODE": "ee", "NAME": "Estija"},
        ],
        # This query filters for 'lv', but prepare filters for 'lt'
        # So the result should be empty (AND of conflicting conditions)
        query="code='lv'",
    )

    pdf = result.compute()
    assert len(pdf) == 0


manifest_2 = """
    d | r | b | m | property | type     | ref     | source          | prepare          | access
    example                  |          |         |                 |                  |
      | data                 | dask     |         |                 |                  |
      |   |                  |          |         |                 |                  |
      |   |   | Country      |          | code    | COUNTRY         | code='lt'        |
      |   |   |   | code     | string   |         | CODE            |                  | open
      |   |   |   | name     | string   |         | NAME            |                  | open
      |   |                  |          |         |                 |                  |
      |   |   | City         |          | id      | CITY            |                  |
      |   |   |   | id       | integer  |         | ID              |                  | open
      |   |   |   | name     | string   |         | NAME            |                  | open
      |   |   |   | country  | ref      | Country | Country.code    |                  | open
"""


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_manifest_2_country_with_prepare_filter(rc: RawConfig):
    """Test Country model from manifest_2 with prepare filter (code='lt')."""
    result = _build(
        rc,
        manifest_2,
        "example/Country",
        data=[
            {"CODE": "lt", "NAME": "Lietuva"},
            {"CODE": "lv", "NAME": "Latvija"},
            {"CODE": "ee", "NAME": "Estija"},
        ],
    )

    pdf = result.compute()
    # Only 'lt' should be returned due to prepare filter
    assert len(pdf) == 1
    assert list(pdf["CODE"]) == ["lt"]
    assert list(pdf["NAME"]) == ["Lietuva"]


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_manifest_2_country_with_query_and_prepare(rc: RawConfig):
    """Test that Country prepare filter and query filter work together."""
    result = _build(
        rc,
        manifest_2,
        "example/Country",
        data=[
            {"CODE": "lt", "NAME": "Lietuva"},
            {"CODE": "lv", "NAME": "Latvija"},
            {"CODE": "ee", "NAME": "Estija"},
        ],
        query="name='Lietuva'",
    )

    pdf = result.compute()
    # Both prepare (code='lt') and query (name='Lietuva') should match
    assert len(pdf) == 1
    assert list(pdf["CODE"]) == ["lt"]
    assert list(pdf["NAME"]) == ["Lietuva"]


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_manifest_2_country_conflicting_filters(rc: RawConfig):
    """Test that conflicting prepare and query filters return empty result."""
    result = _build(
        rc,
        manifest_2,
        "example/Country",
        data=[
            {"CODE": "lt", "NAME": "Lietuva"},
            {"CODE": "lv", "NAME": "Latvija"},
            {"CODE": "ee", "NAME": "Estija"},
        ],
        query="code='lv'",
    )

    pdf = result.compute()
    # prepare filter is code='lt' but query is code='lv', so no results
    assert len(pdf) == 0


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_city_with_ref_filter(rc: RawConfig):
    """Test that City inherits Country's prepare filter (code='lt') via ref."""
    result = _build(
        rc,
        manifest_2,
        "example/City",
        data=[
            {"ID": 1, "NAME": "Vilnius", "Country.code": "lt"},
            {"ID": 2, "NAME": "Kaunas", "Country.code": "lt"},
            {"ID": 3, "NAME": "Riga", "Country.code": "lv"},
            {"ID": 4, "NAME": "Tallinn", "Country.code": "ee"},
        ],
    )

    pdf = result.compute()
    # Only cities with country code='lt' should be returned
    # due to Country model's prepare filter
    assert len(pdf) == 2
    assert list(pdf["NAME"]) == ["Vilnius", "Kaunas"]


manifest_3 = """
    d | r | b | m | property | type     | ref     | source          | prepare          | access
    example                  |          |         |                 |                  |
      | data                 | dask     |         |                 |                  |
      |   |                  |          |         |                 |                  |
      |   |   | Country      |          | code    | COUNTRY         | code='lt'        |
      |   |   |   | code     | string   |         | CODE            |                  | open
      |   |   |   | name     | string   |         | NAME            |                  | open
      |   |                  |          |         |                 |                  |
      |   |   | City         |          | name    | CITY            |                  |
      |   |   |   | name     | string   |         | NAME            |                  | open
      |   |   |   | country  | ref      | Country |                 |                  | open
"""


@pytest.mark.skip(reason="Filters expressions not yet supported in Dask queries")
def test_city_ref_filter_with_no_source_on_filtered_property(rc: RawConfig):
    """Test ref filter when the ref property has no source.

    - Country has prepare filter code='lt'
    - City.country ref has NO SOURCE
    - For refs without source, data column uses {prop_name}.{refprop_name} convention
    """
    result = _build(
        rc,
        manifest_3,
        "example/City",
        data=[
            {"NAME": "Vilnius", "country.code": "lt"},
            {"NAME": "Kaunas", "country.code": "lt"},
            {"NAME": "Riga", "country.code": "lv"},
            {"NAME": "Tallinn", "country.code": "ee"},
        ],
    )

    pdf = result.compute()
    # Only cities with country.code='lt' should be returned (due to Country's filter)
    assert len(pdf) == 2
    assert list(pdf["NAME"]) == ["Vilnius", "Kaunas"]
