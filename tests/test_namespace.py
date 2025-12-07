import hashlib
from pathlib import Path
from typing import Tuple

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.testing.client import TestClient
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.data import pushdata
from spinta.testing.manifest import bootstrap_manifest, load_manifest_and_context
from spinta.types.namespace import sort_models_by_refs
from spinta.utils.data import take


def _create_data(app: TestClient, ns: str) -> Tuple[str, str]:
    continent = ns + "/Continent"
    country = ns + "/Country"
    capital = ns + "/Capital"

    app.authmodel(continent, ["create"])
    app.authmodel(country, ["create"])
    app.authmodel(capital, ["create"])

    eu = take(
        "_id",
        pushdata(
            app,
            continent,
            {
                "title": "Europe",
            },
        ),
    )
    lt = take(
        "_id",
        pushdata(
            app,
            country,
            {
                "code": "lt",
                "title": "Lithuania",
                "continent": {"_id": eu},
            },
        ),
    )
    pushdata(
        app,
        capital,
        {
            "title": "Vilnius",
            "country": {"_id": lt},
        },
    )

    return eu, lt


@pytest.mark.models("datasets/backends/postgres/dataset")
@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_getall(
    model: str,
    app: TestClient,
    scopes: list,
):
    eu, lt = _create_data(app, model)
    app.authorize(scopes)

    resp = app.get("/datasets/backends/postgres/dataset/:all")
    assert listdata(resp, full=True) == [
        {"code": "lt", "continent._id": eu, "title": "Lithuania"},
        {"country._id": lt, "title": "Vilnius"},
        {"title": "Europe"},
    ]

    resp = app.get("/datasets/csv/:all")
    assert listdata(resp) == []


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.models("datasets/backends/postgres/dataset")
@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_getall_ns(
    model,
    app,
    scopes: list,
):
    _create_data(app, model)
    app.authorize(scopes)

    resp = app.get("/datasets/backends/postgres/dataset/:ns/:all")
    assert listdata(resp, "name") == [
        "datasets/backends/postgres/dataset/Capital",
        "datasets/backends/postgres/dataset/Continent",
        "datasets/backends/postgres/dataset/Country",
        "datasets/backends/postgres/dataset/Org",
        "datasets/backends/postgres/dataset/Report",
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_ns_titles(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    scopes: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref | title               | description
    datasets                 | ns     |     | All datasets        | All external datasets.
    datasets/gov             | ns     |     | Government datasets | All external government datasets.
                             |        |     |                     |
    datasets/gov/vpt/new     |        |     | New data            | Data from a new database.
      | resource             |        |     |                     |
      |   |   | Country      |        |     | Countries           | All countries.
      |   |   |   | name     | string |     | Country name        | Name of a country.
      |   |   | City         |        |     | Cities              | All cities.
      |   |   |   | name     | string |     | City name           | Name of a city.
    """,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        full_load=True,
    )
    app = create_test_client(context, scope=scopes)
    assert listdata(app.get("/:ns"), "title", "description") == [
        ("All datasets", "All external datasets."),
    ]
    assert listdata(app.get("/:ns/:all"), "name", "title", "description") == [
        ("datasets/:ns", "All datasets", "All external datasets."),
        ("datasets/gov/:ns", "Government datasets", "All external government datasets."),
        ("datasets/gov/vpt/:ns", "", ""),
        ("datasets/gov/vpt/new/:ns", "New data", "Data from a new database."),
        ("datasets/gov/vpt/new/City", "Cities", "All cities."),
        ("datasets/gov/vpt/new/Country", "Countries", "All countries."),
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize("scopes", [["spinta_getall"], ["uapi:/:getall"]])
def test_ns_titles_bare_models(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    scopes: list,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property                 | type   | ref | title               | description
    datasets                                 | ns     |     | All datasets        | All external datasets.
    datasets/gov                             | ns     |     | Government datasets | All external government datasets.
                                             |        |     |                     |
    datasets/gov/vpt/new                     |        |     | New data            | Data from a new database.
                                             |        |     |                     |               
      |   |   | datasets/gov/vpt/new/Country |        |     | Countries           | All countries.
      |   |   |   | name                     | string |     | Country name        | Name of a country.
      |   |   | datasets/gov/vpt/new/City    |        |     | Cities              | All cities.
      |   |   |   | name                     | string |     | City name           | Name of a city.
    """,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        full_load=True,
    )
    app = create_test_client(context, scope=scopes)
    assert listdata(app.get("/:ns"), "title", "description") == [
        ("All datasets", "All external datasets."),
    ]
    assert listdata(app.get("/:ns/:all"), "name", "title", "description") == [
        ("datasets/:ns", "All datasets", "All external datasets."),
        ("datasets/gov/:ns", "Government datasets", "All external government datasets."),
        ("datasets/gov/vpt/:ns", "", ""),
        ("datasets/gov/vpt/new/:ns", "New data", "Data from a new database."),
        ("datasets/gov/vpt/new/City", "Cities", "All cities."),
        ("datasets/gov/vpt/new/Country", "Countries", "All countries."),
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_sort_models_by_refs(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
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
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )

    models = sort_models_by_refs(commands.get_models(context, manifest).values())
    names = [model.name for model in models]
    assert names == [
        "datasets/gov/example/City",
        "datasets/gov/example/Country",
        "datasets/gov/example/Continent",
    ]


@pytest.mark.manifests("internal_sql", "csv")
def test_sort_models_by_ref_with_base(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type    | ref     | access
    datasets/basetest        |         |         |
      |   |   | Place        |         | id      |
      |   |   |   | id       | integer |         | open
      |   |   |   | name     | string  |         | open
      |   |   |   |          |         |         |
      |   | Place            |         | name    |
      |   |   |   |          |         |         |
      |   |   | Country      |         | id      |
      |   |   |   | id       | integer |         | open
      |   |   |   | name     |         |         | open
      |   |   |   |          |         |         |
      |   |   | City         |         | id      |
      |   |   |   | id       | integer |         | open
      |   |   |   | name     |         |         | open
      |   |   |   | country  | ref     | Country | open
    """,
        manifest_type=manifest_type,
        tmp_path=tmp_path,
    )

    models = sort_models_by_refs(commands.get_models(context, manifest).values())
    names = [model.name for model in models]
    assert names == [
        "datasets/basetest/City",
        "datasets/basetest/Country",
        "datasets/basetest/Place",
    ]
