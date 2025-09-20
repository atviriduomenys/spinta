from pathlib import Path

import pytest
import sqlalchemy as sa

from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.client import create_test_client
from spinta.testing.config import configure
from spinta.testing.data import listdata
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest


@pytest.mark.models(
    "backends/mongo/{}",
    "backends/postgres/{}",
)
def test_schema_loader(model, app):
    model_org = model.format("Org")
    model_country = model.format("Country")

    app.authmodel(model_org, ["insert"])
    app.authmodel(model_country, ["insert"])

    country = app.post(
        f"/{model_country}",
        json={
            "code": "lt",
            "title": "Lithuania",
        },
    ).json()
    org = app.post(
        f"/{model_org}",
        json={
            "title": "My Org",
            "govid": "0042",
            "country": {"_id": country["_id"]},
        },
    ).json()

    assert country == {
        "_id": country["_id"],
        "_type": model_country,
        "_revision": country["_revision"],
        "code": "lt",
        "continent": None,
        "title": "Lithuania",
    }
    assert org == {
        "_id": org["_id"],
        "_type": model_org,
        "_revision": org["_revision"],
        "country": {"_id": country["_id"]},
        "govid": "0042",
        "title": "My Org",
    }

    app.authorize(["uapi:/:getone"])

    resp = app.get(f"/{model_org}/{org['_id']}")
    data = resp.json()
    revision = data["_revision"]
    assert data == {
        "_id": org["_id"],
        "govid": "0042",
        "title": "My Org",
        "country": {"_id": country["_id"]},
        "_type": model_org,
        "_revision": revision,
    }

    resp = app.get(f"/{model_country}/{country['_id']}")
    data = resp.json()
    revision = data["_revision"]
    assert data == {
        "_id": country["_id"],
        "code": "lt",
        "continent": None,
        "title": "Lithuania",
        "_type": model_country,
        "_revision": revision,
    }


@pytest.mark.models(
    "backends/mongo/Report",
    "backends/postgres/Report",
)
def test_nested(model, app):
    app.authmodel(model, ["insert", "getone"])

    resp = app.post(f"/{model}", json={"_type": model, "notes": [{"note": "foo"}]})
    assert resp.status_code == 201
    data = resp.json()
    id_ = data["_id"]
    revision = data["_revision"]

    data = app.get(f"/{model}/{id_}").json()
    assert data["_id"] == id_
    assert data["_type"] == model
    assert data["_revision"] == revision
    assert data["notes"] == [{"note": "foo", "note_type": None, "create_date": None}]


def test_root(context, rc: RawConfig, tmp_path: Path):
    rc = configure(
        context,
        rc,
        None,
        tmp_path / "manifest.csv",
        """
    d | r | b | m | property | type   | title
    datasets/gov/vpt/old     |        | Old data
      | sql                  | sql    |
      |   |   | Country      |        |
      |   |   |   | name     | string |
    datasets/gov/vpt/new     |        | New data
      | sql                  | sql    |
      |   |   | Country      |        |
      |   |   |   | name     | string |
    """,
    )
    app = create_test_client(
        rc,
        config={
            "root": "datasets/gov/vpt",
        },
    )
    app.authorize(["uapi:/:getall"])
    assert listdata(app.get("/"), "name") == [
        "datasets/gov/vpt/new/:ns",
        "datasets/gov/vpt/old/:ns",
    ]


def test_resource_backends(
    context,
    rc: RawConfig,
    tmp_path: Path,
    sqlite: Sqlite,
):
    # Prepare source data.
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    sqlite.write(
        "COUNTRY",
        [
            {"ID": 1, "NAME": "Lithuania"},
            {"ID": 2, "NAME": "Latvia"},
        ],
    )

    create_tabular_manifest(
        context,
        tmp_path / "old.csv",
        striptable("""
    d | r | b | m | property | type    | ref | source  | access | title
    datasets/gov/vpt/old     |         |     |         |        | Old data
      | sql                  |         | old |         |        |
      |   |   | Country      |         | id  | COUNTRY |        |
      |   |   |   | id       | integer |     | ID      | open   |
      |   |   |   | name     | string  |     | NAME    | open   |
    """),
    )

    create_tabular_manifest(
        context,
        tmp_path / "new.csv",
        striptable("""
    d | r | b | m | property | type    | ref  | source  | access | title
    datasets/gov/vpt/new     |         |      |         |        | New data
      | sql                  |         | new  |         |        |
      |   |   | Country      |         | id   | COUNTRY |        |
      |   |   |   | id       | integer |      | ID      | open   |
      |   |   |   | name     | string  |      | NAME    | open   |
    """),
    )

    app = create_test_client(
        rc,
        config={
            "backends": {
                "default": {
                    "type": "memory",
                },
                "old": {
                    "type": "sql",
                    "dsn": sqlite.dsn,
                },
                "new": {
                    "type": "sql",
                    "dsn": sqlite.dsn,
                },
            },
            "manifests": {
                "default": {
                    "type": "backend",
                    "backend": "default",
                    "sync": ["old", "new"],
                    "mode": "external",
                },
                "old": {
                    "type": "tabular",
                    "path": str(tmp_path / "old.csv"),
                    "backend": "old",
                },
                "new": {
                    "type": "tabular",
                    "path": str(tmp_path / "new.csv"),
                    "backend": "new",
                },
            },
            "manifest": "default",
        },
    )

    app.authorize(["uapi:/:getall"])

    assert listdata(app.get("/datasets/gov/vpt/old/Country")) == [
        (1, "Lithuania"),
        (2, "Latvia"),
    ]

    assert listdata(app.get("/datasets/gov/vpt/new/Country")) == [
        (1, "Lithuania"),
        (2, "Latvia"),
    ]
