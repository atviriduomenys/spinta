from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes


def _redirect_table_data(context: Context, model: str):
    store = context.get("store")
    model = commands.get_model(context, store.manifest, model)
    backend = model.backend
    assert isinstance(backend, PostgreSQL)

    redirect_table = backend.get_table(model, TableType.REDIRECT)
    with backend.begin() as conn:
        result = conn.execute(redirect_table.select())
        for row in result:
            yield {"_id": row["_id"], "redirect": row["redirect"]}


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_insert",
            "spinta_getall",
            "spinta_wipe",
            "spinta_changes",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_patch",
        ],
        [
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:wipe",
            "uapi:/:changes",
            "uapi:/:set_meta_fields",
            "uapi:/:move",
            "uapi:/:patch",
        ]
    ]
)
def test_move_delete_missing_revision(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type     | ref     | prepare                 | access | level
            datasets/redirect/delete   |          |         |                         |        |
              |   |   | Country        |          |         |                         |        |
              |   |   |   | id         | integer  |         |                         | open   |
              |   |   |   | name       | string   |         |                         | open   |
              |   |   | City           |          |         |                         |        |
              |   |   |   | id         | integer  |         |                         | open   |
              |   |   |   | name       | string   |         |                         | open   |
              |   |   |   | country    | ref      | Country |                         | open   |
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = "af697924-00ef-4dda-b212-0666f74b5365"
    new_lt_id = "bf697924-00ef-4dda-b212-0666f74b5365"
    en_id = "cf697924-00ef-4dda-b212-0666f74b5365"

    # Preset Country data
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    # Create duplicate value
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": new_lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": en_id, "id": 1, "name": "United Kingdom"})
    assert resp.status_code == 201

    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    # Preset City data, using lt_id as Country
    vln_id = "a5127db3-4459-4a67-af70-781243fe3418"
    kau_id = "b5127db3-4459-4a67-af70-781243fe3418"
    lnd_id = "c5127db3-4459-4a67-af70-781243fe3418"
    app.post(
        "/datasets/redirect/delete/City", json={"_id": vln_id, "id": 0, "name": "Vilnius", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": kau_id, "id": 1, "name": "Kaunas", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": lnd_id, "id": 2, "name": "London", "country": {"_id": en_id}}
    )

    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    # Library does not allow app.delete with data
    resp = app.request(
        "DELETE",
        f"/datasets/redirect/delete/Country/{lt_id}/:move",
        json={
            "_id": new_lt_id,
        },
    )
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["NoItemRevision"]

    # Check that nothing changed in Country table
    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    redirect_data = list(_redirect_table_data(context, "datasets/redirect/delete/Country"))
    assert redirect_data == []

    resp = app.get("/datasets/redirect/delete/Country/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    # Check changes in City tables
    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    resp = app.get("/datasets/redirect/delete/City/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "country._id", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_insert",
            "spinta_getall",
            "spinta_wipe",
            "spinta_changes",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_patch",
        ],
        [
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:wipe",
            "uapi:/:changes",
            "uapi:/:set_meta_fields",
            "uapi:/:move",
            "uapi:/:patch",
        ]
    ]
)
def test_move_delete_missing_id(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type     | ref     | prepare                 | access | level
            datasets/redirect/delete   |          |         |                         |        |
              |   |   | Country        |          |         |                         |        |
              |   |   |   | id         | integer  |         |                         | open   |
              |   |   |   | name       | string   |         |                         | open   |
              |   |   | City           |          |         |                         |        |
              |   |   |   | id         | integer  |         |                         | open   |
              |   |   |   | name       | string   |         |                         | open   |
              |   |   |   | country    | ref      | Country |                         | open   |
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = "af697924-00ef-4dda-b212-0666f74b5365"
    new_lt_id = "bf697924-00ef-4dda-b212-0666f74b5365"
    en_id = "cf697924-00ef-4dda-b212-0666f74b5365"

    # Preset Country data
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    revision = resp.json()["_revision"]
    # Create duplicate value
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": new_lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": en_id, "id": 1, "name": "United Kingdom"})
    assert resp.status_code == 201

    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    # Preset City data, using lt_id as Country
    vln_id = "a5127db3-4459-4a67-af70-781243fe3418"
    kau_id = "b5127db3-4459-4a67-af70-781243fe3418"
    lnd_id = "c5127db3-4459-4a67-af70-781243fe3418"
    app.post(
        "/datasets/redirect/delete/City", json={"_id": vln_id, "id": 0, "name": "Vilnius", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": kau_id, "id": 1, "name": "Kaunas", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": lnd_id, "id": 2, "name": "London", "country": {"_id": en_id}}
    )

    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    # Library does not allow app.delete with data
    resp = app.request("DELETE", f"/datasets/redirect/delete/Country/{lt_id}/:move", json={"_revision": revision})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredField"]

    # Check that nothing changed in Country table
    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    redirect_data = list(_redirect_table_data(context, "datasets/redirect/delete/Country"))
    assert redirect_data == []

    resp = app.get("/datasets/redirect/delete/Country/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    # Check changes in City tables
    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    resp = app.get("/datasets/redirect/delete/City/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "country._id", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_insert",
            "spinta_getall",
            "spinta_wipe",
            "spinta_changes",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_patch",
        ],
        [
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:wipe",
            "uapi:/:changes",
            "uapi:/:set_meta_fields",
            "uapi:/:move",
            "uapi:/:patch",
        ]
    ]
)
def test_move_delete_invalid_id(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type     | ref     | prepare                 | access | level
            datasets/redirect/delete   |          |         |                         |        |
              |   |   | Country        |          |         |                         |        |
              |   |   |   | id         | integer  |         |                         | open   |
              |   |   |   | name       | string   |         |                         | open   |
              |   |   | City           |          |         |                         |        |
              |   |   |   | id         | integer  |         |                         | open   |
              |   |   |   | name       | string   |         |                         | open   |
              |   |   |   | country    | ref      | Country |                         | open   |
            """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = "af697924-00ef-4dda-b212-0666f74b5365"
    new_lt_id = "bf697924-00ef-4dda-b212-0666f74b5365"
    en_id = "cf697924-00ef-4dda-b212-0666f74b5365"
    invalid_id = "df697924-00ef-4dda-b212-0666f74b5365"

    # Preset Country data
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    revision = resp.json()["_revision"]
    # Create duplicate value
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": new_lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": en_id, "id": 1, "name": "United Kingdom"})
    assert resp.status_code == 201

    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    # Preset City data, using lt_id as Country
    vln_id = "a5127db3-4459-4a67-af70-781243fe3418"
    kau_id = "b5127db3-4459-4a67-af70-781243fe3418"
    lnd_id = "c5127db3-4459-4a67-af70-781243fe3418"
    app.post(
        "/datasets/redirect/delete/City", json={"_id": vln_id, "id": 0, "name": "Vilnius", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": kau_id, "id": 1, "name": "Kaunas", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": lnd_id, "id": 2, "name": "London", "country": {"_id": en_id}}
    )

    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    # Library does not allow app.delete with data
    resp = app.request(
        "DELETE", f"/datasets/redirect/delete/Country/{lt_id}/:move", json={"_id": invalid_id, "_revision": revision}
    )
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ItemDoesNotExist"]

    # Check that nothing changed in Country table
    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    redirect_data = list(_redirect_table_data(context, "datasets/redirect/delete/Country"))
    assert redirect_data == []

    resp = app.get("/datasets/redirect/delete/Country/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    # Check changes in City tables
    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    resp = app.get("/datasets/redirect/delete/City/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "country._id", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        ["spinta_insert", "spinta_getall", "spinta_wipe", "spinta_changes", "spinta_set_meta_fields", "spinta_move"],
        ["uapi:/:create", "uapi:/:getall", "uapi:/:wipe", "uapi:/:changes", "uapi:/:set_meta_fields", "uapi:/:move"]
    ]
)
def test_move_delete_simple(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property   | type     | ref | prepare                 | access | level
        datasets/redirect/delete   |          |     |                         |        |
          |   |   | Country        |          |     |                         |        |
          |   |   |   | id         | integer  |     |                         | open   |
          |   |   |   | name       | string   |     |                         | open   |
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = "af697924-00ef-4dda-b212-0666f74b5365"
    new_lt_id = "ef697924-00ef-4dda-b212-0666f74b5365"

    resp = app.post("/datasets/redirect/delete/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    revision = resp.json()["_revision"]
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": new_lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201

    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
    ]

    # Library does not allow app.delete with data
    resp = app.request(
        "DELETE", f"/datasets/redirect/delete/Country/{lt_id}/:move", json={"_id": new_lt_id, "_revision": revision}
    )
    assert listdata(resp, "_id", "_same_as", full=True) == [
        {
            "_id": lt_id,
            "_same_as": new_lt_id,
        },
    ]
    assert resp.status_code == 200

    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
    ]

    redirect_data = list(_redirect_table_data(context, "datasets/redirect/delete/Country"))
    assert redirect_data == [{"_id": lt_id, "redirect": new_lt_id}]

    resp = app.get("/datasets/redirect/delete/Country/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", full=True) == [
        {"_cid": 1, "_op": "insert", "_id": lt_id, "id": 0, "name": "Lithuania"},
        {"_cid": 2, "_op": "insert", "_id": new_lt_id, "id": 0, "name": "Lithuania"},
        {
            "_cid": 3,
            "_op": "move",
            "_id": lt_id,
            "_same_as": new_lt_id,
        },
    ]


@pytest.mark.manifests("internal_sql", "csv")
@pytest.mark.parametrize(
    "scope",
    [
        [
            "spinta_insert",
            "spinta_getall",
            "spinta_wipe",
            "spinta_changes",
            "spinta_set_meta_fields",
            "spinta_move",
            "spinta_patch",
        ],
        [
            "uapi:/:create",
            "uapi:/:getall",
            "uapi:/:wipe",
            "uapi:/:changes",
            "uapi:/:set_meta_fields",
            "uapi:/:move",
            "uapi:/:patch",
        ]
    ]
)
def test_move_delete_referenced(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
    scope: list,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property   | type     | ref     | prepare                 | access | level
        datasets/redirect/delete   |          |         |                         |        |
          |   |   | Country        |          |         |                         |        |
          |   |   |   | id         | integer  |         |                         | open   |
          |   |   |   | name       | string   |         |                         | open   |
          |   |   | City           |          |         |                         |        |
          |   |   |   | id         | integer  |         |                         | open   |
          |   |   |   | name       | string   |         |                         | open   |
          |   |   |   | country    | ref      | Country |                         | open   |
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )

    app = create_test_client(context)
    app.authorize(scope)
    lt_id = "af697924-00ef-4dda-b212-0666f74b5365"
    new_lt_id = "bf697924-00ef-4dda-b212-0666f74b5365"
    en_id = "cf697924-00ef-4dda-b212-0666f74b5365"

    # Preset Country data
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    revision = resp.json()["_revision"]
    # Create duplicate value
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": new_lt_id, "id": 0, "name": "Lithuania"})
    assert resp.status_code == 201
    resp = app.post("/datasets/redirect/delete/Country", json={"_id": en_id, "id": 1, "name": "United Kingdom"})
    assert resp.status_code == 201

    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    # Preset City data, using lt_id as Country
    vln_id = "a5127db3-4459-4a67-af70-781243fe3418"
    kau_id = "b5127db3-4459-4a67-af70-781243fe3418"
    lnd_id = "c5127db3-4459-4a67-af70-781243fe3418"
    app.post(
        "/datasets/redirect/delete/City", json={"_id": vln_id, "id": 0, "name": "Vilnius", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": kau_id, "id": 1, "name": "Kaunas", "country": {"_id": lt_id}}
    )
    app.post(
        "/datasets/redirect/delete/City", json={"_id": lnd_id, "id": 2, "name": "London", "country": {"_id": en_id}}
    )

    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    # Library does not allow app.delete with data
    resp = app.request(
        "DELETE", f"/datasets/redirect/delete/Country/{lt_id}/:move", json={"_id": new_lt_id, "_revision": revision}
    )
    assert listdata(resp, "_id", "_same_as", full=True) == [
        {
            "_id": lt_id,
            "_same_as": new_lt_id,
        },
    ]
    assert resp.status_code == 200

    # Check changes in Country tables
    resp = app.get("/datasets/redirect/delete/Country")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", full=True) == [
        {
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
    ]

    redirect_data = list(_redirect_table_data(context, "datasets/redirect/delete/Country"))
    assert redirect_data == [{"_id": lt_id, "redirect": new_lt_id}]

    resp = app.get("/datasets/redirect/delete/Country/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": new_lt_id,
            "id": 0,
            "name": "Lithuania",
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": en_id,
            "id": 1,
            "name": "United Kingdom",
        },
        {
            "_cid": 4,
            "_op": "move",
            "_id": lt_id,
            "_same_as": new_lt_id,
        },
    ]

    # Check changes in City tables
    resp = app.get("/datasets/redirect/delete/City")
    assert resp.status_code == 200
    assert listdata(resp, "_id", "id", "name", "country._id", full=True) == [
        {
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": new_lt_id,
        },
        {
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": new_lt_id,
        },
        {
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
    ]

    resp = app.get("/datasets/redirect/delete/City/:changes")
    assert resp.status_code == 200
    assert listdata(resp, "_cid", "_id", "_op", "_same_as", "id", "name", "country._id", full=True) == [
        {
            "_cid": 1,
            "_op": "insert",
            "_id": vln_id,
            "id": 0,
            "name": "Vilnius",
            "country._id": lt_id,
        },
        {
            "_cid": 2,
            "_op": "insert",
            "_id": kau_id,
            "id": 1,
            "name": "Kaunas",
            "country._id": lt_id,
        },
        {
            "_cid": 3,
            "_op": "insert",
            "_id": lnd_id,
            "id": 2,
            "name": "London",
            "country._id": en_id,
        },
        {
            "_cid": 4,
            "_op": "patch",
            "_id": vln_id,
            "country._id": new_lt_id,
        },
        {
            "_cid": 5,
            "_op": "patch",
            "_id": kau_id,
            "country._id": new_lt_id,
        },
    ]
