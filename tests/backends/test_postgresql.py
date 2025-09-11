from pathlib import Path

import pytest

from spinta.components import Model, Property
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.constants import NAMEDATALEN
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers import get_pg_sequence_name
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.utils import get_error_codes, get_error_context

from _pytest.fixtures import FixtureRequest


def get_model(name: str):
    model = Model()
    model.name = name
    return model


def get_property(model, place):
    prop = Property()
    prop.model = get_model(model)
    prop.place = place
    return prop


def _get_table_name(model, prop=None, ttype=TableType.MAIN):
    if prop is None:
        name = get_table_name(get_model(model), ttype)
    else:
        name = get_table_name(get_property(model, prop), ttype)
    return get_pg_name(name)


def test_get_table_name():
    assert _get_table_name("org") == "org"
    assert len(_get_table_name("a" * 1000)) == NAMEDATALEN
    assert _get_table_name("a" * 1000) == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_41edece4_aaaaaaaaaaaaaaaa"
    assert _get_table_name("some_/name/hėrę!") == "some_/name/hėrę!"


def test_get_table_name_lists():
    assert _get_table_name("org", "names", TableType.LIST) == "org/:list/names"
    assert _get_table_name("org", "names.note", TableType.LIST) == "org/:list/names.note"


@pytest.mark.parametrize(
    "name,result",
    [
        ("datasets/gov/example/City/:changelog", "datasets/gov/example/City/:changelog__id_seq"),
        (
            "datasets/gov/example_with_long_name/VeryLong_name/:changelog",
            "datasets/gov/example_with_long_name/VeryLong_name/:chan__id_seq",
        ),
    ],
)
def test_get_pg_sequence_name(name: str, result: str):
    assert get_pg_sequence_name(name) == result


def test_changes(app):
    app.authmodel("Country", ["insert", "update", "changes"])
    data = app.post("/Country", json={"_type": "Country", "code": "lt", "title": "Lithuania"}).json()
    app.put(f"/Country/{data['_id']}", json={"_type": "Country", "_id": data["_id"], "title": "Lietuva"})
    app.put(f"/Country/{data['_id']}", json={"type": "Country", "_id": data["_id"], "code": "lv", "title": "Latvia"})
    app.get(f"/Country/{data['_id']}/:changes").json() == {}


def test_delete(context, app):
    app.authmodel("Country", ["insert", "getall", "delete"])

    resp = app.post(
        "/",
        json={
            "_data": [
                {"_op": "insert", "_type": "Country", "code": "fi", "title": "Finland"},
                {"_op": "insert", "_type": "Country", "code": "lt", "title": "Lithuania"},
            ],
        },
    )
    ids = [x["_id"] for x in resp.json()["_data"]]
    revs = [x["_revision"] for x in resp.json()["_data"]]

    resp = app.get("/Country").json()
    data = [x["_id"] for x in resp["_data"]]
    assert ids[0] in data
    assert ids[1] in data

    # XXX: DELETE method should not include a request body.
    resp = app.request(
        "DELETE",
        f"/Country/{ids[0]}",
        json={
            "_revision": revs[0],
        },
    )
    assert resp.status_code == 204

    # multiple deletes should just return HTTP/404
    resp = app.delete(f"/Country/{ids[0]}")
    assert resp.status_code == 404
    assert get_error_codes(resp.json()) == ["ItemDoesNotExist"]
    assert get_error_context(resp.json(), "ItemDoesNotExist", ["manifest", "model", "id"]) == {
        "manifest": "default",
        "model": "Country",
        "id": ids[0],
    }

    resp = app.get("/Country").json()
    data = [x["_id"] for x in resp["_data"]]
    assert ids[0] not in data
    assert ids[1] in data


def test_patch(app):
    app.authorize(
        [
            "spinta_set_meta_fields",
            "spinta_country_insert",
            "spinta_country_getone",
            "spinta_org_insert",
            "spinta_org_getone",
            "spinta_org_patch",
        ]
    )

    country_data = app.post(
        "/Country",
        json={
            "_type": "Country",
            "code": "lt",
            "title": "Lithuania",
        },
    ).json()
    org_data = app.post(
        "/Org",
        json={
            "_type": "Org",
            "title": "My Org",
            "govid": "0042",
            "country": {
                "_id": country_data["_id"],
            },
        },
    ).json()
    id_ = org_data["_id"]

    resp = app.patch(
        f"/Org/{org_data['_id']}",
        json={
            "_revision": org_data["_revision"],
            "title": "foo org",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["title"] == "foo org"
    revision = resp.json()["_revision"]
    assert org_data["_revision"] != revision

    # test that revision mismatch is checked
    resp = app.patch(
        f"/Org/{org_data['_id']}",
        json={
            "_revision": "r3v1510n",
            "title": "foo org",
        },
    )
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(resp.json(), "ConflictingValue", ["given", "expected", "model"]) == {
        "given": "r3v1510n",
        "expected": revision,
        "model": "Org",
    }

    # test that type mismatch is checked
    resp = app.patch(
        f"/Org/{org_data['_id']}",
        json={
            "_type": "Country",
            "_revision": org_data["_revision"],
            "title": "foo org",
        },
    )
    assert resp.status_code == 409
    assert get_error_codes(resp.json()) == ["ConflictingValue"]
    assert get_error_context(resp.json(), "ConflictingValue", ["given", "expected", "model"]) == {
        "given": "Country",
        "expected": "Org",
        "model": "Org",
    }

    # test that id mismatch is checked
    resp = app.patch(
        f"/Org/{org_data['_id']}",
        json={
            "_id": "0007ddec-092b-44b5-9651-76884e6081b4",
            "_revision": revision,
            "title": "foo org",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["_revision"] != revision
    assert data == {
        "_type": "Org",
        "_id": "0007ddec-092b-44b5-9651-76884e6081b4",
        "_revision": data["_revision"],
    }
    id_ = data["_id"]
    revision = data["_revision"]

    # patch using same values as already stored in database
    resp = app.patch(
        f"/Org/{id_}",
        json={
            "_id": id_,
            "_type": "Org",
            "_revision": revision,
            "title": "foo org",
        },
    )
    assert resp.status_code == 200
    resp_data = resp.json()

    assert resp_data["_id"] == id_
    assert resp_data["_type"] == "Org"
    # title have not changed, so should not be included in result
    assert "title" not in resp_data
    # revision must be the same, since nothing has changed
    assert resp_data["_revision"] == revision


@pytest.mark.manifests("internal_sql", "ascii")
def test_exceptions_unique_constraint_single_column(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property | type   | ref     | access  | uri
        example/unique/single    |        |         |         |
          |   |   | Country      |        | name    |         | 
          |   |   |   | name     | string |         | open    | 
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/unique/single", ["insert"])

    app.post("/example/unique/single/Country", json={"name": "Lithuania"})
    response = app.post("/example/unique/single/Country", json={"name": "Lithuania"})
    assert response.status_code == 400
    assert response.json() == {
        "errors": [
            {
                "type": "property",
                "code": "UniqueConstraint",
                "template": "Given value ({value}) already exists.",
                "context": {
                    "component": "spinta.components.Property",
                    "manifest": "default",
                    "schema": "3",
                    "dataset": "example/unique/single",
                    "model": "example/unique/single/Country",
                    "entity": "",
                    "property": "name",
                    "attribute": "",
                },
                "message": "Given value ([UNKNOWN]) already exists.",
            }
        ]
    }


@pytest.mark.manifests("internal_sql", "ascii")
def test_exceptions_unique_constraint_multiple_columns(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
        d | r | b | m | property | type    | ref      | access  | uri
        example/unique/multiple  |         |          |         |
          |   |   | Country      |         | name, id |         | 
          |   |   |   | name     | string  |          | open    | 
          |   |   |   | id       | integer |          | open    | 
        """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/unique/multiple", ["insert"])

    app.post("/example/unique/multiple/Country", json={"name": "Lithuania", "id": 0})
    response = app.post("/example/unique/multiple/Country", json={"name": "Lithuania", "id": 0})
    assert response.status_code == 400
    assert response.json() == {
        "errors": [
            {
                "type": "model",
                "code": "CompositeUniqueConstraint",
                "template": "Given values for composition of properties ({properties}) already exist.",
                "context": {
                    "component": "spinta.components.Model",
                    "manifest": "default",
                    "schema": "3",
                    "dataset": "example/unique/multiple",
                    "model": "example/unique/multiple/Country",
                    "entity": "",
                    "properties": "name,id",
                },
                "message": "Given values for composition of properties (name,id) already exist.",
            }
        ]
    }
