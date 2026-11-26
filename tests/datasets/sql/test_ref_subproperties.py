import pytest
import sqlalchemy as sa

from spinta.manifests.tabular.helpers import striptable
from spinta.testing.client import create_client
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import get_error_codes

# `Apskritis.admin_unit` is a ref to `AdminUnit`, which lives in a dataset without
# a resource, so the model has no backend and its `_id` cannot be looked up by
# selecting rows. `admin_unit` has a `prepare` value and sub-properties.
_MANIFEST = """
d | r | b | m | property             | type    | ref                  | source      | prepare     | access
example                              |         |                      |             |             |
  | db                               |         | sql                  |             |             |
  |                                  |         |                      |             |             |
  |   |   | Apskritis                |         | code                 | apskritis   |             |
  |   |   |   | _id                  | integer |                      |             |             | open
  |   |   |   | code                 | integer |                      | adm_kodas   |             | open
  |   |   |   | name@lt              | string  |                      | pavadinimas |             | open
  |   |   |   | admin_unit           | ref     | /addresses/AdminUnit |             | 1           | open
  |   |   |   | admin_unit.code      | integer |                      |             | 1           | open
  |   |   |   | admin_unit.level     | string  |                      | tipas       |             | open
  |   |   |   | admin_unit.name@lt   | string  |                      |             | "Apskritis" | open
  |   |   |   | admin_unit.santrumpa | string  |                      | santrumpa   |             | open
addresses                            |         |                      |             |             |
  |   |   | AdminUnit                |         | code                 |             |             |
  |   |   |   | _id                  | integer |                      |             |             | open
  |   |   |   | code                 | integer |                      |             |             | open
  |   |   |   | level                | string  |                      |             |             | open
  |   |   |   | name@lt              | string  |                      |             |             | open
  |   |   |   | santrumpa            | string  |                      |             |             | open
"""

# Same as `_MANIFEST`, but `admin_unit` has no `prepare` and no `source`, so its
# value is built from the sub-properties only.
_MANIFEST_WITHOUT_REF_PREPARE = """
d | r | b | m | property             | type    | ref                  | source      | prepare     | access
example                              |         |                      |             |             |
  | db                               |         | sql                  |             |             |
  |                                  |         |                      |             |             |
  |   |   | Apskritis                |         | code                 | apskritis   |             |
  |   |   |   | _id                  | integer |                      |             |             | open
  |   |   |   | code                 | integer |                      | adm_kodas   |             | open
  |   |   |   | name@lt              | string  |                      | pavadinimas |             | open
  |   |   |   | admin_unit           | ref     | /addresses/AdminUnit |             |             | open
  |   |   |   | admin_unit.code      | integer |                      |             | 1           | open
  |   |   |   | admin_unit.level     | string  |                      | tipas       |             | open
  |   |   |   | admin_unit.name@lt   | string  |                      |             | "Apskritis" | open
  |   |   |   | admin_unit.santrumpa | string  |                      | santrumpa   |             | open
addresses                            |         |                      |             |             |
  |   |   | AdminUnit                |         | code                 |             |             |
  |   |   |   | _id                  | integer |                      |             |             | open
  |   |   |   | code                 | integer |                      |             |             | open
  |   |   |   | level                | string  |                      |             |             | open
  |   |   |   | name@lt              | string  |                      |             |             | open
  |   |   |   | santrumpa            | string  |                      |             |             | open
"""

# `admin_unit` refers to `AdminUnit` by `level`, which is not its primary key, so
# its `_id` cannot be built without looking the row up in `AdminUnit`.
_MANIFEST_REF_NOT_PKEY = """
d | r | b | m | property   | type    | ref                    | source | prepare     | access
example                    |         |                        |        |             |
  | db                     |         | sql                    |        |             |
  |                        |         |                        |        |             |
  |   |   | Apskritis      |         | code                   | apskritis |          |
  |   |   |   | _id        | integer |                        |        |             | open
  |   |   |   | code       | integer |                        | adm_kodas |          | open
  |   |   |   | admin_unit | ref     | /addresses/AdminUnit[level] |   | "apskritis" | open
addresses                  |         |                        |        |             |
  |   |   | AdminUnit      |         | code                   |        |             |
  |   |   |   | _id        | integer |                        |        |             | open
  |   |   |   | code       | integer |                        |        |             | open
  |   |   |   | level      | string  |                        |        |             | open
"""

_EXPECTED_ADMIN_UNIT = {
    "_id": 1,
    "code": 1,
    "level": "apskritis",
    "name": "Apskritis",
    "santrumpa": "apskr.",
}


@pytest.fixture(scope="module")
def geodb_apskritis():
    with create_sqlite_db(
        {
            "apskritis": [
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("adm_kodas", sa.Integer),
                sa.Column("tipas", sa.Text),
                sa.Column("santrumpa", sa.Text),
                sa.Column("pavadinimas", sa.Text),
            ]
        }
    ) as db:
        db.write(
            "apskritis",
            [
                {
                    "id": 1,
                    "adm_kodas": 1,
                    "tipas": "apskritis",
                    "santrumpa": "apskr.",
                    "pavadinimas": "Alytaus",
                },
            ],
        )
        yield db


def test_getone_ref_with_prepare_is_built_from_sub_properties(context, rc, tmp_path, geodb_apskritis):
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(_MANIFEST))
    app = create_client(rc, tmp_path, geodb_apskritis, mode="external")

    resp = app.get("/example/Apskritis/1")
    assert resp.status_code == 200
    assert resp.json()["admin_unit"] == _EXPECTED_ADMIN_UNIT


def test_getall_ref_with_prepare_is_built_from_sub_properties(context, rc, tmp_path, geodb_apskritis):
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(_MANIFEST))
    app = create_client(rc, tmp_path, geodb_apskritis, mode="external")

    resp = app.get("/example/Apskritis")
    assert resp.status_code == 200
    rows = resp.json()["_data"]
    assert [row["admin_unit"] for row in rows] == [_EXPECTED_ADMIN_UNIT]


def test_getall_select_ref_sub_properties(context, rc, tmp_path, geodb_apskritis):
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(_MANIFEST))
    app = create_client(rc, tmp_path, geodb_apskritis, mode="external")

    resp = app.get("/example/Apskritis?select(_id,admin_unit.code,admin_unit.level)")
    assert resp.status_code == 200
    assert resp.json()["_data"] == [{"_id": 1, "admin_unit": {"code": 1, "level": "apskritis"}}]


def test_getone_ref_without_prepare_is_built_from_sub_properties(context, rc, tmp_path, geodb_apskritis):
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(_MANIFEST_WITHOUT_REF_PREPARE))
    app = create_client(rc, tmp_path, geodb_apskritis, mode="external")

    resp = app.get("/example/Apskritis/1")
    assert resp.status_code == 200
    # No `_id`: a ref without a source has no column to take it from.
    assert resp.json()["admin_unit"] == {k: v for k, v in _EXPECTED_ADMIN_UNIT.items() if k != "_id"}


def test_ref_to_model_without_backend_not_by_primary_key_raises(context, rc, tmp_path, geodb_apskritis):
    create_tabular_manifest(context, tmp_path / "manifest.csv", striptable(_MANIFEST_REF_NOT_PKEY))
    app = create_client(rc, tmp_path, geodb_apskritis, mode="external")

    resp = app.get("/example/Apskritis/1")
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RefIdNotResolvable"]
