import pytest
import sqlalchemy as sa

from pathlib import Path
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import (
    ReservedPropertyTypeShouldMatchPrimaryKey,
    ReservedPropertySourceOrModelRefShouldBeSet,
    Base32TypeOnlyAllowedOnId,
)
from spinta.testing.client import create_client
from spinta.testing.datasets import create_sqlite_db
from spinta.testing.manifest import prepare_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.manifests.tabular.helpers import striptable


@pytest.fixture(scope="module")
def db():
    with create_sqlite_db(
        {
            "order": [
                sa.Column("code", sa.Text),
                sa.Column("id", sa.Integer),
                sa.Column("uuid_id", sa.String),
                sa.Column("comma_code", sa.Text),
            ],
        }
    ) as db:
        db.write(
            "order",
            [
                {"code": "ORD001", "id": 123, "uuid_id": "d6420786-082f-4ee4-9624-7a559f31d032", "comma_code": "OR,D001"},
                {"code": "ORD002", "id": 1234, "uuid_id": "8f5773d6-a5eb-4409-8f88-aca874e27200", "comma_code": "OR,D002"},
            ],
        )
        yield db


class TestIdStr:
    def test_for_id_string_with_string(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      |            | sqlite   |               |            |
          |   |   | Region                            |            | code     | order         |            |
          |   |   |   | code                          | string     |          | code          |            | open
          |   |   |   | _id                           | string     |          |               |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]
        assert rows[0]["code"]["value"] == "ORD001"
        assert rows[1]["code"]["value"] == "ORD002"
        assert rows[0]["_id"]["value"] == "ORD001"
        assert rows[1]["_id"]["value"] == "ORD002"
        assert rows[0]["_id"]["link"] == "/example/Region/=ORD001"
        assert rows[1]["_id"]["link"] == "/example/Region/=ORD002"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD001"

    def test_for_id_string_with_integer(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      |            | sqlite   |               |            |
          |   |   | Region                            |            | id       | order         |            |
          |   |   |   | id                            | integer    |          | id            |            | open
          |   |   |   | _id                           | string     |          |               |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == "123"
        assert rows[1]["_id"]["value"] == "1234"
        assert rows[0]["_id"]["link"] == "/example/Region/=123"
        assert rows[1]["_id"]["link"] == "/example/Region/=1234"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["id"] == 123

    def test_for_id_string_with_uuid(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      |            | sqlite   |               |            |
          |   |   | Region                            |            | uuid_id  | order         |            |
          |   |   |   | uuid_id                       | uuid       |          | uuid_id       |            | open
          |   |   |   | _id                           | string     |          |               |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]
        assert rows[0]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[1]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[0]["_id"]["value"] == "8f5773d6"
        assert rows[1]["_id"]["value"] == "d6420786"
        assert rows[0]["_id"]["link"] == "/example/Region/=8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[1]["_id"]["link"] == "/example/Region/=d6420786-082f-4ee4-9624-7a559f31d032"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"

    def test_for_id_string_with_string_correct_equal_sign(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref        | source        | level      | access
        example                                       |            |            |               |            |
          | data                                      |            | sqlite     |               |            |
          |   |   | Region                            |            | comma_code | order         |            |
          |   |   |   | comma_code                    | string     |            | comma_code    |            | open
          |   |   |   | _id                           | string     |            |               |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]
        assert rows[0]["comma_code"]["value"] == "OR,D001"
        assert rows[1]["comma_code"]["value"] == "OR,D002"
        assert rows[0]["_id"]["value"] == "OR,D001"
        assert rows[1]["_id"]["value"] == "OR,D002"
        assert rows[0]["_id"]["link"] == "/example/Region/=OR,D001"
        assert rows[1]["_id"]["link"] == "/example/Region/=OR,D002"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["comma_code"] == "OR,D001"


class TestIdInt:
    def test_for_id_integer(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      |            | sqlite   |               |            |
          |   |   | Region                            |            | id       | order         |            |
          |   |   |   | id                            | integer    |          | id            |            | open
          |   |   |   | _id                           | integer    |          |               |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == 123
        assert rows[1]["_id"]["value"] == 1234
        assert rows[0]["_id"]["link"] == "/example/Region/123"
        assert rows[1]["_id"]["link"] == "/example/Region/1234"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["id"] == 123

    def test_for_id_integer_errors_with_string(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | sql        |          |               |            |
              |   |   | Region                            |            | id       | order         |            |
              |   |   |   | id                            | string     |          | id            |            | open
              |   |   |   | _id                           | integer    |          |               |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_integer_errors_with_uuid(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | sql        |          |               |            |
              |   |   | Region                            |            | id       | order         |            |
              |   |   |   | id                            | uuid       |          | id            |            | open
              |   |   |   | _id                           | integer    |          |               |            | open
            """,
                mode=Mode.external,
            )


class TestIdUuid:
    def test_for_id_uuid(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      |            | sqlite   |               |            |
          |   |   | Region                            |            | uuid_id  | order         |            |
          |   |   |   | uuid_id                       | uuid       |          | uuid_id       |            | open
          |   |   |   | _id                           | uuid       |          |               |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[1]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[0]["_id"]["value"] == "8f5773d6"
        assert rows[1]["_id"]["value"] == "d6420786"
        assert rows[0]["_id"]["link"] == "/example/Region/8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[1]["_id"]["link"] == "/example/Region/d6420786-082f-4ee4-9624-7a559f31d032"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"

    def test_for_id_uuid_errors_with_string(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | sql        |          |               |            |
              |   |   | Region                            |            | id       | order         |            |
              |   |   |   | id                            | string     |          | id            |            | open
              |   |   |   | _id                           | uuid       |          |               |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_uuid_errors_with_integer(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | sql        |          |               |            |
              |   |   | Region                            |            | id       | order         |            |
              |   |   |   | id                            | integer    |          | id            |            | open
              |   |   |   | _id                           | uuid       |          |               |            | open
            """,
                mode=Mode.external,
            )


class TestIdComp:
    def test_for_id_comp(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      |            | sqlite    |               |            |
          |   |   | Region                            |            | id, code  | order         |            |
          |   |   |   | code                          | string     |           | code          |            | open
          |   |   |   | _id                           | string     |           |               |            | open
          |   |   |   | id                            | integer    |           | id            |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["code"]["value"] == "ORD001"
        assert rows[1]["code"]["value"] == "ORD002"
        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == "123,ORD0"
        assert rows[1]["_id"]["value"] == "1234,ORD"
        assert rows[0]["_id"]["link"] == "/example/Region/123,ORD001"
        assert rows[1]["_id"]["link"] == "/example/Region/1234,ORD002"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD001"
        assert resp.json()["id"] == 123

    def test_for_id_comp_error_with_integer(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | sql        |           |               |            |
              |   |   | Region                            |            | id, code  | order         |            |
              |   |   |   | code                          | string     |           | code          |            | open
              |   |   |   | _id                           | integer    |           |               |            | open
              |   |   |   | id                            | integer    |           | id            |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_comp_error_with_uuid(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | sql        |           |               |            |
              |   |   | Region                            |            | id, code  | order         |            |
              |   |   |   | code                          | string     |           | code          |            | open
              |   |   |   | _id                           | uuid       |           |               |            | open
              |   |   |   | id                            | integer    |           | id            |            | open
            """,
                mode=Mode.external,
            )


class TestIdBase:
    def test_for_id_base32_with_string(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      |            | sqlite    |               |            |
          |   |   | Region                            |            | code      | order         |            |
          |   |   |   | code                          | string     |           | code          |            | open
          |   |   |   | _id                           | base32     |           |               |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["code"]["value"] == "ORD001"
        assert rows[1]["code"]["value"] == "ORD002"
        assert rows[0]["_id"]["value"] == "J5JEIMBQ"
        assert rows[1]["_id"]["value"] == "J5JEIMBQ"
        assert rows[0]["_id"]["link"] == "/example/Region/=J5JEIMBQGE======"
        assert rows[1]["_id"]["link"] == "/example/Region/=J5JEIMBQGI======"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD001"

    def test_for_id_base32_with_integer(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      |            | sqlite    |               |            |
          |   |   | Region                            |            | id        | order         |            |
          |   |   |   | _id                           | base32     |           |               |            | open
          |   |   |   | id                            | integer    |           | id            |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == "GEZDG==="
        assert rows[1]["_id"]["value"] == "GEZDGNA="
        assert rows[0]["_id"]["link"] == "/example/Region/=GEZDG==="
        assert rows[1]["_id"]["link"] == "/example/Region/=GEZDGNA="

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["id"] == 123

    def test_for_id_base32_with_uuid(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      |            | sqlite    |               |            |
          |   |   | Region                            |            | uuid_id   | order         |            |
          |   |   |   | _id                           | base32     |           |               |            | open
          |   |   |   | uuid_id                       | uuid       |           | uuid_id       |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[1]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[0]["_id"]["value"] == "HBTDKNZX"
        assert rows[1]["_id"]["value"] == "MQ3DIMRQ"
        assert (
            rows[0]["_id"]["link"]
            == "/example/Region/=HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGA======"
        )
        assert (
            rows[1]["_id"]["link"]
            == "/example/Region/=MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGI======"
        )

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"

    def test_for_id_base32_with_composite(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      |            | sqlite         |               |            |
          |   |   | Region                            |            | uuid_id, code  | order         |            |
          |   |   |   | code                          | string     |                | code          |            | open
          |   |   |   | _id                           | base32     |                |               |            | open
          |   |   |   | uuid_id                       | uuid       |                | uuid_id       |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[1]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[0]["code"]["value"] == "ORD002"
        assert rows[1]["code"]["value"] == "ORD001"
        assert rows[0]["_id"]["value"] == "QJ4CIODG"
        assert rows[1]["_id"]["value"] == "QJ4CIZBW"
        assert rows[0]["_id"]["link"] == (
            "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDGJ5JEIMBQGI======"
        )
        assert rows[1]["_id"]["link"] == (
            "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTGJ5JEIMBQGE======"
        )

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD002"
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"


class TestManifestLoading:
    def test_error_if_model_ref_and_source_empty(self, rc: RawConfig):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | sql        |           |               |            |
              |   |   | Region                            |            |           | order         |            |
              |   |   |   | _id                           | base32     |           |               |            | open
            """,
                mode=Mode.external,
            )

    def test_error_if_model_ref_and_source_populated(self, rc: RawConfig):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | sql        |           |               |            |
              |   |   | Region                            |            | id        | order         |            |
              |   |   |   | _id                           | base32     |           | id            |            | open
              |   |   |   | id                            | integer    |           |               |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_uuid_works_with_string_if_source_set(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      |            | sqlite   |               |            |
          |   |   | Region                            |            |          | order         |            |
          |   |   |   | uuid_id                       | uuid       |          | uuid_id       |            | open
          |   |   |   | _id                           | uuid       |          | uuid_id       |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[1]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[0]["_id"]["value"] == "d6420786"
        assert rows[1]["_id"]["value"] == "8f5773d6"
        assert rows[0]["_id"]["link"] == "/example/Region/d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[1]["_id"]["link"] == "/example/Region/8f5773d6-a5eb-4409-8f88-aca874e27200"

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "d6420786-082f-4ee4-9624-7a559f31d032"

    def test_only_id_can_have_base32_type(self, rc: RawConfig):
        with pytest.raises(Base32TypeOnlyAllowedOnId):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | sql        |           |               |            |
              |   |   | Region                            |            | id        | order         |            |
              |   |   |   | _id                           | base32     |           |               |            | open
              |   |   |   | id                            | base32     |           |               |            | open
            """,
                mode=Mode.external,
            )

    def test_composite_values_cant_have_commas(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      |            | sqlite         |               |            |
          |   |   | Region                            |            | uuid_id, code  | order         |            |
          |   |   |   | code                          | string     |                | comma_code    |            | open
          |   |   |   | _id                           | string     |                |               |            | open
          |   |   |   | uuid_id                       | uuid       |                | uuid_id       |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 500
        assert resp.json()["errors"][0]["code"] == "ValuesForIdCantHaveSpecialSymbols"

    def test_composite_values_with_base32_can_have_commas(self, context, rc: RawConfig, tmp_path: Path, db):
        create_tabular_manifest(
            context,
            tmp_path / "manifest.csv",
            striptable("""
        d | r | b | m | property                      | type       | ref                  | source        | level      | access
        example                                       |            |                      |               |            |
          | data                                      |            | sqlite               |               |            |
          |   |   | Region                            |            | uuid_id, comma_code  | order         |            |
          |   |   |   | comma_code                    | string     |                      | comma_code    |            | open
          |   |   |   | _id                           | base32     |                      |               |            | open
          |   |   |   | uuid_id                       | uuid       |                      | uuid_id       |            | open
        """),
        )
        app = create_client(rc, tmp_path, db, mode="external")
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["_id"]["value"] == "QJ4CIODG"
        assert rows[1]["_id"]["value"] == "QJ4CIZBW"
        assert rows[0]["comma_code"]["value"] == "OR,D002"
        assert rows[1]["comma_code"]["value"] == "OR,D001"
        assert rows[0]["_id"]["link"] == (
            "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDHJ5JCYRBQGAZA===="
        )
        assert rows[1]["_id"]["link"] == (
            "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTHJ5JCYRBQGAYQ===="
        )

        resp = app.get(rows[0]["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["comma_code"] == "OR,D002"
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
