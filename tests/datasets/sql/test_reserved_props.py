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
                {
                    "code": "ORD001",
                    "id": 123,
                    "uuid_id": "d6420786-082f-4ee4-9624-7a559f31d032",
                    "comma_code": "OR,D001",
                },
                {
                    "code": "ORD002",
                    "id": 1234,
                    "uuid_id": "8f5773d6-a5eb-4409-8f88-aca874e27200",
                    "comma_code": "OR,D002",
                },
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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["_id"]["value"] == "ORD001"
        assert second_row_data["_id"]["value"] == "ORD002"
        assert first_row_data["_id"]["link"] == "/example/Region/=ORD001"
        assert second_row_data["_id"]["link"] == "/example/Region/=ORD002"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD001"
        assert resp.json()["_id"] == "ORD001"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == "123"
        assert second_row_data["_id"]["value"] == "1234"
        assert first_row_data["_id"]["link"] == "/example/Region/=123"
        assert second_row_data["_id"]["link"] == "/example/Region/=1234"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["id"] == 123
        assert resp.json()["_id"] == "123"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert second_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert first_row_data["_id"]["value"] == "8f5773d6"
        assert second_row_data["_id"]["value"] == "d6420786"
        assert first_row_data["_id"]["link"] == "/example/Region/=8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert second_row_data["_id"]["link"] == "/example/Region/=d6420786-082f-4ee4-9624-7a559f31d032"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert resp.json()["_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]
        assert first_row_data["comma_code"]["value"] == "OR,D001"
        assert second_row_data["comma_code"]["value"] == "OR,D002"
        assert first_row_data["_id"]["value"] == "OR,D001"
        assert second_row_data["_id"]["value"] == "OR,D002"
        assert first_row_data["_id"]["link"] == "/example/Region/=OR,D001"
        assert second_row_data["_id"]["link"] == "/example/Region/=OR,D002"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["comma_code"] == "OR,D001"
        assert resp.json()["_id"] == "OR,D001"


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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == 123
        assert second_row_data["_id"]["value"] == 1234
        assert first_row_data["_id"]["link"] == "/example/Region/123"
        assert second_row_data["_id"]["link"] == "/example/Region/1234"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["id"] == 123
        assert resp.json()["_id"] == 123

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert second_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert first_row_data["_id"]["value"] == "8f5773d6"
        assert second_row_data["_id"]["value"] == "d6420786"
        assert first_row_data["_id"]["link"] == "/example/Region/8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert second_row_data["_id"]["link"] == "/example/Region/d6420786-082f-4ee4-9624-7a559f31d032"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert resp.json()["_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == "123,ORD0"
        assert second_row_data["_id"]["value"] == "1234,ORD"
        assert first_row_data["_id"]["link"] == "/example/Region/123,ORD001"
        assert second_row_data["_id"]["link"] == "/example/Region/1234,ORD002"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD001"
        assert resp.json()["id"] == 123
        assert resp.json()["_id"] == "123,ORD001"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["_id"]["value"] == "J5JEIMBQ"
        assert second_row_data["_id"]["value"] == "J5JEIMBQ"
        assert first_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGE"
        assert second_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGI"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD001"
        assert resp.json()["_id"] == "J5JEIMBQGE"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == "GEZDG"
        assert second_row_data["_id"]["value"] == "GEZDGNA"
        assert first_row_data["_id"]["link"] == "/example/Region/=GEZDG"
        assert second_row_data["_id"]["link"] == "/example/Region/=GEZDGNA"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["id"] == 123
        assert resp.json()["_id"] == "GEZDG"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert second_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert first_row_data["_id"]["value"] == "HBTDKNZX"
        assert second_row_data["_id"]["value"] == "MQ3DIMRQ"
        assert (
            first_row_data["_id"]["link"]
            == "/example/Region/=HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGA"
        )
        assert (
            second_row_data["_id"]["link"]
            == "/example/Region/=MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGI"
        )

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert resp.json()["_id"] == "HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGA"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert second_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert first_row_data["code"]["value"] == "ORD002"
        assert second_row_data["code"]["value"] == "ORD001"
        assert first_row_data["_id"]["value"] == "QJ4CIODG"
        assert second_row_data["_id"]["value"] == "QJ4CIZBW"
        assert first_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDGJ5JEIMBQGI"
        )
        assert second_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTGJ5JEIMBQGE"
        )

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["code"] == "ORD002"
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert resp.json()["_id"] == "QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDGJ5JEIMBQGI"


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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["_id"]["value"] == "d6420786"
        assert second_row_data["_id"]["value"] == "8f5773d6"
        assert first_row_data["_id"]["link"] == "/example/Region/d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["_id"]["link"] == "/example/Region/8f5773d6-a5eb-4409-8f88-aca874e27200"

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["uuid_id"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert resp.json()["_id"] == "d6420786-082f-4ee4-9624-7a559f31d032"

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
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["_id"]["value"] == "QJ4CIODG"
        assert second_row_data["_id"]["value"] == "QJ4CIZBW"
        assert first_row_data["comma_code"]["value"] == "OR,D002"
        assert second_row_data["comma_code"]["value"] == "OR,D001"
        assert first_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDHJ5JCYRBQGAZA"
        )
        assert second_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTHJ5JCYRBQGAYQ"
        )

        resp = app.get(first_row_data["_id"]["link"])
        assert resp.status_code == 200
        assert resp.json()["comma_code"] == "OR,D002"
        assert resp.json()["uuid_id"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert resp.json()["_id"] == "QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDHJ5JCYRBQGAZA"


class TestIdRef:
    def test_ref_to_string_id_model(self, context, rc: RawConfig, tmp_path: Path):
        with create_sqlite_db(
            {
                "region": [
                    sa.Column("code", sa.Text),
                ],
                "city": [
                    sa.Column("id", sa.Integer),
                    sa.Column("region_code", sa.Text),
                ],
            }
        ) as db:
            db.write("region", [{"code": "ORD001"}, {"code": "ORD002"}])
            db.write("city", [{"id": 1, "region_code": "ORD001"}, {"id": 2, "region_code": "ORD002"}])

            create_tabular_manifest(
                context,
                tmp_path / "manifest.csv",
                striptable("""
            d | r | b | m | property   | type       | ref       | source       | level      | access
            example                   |            |           |              |            |
              | data                  |            | sqlite    |              |            |
              |   |   | Region        |            | code      | region       |            |
              |   |   |   | code      | string     |           | code         |            | open
              |   |   |   | _id       | string     |           |              |            | open
              |   |   | City          |            | id        | city         |            |
              |   |   |   | id        | integer    |           | id           |            | open
              |   |   |   | region    | ref        | Region    | region_code  |            | open
            """),
            )
            app = create_client(rc, tmp_path, db, mode="external")
            app.authmodel("example/Region", ["getall"])
            app.authmodel("example/City", ["getall"])

            resp = app.get("/example/Region")
            assert resp.status_code == 200
            region_rows = {r["code"]: r["_id"] for r in resp.json()["_data"]}

            resp = app.get("/example/City")
            assert resp.status_code == 200
            city_by_id = {r["id"]: r for r in resp.json()["_data"]}
            assert city_by_id[1]["region"]["_id"] == region_rows["ORD001"]
            assert city_by_id[2]["region"]["_id"] == region_rows["ORD002"]

    def test_ref_to_integer_id_model(self, context, rc: RawConfig, tmp_path: Path):
        with create_sqlite_db(
            {
                "region": [
                    sa.Column("id", sa.Integer),
                ],
                "city": [
                    sa.Column("id", sa.Integer),
                    sa.Column("region_id", sa.Integer),
                ],
            }
        ) as db:
            db.write("region", [{"id": 123}, {"id": 1234}])
            db.write("city", [{"id": 1, "region_id": 123}, {"id": 2, "region_id": 1234}])

            create_tabular_manifest(
                context,
                tmp_path / "manifest.csv",
                striptable("""
            d | r | b | m | property   | type       | ref       | source       | level      | access
            example                   |            |           |              |            |
              | data                  |            | sqlite    |              |            |
              |   |   | Region        |            | id        | region       |            |
              |   |   |   | id        | integer    |           | id           |            | open
              |   |   |   | _id       | integer    |           |              |            | open
              |   |   | City          |            | id        | city         |            |
              |   |   |   | id        | integer    |           | id           |            | open
              |   |   |   | region    | ref        | Region    | region_id    |            | open
            """),
            )
            app = create_client(rc, tmp_path, db, mode="external")
            app.authmodel("example/Region", ["getall"])
            app.authmodel("example/City", ["getall"])

            resp = app.get("/example/Region")
            assert resp.status_code == 200
            region_rows = {r["id"]: r["_id"] for r in resp.json()["_data"]}

            resp = app.get("/example/City")
            assert resp.status_code == 200
            city_by_id = {r["id"]: r for r in resp.json()["_data"]}
            assert city_by_id[1]["region"]["_id"] == region_rows[123]
            assert city_by_id[2]["region"]["_id"] == region_rows[1234]

    def test_ref_to_uuid_id_model(self, context, rc: RawConfig, tmp_path: Path):
        uuid1 = "8f5773d6-a5eb-4409-8f88-aca874e27200"
        uuid2 = "d6420786-082f-4ee4-9624-7a559f31d032"
        with create_sqlite_db(
            {
                "region": [
                    sa.Column("uuid_id", sa.String),
                ],
                "city": [
                    sa.Column("id", sa.Integer),
                    sa.Column("region_uuid", sa.String),
                ],
            }
        ) as db:
            db.write("region", [{"uuid_id": uuid1}, {"uuid_id": uuid2}])
            db.write("city", [{"id": 1, "region_uuid": uuid1}, {"id": 2, "region_uuid": uuid2}])

            create_tabular_manifest(
                context,
                tmp_path / "manifest.csv",
                striptable("""
            d | r | b | m | property   | type       | ref       | source       | level      | access
            example                   |            |           |              |            |
              | data                  |            | sqlite    |              |            |
              |   |   | Region        |            | uuid_id   | region       |            |
              |   |   |   | uuid_id   | uuid       |           | uuid_id      |            | open
              |   |   |   | _id       | uuid       |           |              |            | open
              |   |   | City          |            | id        | city         |            |
              |   |   |   | id        | integer    |           | id           |            | open
              |   |   |   | region    | ref        | Region    | region_uuid  |            | open
            """),
            )
            app = create_client(rc, tmp_path, db, mode="external")
            app.authmodel("example/Region", ["getall"])
            app.authmodel("example/City", ["getall"])

            resp = app.get("/example/Region")
            assert resp.status_code == 200
            region_rows = {r["uuid_id"]: r["_id"] for r in resp.json()["_data"]}

            resp = app.get("/example/City")
            assert resp.status_code == 200
            city_by_id = {r["id"]: r for r in resp.json()["_data"]}
            assert city_by_id[1]["region"]["_id"] == region_rows[uuid1]
            assert city_by_id[2]["region"]["_id"] == region_rows[uuid2]

    def test_ref_to_composite_id_model(self, context, rc: RawConfig, tmp_path: Path):
        with create_sqlite_db(
            {
                "region": [
                    sa.Column("id", sa.Integer),
                    sa.Column("code", sa.Text),
                ],
                "city": [
                    sa.Column("id", sa.Integer),
                    sa.Column("region_id", sa.Integer),
                    sa.Column("region_code", sa.Text),
                ],
            }
        ) as db:
            db.write("region", [{"id": 123, "code": "ORD001"}, {"id": 1234, "code": "ORD002"}])
            db.write(
                "city",
                [
                    {"id": 1, "region_id": 123, "region_code": "ORD001"},
                    {"id": 2, "region_id": 1234, "region_code": "ORD002"},
                ],
            )

            create_tabular_manifest(
                context,
                tmp_path / "manifest.csv",
                striptable("""
            d | r | b | m | property   | type       | ref       | source       | prepare                  | level | access
            example                    |            |           |              |                          |       |
              | data                   |            | sqlite    |              |                          |       |
              |   |   | Region         |            | id, code  | region       |                          |       |
              |   |   |   | id         | integer    |           | id           |                          |       | open
              |   |   |   | code       | string     |           | code         |                          |       | open
              |   |   |   | _id        | string     |           |              |                          |       | open
              |   |   | City           |            | id        | city         |                          |       |
              |   |   |   | id         | integer    |           | id           |                          |       | open
              |   |   |   | region     | ref        | Region    |              | region_id, region_code   |       | open
              |   |   |   | region_id  | integer    |           | region_id    |                          |       | open
              |   |   |   | region_code| string     |           | region_code  |                          |       | open
            """),
            )
            app = create_client(rc, tmp_path, db, mode="external")
            app.authmodel("example/Region", ["getall"])
            app.authmodel("example/City", ["getall", "search"])

            resp = app.get("/example/Region")
            assert resp.status_code == 200
            region_rows = {(r["id"], r["code"]): r["_id"] for r in resp.json()["_data"]}

            resp = app.get("/example/City")

            assert resp.status_code == 200
            city_by_id = {r["id"]: r for r in resp.json()["_data"]}
            assert city_by_id[1]["region"]["_id"] == region_rows[(123, "ORD001")]
            assert city_by_id[2]["region"]["_id"] == region_rows[(1234, "ORD002")]

    def test_ref_to_base32_model_produces_correct_id(self, context, rc: RawConfig, tmp_path: Path):
        with create_sqlite_db(
            {
                "region": [
                    sa.Column("code", sa.Text),
                ],
                "city": [
                    sa.Column("id", sa.Integer),
                    sa.Column("region_code", sa.Text),
                ],
            }
        ) as db:
            db.write("region", [{"code": "ORD001"}, {"code": "ORD002"}])
            db.write("city", [{"id": 1, "region_code": "ORD001"}, {"id": 2, "region_code": "ORD002"}])

            create_tabular_manifest(
                context,
                tmp_path / "manifest.csv",
                striptable("""
            d | r | b | m | property   | type       | ref       | source       | level      | access
            example                   |            |           |              |            |
              | data                  |            | sqlite    |              |            |
              |   |   | Region        |            | code      | region       |            |
              |   |   |   | code      | string     |           | code         |            | open
              |   |   |   | _id       | base32     |           |              |            | open
              |   |   | City          |            | id        | city         |            |
              |   |   |   | id        | integer    |           | id           |            | open
              |   |   |   | region    | ref        | Region    | region_code  |            | open
            """),
            )
            app = create_client(rc, tmp_path, db, mode="external")
            app.authmodel("example/Region", ["getall"])
            app.authmodel("example/City", ["getall"])

            resp = app.get("/example/Region")
            assert resp.status_code == 200
            region_rows = {r["code"]: r["_id"] for r in resp.json()["_data"]}

            resp = app.get("/example/City")
            assert resp.status_code == 200
            city_rows = resp.json()["_data"]

            city_by_id = {r["id"]: r for r in city_rows}
            assert city_by_id[1]["region"]["_id"] == region_rows["ORD001"]
            assert city_by_id[2]["region"]["_id"] == region_rows["ORD002"]
