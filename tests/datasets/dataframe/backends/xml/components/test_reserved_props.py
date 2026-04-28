import json
import pytest

from spinta.core.config import RawConfig, Path
from spinta.core.enums import Mode
from spinta.exceptions import (
    ReservedPropertyTypeShouldMatchPrimaryKey,
    ReservedPropertySourceOrModelRefShouldBeSet,
    Base32TypeOnlyAllowedOnId,
)
from spinta.testing.client import create_test_client
from spinta.testing.manifest import prepare_manifest


XML_DATA = """
        <root>
            <order>
                <code>ORD001</code>
                <id>123</id>
                <uuid_id>d6420786-082f-4ee4-9624-7a559f31d032</uuid_id>
                <comma_code>OR,D001</comma_code>
            </order>
            <order>
                <code>ORD002</code>
                <id>1234</id>
                <uuid_id>8f5773d6-a5eb-4409-8f88-aca874e27200</uuid_id>
                <comma_code>OR,D002</comma_code>
            </order>
        </root>
        """


JSON_DATA = {
    "order": [
        {"code": "ORD001", "id": "123", "uuid_id": "d6420786-082f-4ee4-9624-7a559f31d032", "comma_code": "OR,D001"},
        {"code": "ORD002", "id": "1234", "uuid_id": "8f5773d6-a5eb-4409-8f88-aca874e27200", "comma_code": "OR,D002"},
    ]
}


@pytest.mark.parametrize(
    "fmt,data,source",
    [
        ("json", json.dumps(JSON_DATA), "order"),
        ("xml", XML_DATA, "/root/order"),
    ],
    ids=["json", "xml"],
)
class TestIdStr:
    def test_for_id_string_with_string(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/{fmt} |          | {path}        |            |
          |   |   | Region                            |            | code     | {source}      |            |
          |   |   |   | code                          | string     |          | code          |            | open
          |   |   |   | _id                           | string     |          |               |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_string_with_integer(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/{fmt} |          | {path}        |            |
          |   |   | Region                            |            | id       | {source}      |            |
          |   |   |   | id                            | integer    |          | id            |            | open
          |   |   |   | _id                           | string     |          |               |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_string_with_uuid(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/{fmt} |          | {path}        |            |
          |   |   | Region                            |            | uuid_id  | {source}      |            |
          |   |   |   | uuid_id                       | uuid       |          | uuid_id       |            | open
          |   |   |   | _id                           | string     |          |               |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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
        assert first_row_data["_id"]["link"] == "/example/Region/=d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["_id"]["link"] == "/example/Region/=8f5773d6-a5eb-4409-8f88-aca874e27200"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_string_with_string_correct_equal_sing(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/{fmt} |          | {path}        |            |
          |   |   | Region                            |            | code     | {source}      |            |
          |   |   |   | code                          | string     |          | comma_code    |            | open
          |   |   |   | _id                           | string     |          |               |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["code"]["value"] == "OR,D001"
        assert second_row_data["code"]["value"] == "OR,D002"
        assert first_row_data["_id"]["value"] == "OR,D001"
        assert second_row_data["_id"]["value"] == "OR,D002"
        assert first_row_data["_id"]["link"] == "/example/Region/=OR,D001"
        assert second_row_data["_id"]["link"] == "/example/Region/=OR,D002"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations


@pytest.mark.parametrize(
    "fmt,data,source",
    [
        ("json", json.dumps(JSON_DATA), "order"),
        ("xml", XML_DATA, "/root/order"),
    ],
    ids=["json", "xml"],
)
class TestIdInt:
    def test_for_id_integer(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/{fmt} |          | {path}        |            |
          |   |   | Region                            |            | id       | {source}      |            |
          |   |   |   | id                            | integer    |          | id            |            | open
          |   |   |   | _id                           | integer    |          |               |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_integer_errors_with_string(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/{fmt} |          |               |            |
              |   |   | Region                            |            | id       | {source}      |            |
              |   |   |   | id                            | string     |          | id            |            | open
              |   |   |   | _id                           | integer    |          |               |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_integer_errors_with_uuid(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/{fmt} |          |               |            |
              |   |   | Region                            |            | id       | {source}      |            |
              |   |   |   | id                            | uuid       |          | id            |            | open
              |   |   |   | _id                           | integer    |          |               |            | open
            """,
                mode=Mode.external,
            )


@pytest.mark.parametrize(
    "fmt,data,source",
    [
        ("json", json.dumps(JSON_DATA), "order"),
        ("xml", XML_DATA, "/root/order"),
    ],
    ids=["json", "xml"],
)
class TestIdUuid:
    def test_for_id_uuid(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/{fmt} |          | {path}        |            |
          |   |   | Region                            |            | uuid_id  | {source}      |            |
          |   |   |   | uuid_id                       | uuid       |          | uuid_id       |            | open
          |   |   |   | _id                           | uuid       |          |               |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_uuid_errors_with_string(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/{fmt} |          |               |            |
              |   |   | Region                            |            | id       | {source}      |            |
              |   |   |   | id                            | string     |          | id            |            | open
              |   |   |   | _id                           | uuid       |          |               |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_uuid_errors_with_integer(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/{fmt} |          |               |            |
              |   |   | Region                            |            | id       | {source}      |            |
              |   |   |   | id                            | integer    |          | id            |            | open
              |   |   |   | _id                           | uuid       |          |               |            | open
            """,
                mode=Mode.external,
            )


@pytest.mark.parametrize(
    "fmt,data,source",
    [
        ("json", json.dumps(JSON_DATA), "order"),
        ("xml", XML_DATA, "/root/order"),
    ],
    ids=["json", "xml"],
)
class TestIdComp:
    def test_for_id_comp(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/{fmt} |           | {path}        |            |
          |   |   | Region                            |            | id, code  | {source}      |            |
          |   |   |   | code                          | string     |           | code          |            | open
          |   |   |   | _id                           | string     |           |               |            | open
          |   |   |   | id                            | integer    |           | id            |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_comp_error_with_integer(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/{fmt} |           |               |            |
              |   |   | Region                            |            | id, code  | {source}      |            |
              |   |   |   | code                          | string     |           | code          |            | open
              |   |   |   | _id                           | integer    |           |               |            | open
              |   |   |   | id                            | integer    |           | id            |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_comp_error_with_uuid(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/{fmt} |           |               |            |
              |   |   | Region                            |            | id, code  | {source}      |            |
              |   |   |   | code                          | string     |           | code          |            | open
              |   |   |   | _id                           | uuid       |           |               |            | open
              |   |   |   | id                            | integer    |           | id            |            | open
            """,
                mode=Mode.external,
            )


@pytest.mark.parametrize(
    "fmt,data,source",
    [
        ("json", json.dumps(JSON_DATA), "order"),
        ("xml", XML_DATA, "/root/order"),
    ],
    ids=["json", "xml"],
)
class TestIdBase:
    def test_for_id_base32_with_string(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/{fmt} |           | {path}        |            |
          |   |   | Region                            |            | code      | {source}      |            |
          |   |   |   | code                          | string     |           | code          |            | open
          |   |   |   | _id                           | base32     |           |               |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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
        assert first_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGE======"
        assert second_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGI======"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_base32_with_integer(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/{fmt} |           | {path}        |            |
          |   |   | Region                            |            | id        | {source}      |            |
          |   |   |   | _id                           | base32     |           |               |            | open
          |   |   |   | id                            | integer    |           | id            |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["id"]["value"] == 123
        assert second_row_data["id"]["value"] == 1234
        assert first_row_data["_id"]["value"] == "GEZDG==="
        assert second_row_data["_id"]["value"] == "GEZDGNA="
        assert first_row_data["_id"]["link"] == "/example/Region/=GEZDG==="
        assert second_row_data["_id"]["link"] == "/example/Region/=GEZDGNA="

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_base32_with_uuid(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/{fmt} |           | {path}        |            |
          |   |   | Region                            |            | uuid_id   | {source}      |            |
          |   |   |   | _id                           | base32     |           |               |            | open
          |   |   |   | uuid_id                       | uuid       |           | uuid_id       |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["_id"]["value"] == "MQ3DIMRQ"
        assert second_row_data["_id"]["value"] == "HBTDKNZX"
        assert (
            first_row_data["_id"]["link"]
            == "/example/Region/=MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGI======"
        )
        assert (
            second_row_data["_id"]["link"]
            == "/example/Region/=HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGA======"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_base32_with_composite(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      | dask/{fmt} |                | {path}        |            |
          |   |   | Region                            |            | uuid_id, code  | {source}      |            |
          |   |   |   | code                          | string     |                | code          |            | open
          |   |   |   | _id                           | base32     |                |               |            | open
          |   |   |   | uuid_id                       | uuid       |                | uuid_id       |            | open
        """,
            mode=Mode.external,
        )

        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert second_row_data["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert first_row_data["code"]["value"] == "ORD001"
        assert second_row_data["code"]["value"] == "ORD002"
        assert first_row_data["_id"]["value"] == "QJ4CIZBW"
        assert second_row_data["_id"]["value"] == "QJ4CIODG"
        assert first_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTGJ5JEIMBQGE======"
        )
        assert second_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDGJ5JEIMBQGI======"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations


@pytest.mark.parametrize(
    "fmt,data,source",
    [
        ("json", json.dumps(JSON_DATA), "order"),
        ("xml", XML_DATA, "/root/order"),
    ],
    ids=["json", "xml"],
)
class TestManifestLoading:
    def test_error_if_model_ref_and_source_empty(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/{fmt} |           |               |            |
              |   |   | Region                            |            |           | {source}      |            |
              |   |   |   | _id                           | base32     |           |               |            | open
            """,
                mode=Mode.external,
            )

    def test_error_if_model_ref_and_source_populated(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/{fmt} |           |               |            |
              |   |   | Region                            |            | id        | {source}      |            |
              |   |   |   | _id                           | base32     |           | id            |            | open
              |   |   |   | id                            | integer    |           |               |            | open

            """,
                mode=Mode.external,
            )

    def test_for_id_uuid_works_with_string_if_source_set(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/{fmt} |          | {path}        |            |
          |   |   | Region                            |            |          | {source}      |            |
          |   |   |   | uuid_id                       | uuid       |          | uuid_id       |            | open
          |   |   |   | _id                           | uuid       |          | uuid_id       |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_only_id_can_have_base32_type(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        with pytest.raises(Base32TypeOnlyAllowedOnId):
            prepare_manifest(
                rc,
                f"""
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/{fmt} |           |               |            |
              |   |   | Region                            |            | id        | {source}      |            |
              |   |   |   | _id                           | base32     |           |               |            | open
              |   |   |   | id                            | base32     |           |               |            | open

            """,
                mode=Mode.external,
            )

    def test_composite_values_cant_have_commas(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      | dask/{fmt} |                | {path}        |            |
          |   |   | Region                            |            | uuid_id, code  | {source}      |            |
          |   |   |   | code                          | string     |                | comma_code    |            | open
          |   |   |   | _id                           | string     |                |               |            | open
          |   |   |   | uuid_id                       | uuid       |                | uuid_id       |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 500
        assert resp.json()["errors"][0]["code"] == "ValuesForIdCantHaveSpecialSymbols"

    def test_composite_values_with_base32_can_have_commas(self, fmt, data, source, rc: RawConfig, tmp_path: Path):
        path = tmp_path / f"test.{fmt}"
        path.write_text(data)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      | dask/{fmt} |                | {path}        |            |
          |   |   | Region                            |            | uuid_id, code  | {source}      |            |
          |   |   |   | code                          | string     |                | comma_code    |            | open
          |   |   |   | _id                           | base32     |                |               |            | open
          |   |   |   | uuid_id                       | uuid       |                | uuid_id       |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        first_row_data, second_row_data = [
            {k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]
        ]

        assert first_row_data["_id"]["value"] == "QJ4CIZBW"
        assert second_row_data["_id"]["value"] == "QJ4CIODG"
        assert first_row_data["code"]["value"] == "OR,D001"
        assert second_row_data["code"]["value"] == "OR,D002"
        assert (
            first_row_data["_id"]["link"]
            == "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTHJ5JCYRBQGAYQ===="
        )
        assert (
            second_row_data["_id"]["link"]
            == "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDHJ5JCYRBQGAZA===="
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, XML does not support getone operations
