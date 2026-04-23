import pytest

from spinta.core.config import RawConfig, Path
from spinta.core.enums import Mode
from spinta.exceptions import ReservedPropertyTypeShouldMatchPrimaryKey, ReservedPropertySourceOrModelRefShouldBeSet
from spinta.testing.client import create_test_client
from spinta.testing.manifest import prepare_manifest


class TestIdStr:
    def test_for_id_string_with_string(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <code>ORD001</code>
            </order>
            <order>
                <code>ORD002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          | {path}        |            |
          |   |   | Region                            |            | code     | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["code"]["value"] == "ORD001"
        assert rows[1]["code"]["value"] == "ORD002"
        assert rows[0]["_id"]["value"] == "ORD001"
        assert rows[1]["_id"]["value"] == "ORD002"
        assert rows[0]["_id"]["link"] == "/example/Region/=ORD001"
        assert rows[1]["_id"]["link"] == "/example/Region/=ORD002"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_string_with_integer(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <id>123</id>
            </order>
            <order>
                <id>1234</id>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          | {path}        |            |
          |   |   | Region                            |            | id       | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == "123"
        assert rows[1]["_id"]["value"] == "1234"
        assert rows[0]["_id"]["link"] == "/example/Region/=123"
        assert rows[1]["_id"]["link"] == "/example/Region/=1234"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_string_with_uuid(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <uuid_id>d6420786-082f-4ee4-9624-7a559f31d032</uuid_id>
            </order>
            <order>
                <uuid_id>8f5773d6-a5eb-4409-8f88-aca874e27200</uuid_id>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          | {path}        |            |
          |   |   | Region                            |            | uuid_id  | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[1]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[0]["_id"]["value"] == "d6420786"
        assert rows[1]["_id"]["value"] == "8f5773d6"
        assert rows[0]["_id"]["link"] == "/example/Region/=d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[1]["_id"]["link"] == "/example/Region/=8f5773d6-a5eb-4409-8f88-aca874e27200"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_string_with_string_incorrect_equal_sing(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <code>OR,D001</code>
            </order>
            <order>
                <code>OR,D002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          | {path}        |            |
          |   |   | Region                            |            | code     | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["code"]["value"] == "OR,D001"
        assert rows[1]["code"]["value"] == "OR,D002"
        assert rows[0]["_id"]["value"] == "OR,D001"
        assert rows[1]["_id"]["value"] == "OR,D002"
        assert rows[0]["_id"]["link"] == "/example/Region/=OR,D001"
        assert rows[1]["_id"]["link"] == "/example/Region/=OR,D002"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations


class TestIdInt:
    def test_for_id_integer(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <id>123</id>
                <code>ORD001</code>
            </order>
            <order>
                <id>1234</id>
                <code>ORD002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          | {path}        |            |
          |   |   | Region                            |            | id       | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == 123
        assert rows[1]["_id"]["value"] == 1234
        assert rows[0]["_id"]["link"] == "/example/Region/123"
        assert rows[1]["_id"]["link"] == "/example/Region/1234"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_integer_errors_with_string(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/xml   |          |               |            |
              |   |   | Region                            |            | id       | /root/order   |            |
              |   |   |   | id                            | string     |          | id            |            | open
              |   |   |   | _id                           | integer    |          |               |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_integer_errors_with_uuid(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/xml   |          |               |            |
              |   |   | Region                            |            | id       | /root/order   |            |
              |   |   |   | id                            | uuid       |          | id            |            | open
              |   |   |   | _id                           | integer    |          |               |            | open
            """,
                mode=Mode.external,
            )


class TestIdUuid:
    def test_for_id_uuid(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <id>ed76eda3-7922-4a7d-9ba8-62828ca0ae98</id>
                <code>ORD001</code>
            </order>
            <order>
                <id>1590ab44-6463-4da7-8862-3598f6e83924</id>
                <code>ORD002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          | {path}        |            |
          |   |   | Region                            |            | id       | /root/order   |            |
          |   |   |   | id                            | uuid       |          | id            |            | open
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == "ed76eda3-7922-4a7d-9ba8-62828ca0ae98"
        assert rows[1]["id"]["value"] == "1590ab44-6463-4da7-8862-3598f6e83924"
        assert rows[0]["_id"]["value"] == "ed76eda3"
        assert rows[1]["_id"]["value"] == "1590ab44"
        assert rows[0]["_id"]["link"] == "/example/Region/ed76eda3-7922-4a7d-9ba8-62828ca0ae98"
        assert rows[1]["_id"]["link"] == "/example/Region/1590ab44-6463-4da7-8862-3598f6e83924"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_uuid_errors_with_string(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/xml   |          |               |            |
              |   |   | Region                            |            | id       | /root/order   |            |
              |   |   |   | id                            | string     |          | id            |            | open
              |   |   |   | _id                           | uuid       |          |               |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_uuid_errors_with_integer(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/xml   |          |               |            |
              |   |   | Region                            |            | id       | /root/order   |            |
              |   |   |   | id                            | integer    |          | id            |            | open
              |   |   |   | _id                           | uuid       |          |               |            | open
            """,
                mode=Mode.external,
            )


class TestIdComp:
    def test_for_id_comp(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <id>123</id>
                <code>ORD001</code>
            </order>
            <order>
                <id>1234</id>
                <code>ORD002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/xml   |           | {path}        |            |
          |   |   | Region                            |            | id, code  | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["code"]["value"] == "ORD001"
        assert rows[1]["code"]["value"] == "ORD002"
        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == "123,ORD0"
        assert rows[1]["_id"]["value"] == "1234,ORD"
        assert rows[0]["_id"]["link"] == "/example/Region/123,ORD001"
        assert rows[1]["_id"]["link"] == "/example/Region/1234,ORD002"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_comp_error_with_integer(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/xml   |           |               |            |
              |   |   | Region                            |            | id, code  | /root/order   |            |
              |   |   |   | code                          | string     |           | code          |            | open
              |   |   |   | _id                           | integer    |           |               |            | open
              |   |   |   | id                            | integer    |           | id            |            | open
            """,
                mode=Mode.external,
            )

    def test_for_id_comp_error_with_uuid(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/xml   |           |               |            |
              |   |   | Region                            |            | id, code  | /root/order   |            |
              |   |   |   | code                          | string     |           | code          |            | open
              |   |   |   | _id                           | uuid       |           |               |            | open
              |   |   |   | id                            | integer    |           | id            |            | open
            """,
                mode=Mode.external,
            )


class TestIdBase:
    def test_for_id_base32_with_string(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <code>ORD001</code>
            </order>
            <order>
                <code>ORD002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/xml   |           | {path}        |            |
          |   |   | Region                            |            | code      | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["code"]["value"] == "ORD001"
        assert rows[1]["code"]["value"] == "ORD002"
        assert rows[0]["_id"]["value"] == "QFTE6USE"
        assert rows[1]["_id"]["value"] == "QFTE6USE"
        assert rows[0]["_id"]["link"] == "/example/Region/=QFTE6USEGAYDC==="
        assert rows[1]["_id"]["link"] == "/example/Region/=QFTE6USEGAYDE==="

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_base32_with_integer(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <id>123</id>
            </order>
            <order>
                <id>1234</id>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/xml   |           | {path}        |            |
          |   |   | Region                            |            | id        | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == 123
        assert rows[1]["id"]["value"] == 1234
        assert rows[0]["_id"]["value"] == "QFRTCMRT"
        assert rows[1]["_id"]["value"] == "QFSDCMRT"
        assert rows[0]["_id"]["link"] == "/example/Region/=QFRTCMRT"
        assert rows[1]["_id"]["link"] == "/example/Region/=QFSDCMRTGQ======"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_base32_with_uuid(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <uuid_id>d6420786-082f-4ee4-9624-7a559f31d032</uuid_id>
            </order>
            <order>
                <uuid_id>8f5773d6-a5eb-4409-8f88-aca874e27200</uuid_id>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/xml   |           | {path}        |            |
          |   |   | Region                            |            | uuid_id   | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[1]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[0]["_id"]["value"] == "QF4CIZBW"
        assert rows[1]["_id"]["value"] == "QF4CIODG"
        assert (
            rows[0]["_id"]["link"]
            == "/example/Region/=QF4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMQ="
        )
        assert (
            rows[1]["_id"]["link"]
            == "/example/Region/=QF4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMA="
        )

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations

    def test_for_id_base32_with_composite(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <uuid_id>d6420786-082f-4ee4-9624-7a559f31d032</uuid_id>
                <code>ORD001</code>
            </order>
            <order>
                <uuid_id>8f5773d6-a5eb-4409-8f88-aca874e27200</uuid_id>
                <code>ORD002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      | dask/xml   |                | {path}        |            |
          |   |   | Region                            |            | uuid_id, code  | /root/order   |            |
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
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["uuid_id"]["value"] == "d6420786-082f-4ee4-9624-7a559f31d032"
        assert rows[1]["uuid_id"]["value"] == "8f5773d6-a5eb-4409-8f88-aca874e27200"
        assert rows[0]["code"]["value"] == "ORD001"
        assert rows[1]["code"]["value"] == "ORD002"
        assert rows[0]["_id"]["value"] == "MQ3DIMRQ"
        assert rows[1]["_id"]["value"] == "HBTDKNZX"
        assert rows[0]["_id"]["link"] == (
            "/example/Region/=MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGIWE6USEGAYDC==="
        )
        assert rows[1]["_id"]["link"] == (
            "/example/Region/=HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGAWE6USEGAYDE==="
        )

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations


class TestManifestLoading:
    def test_error_if_model_ref_and_source_empty(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/xml   |           |               |            |
              |   |   | Region                            |            |           | /root/order   |            |
              |   |   |   | _id                           | base32     |           |               |            | open
            """,
                mode=Mode.external,
            )

    def test_error_if_model_ref_and_source_populated(self, rc: RawConfig, tmp_path: Path):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/xml   |           |               |            |
              |   |   | Region                            |            | id        | /root/order   |            |
              |   |   |   | _id                           | base32     |           | id            |            | open
              |   |   |   | id                            | integer    |           |               |            | open

            """,
                mode=Mode.external,
            )

    def test_for_id_uuid_works_with_string_if_source_set(self, rc: RawConfig, tmp_path: Path):
        xml = """
        <root>
            <order>
                <id>ed76eda3-7922-4a7d-9ba8-62828ca0ae98</id>
                <code>ORD001</code>
            </order>
            <order>
                <id>1590ab44-6463-4da7-8862-3598f6e83924</id>
                <code>ORD002</code>
            </order>
        </root>
        """
        path = tmp_path / "test.xml"
        path.write_text(xml)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/xml   |          | {path}        |            |
          |   |   | Region                            |            |          | /root/order   |            |
          |   |   |   | id                            | uuid       |          | id            |            | open
          |   |   |   | _id                           | uuid       |          | id            |            | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall", "getone"])

        resp = app.get("/example/Region/:format/html")
        assert resp.status_code == 200
        header = resp.context["header"]
        rows = [{k: cell.as_dict() for k, cell in zip(header, row)} for row in resp.context["data"]]

        assert rows[0]["id"]["value"] == "ed76eda3-7922-4a7d-9ba8-62828ca0ae98"
        assert rows[1]["id"]["value"] == "1590ab44-6463-4da7-8862-3598f6e83924"
        assert rows[0]["_id"]["value"] == "ed76eda3"
        assert rows[1]["_id"]["value"] == "1590ab44"
        assert rows[0]["_id"]["link"] == "/example/Region/ed76eda3-7922-4a7d-9ba8-62828ca0ae98"
        assert rows[1]["_id"]["link"] == "/example/Region/1590ab44-6463-4da7-8862-3598f6e83924"

        with pytest.raises(NotImplementedError):
            app.get(rows[0]["_id"]["link"])
            # Expected, XML does not support getone operations
