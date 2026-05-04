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


CSV_DATA = (
    "code,id,uuid_id,comma_code\n"
    'ORD001,123,d6420786-082f-4ee4-9624-7a559f31d032,"OR,D001"\n'
    'ORD002,1234,8f5773d6-a5eb-4409-8f88-aca874e27200,"OR,D002"\n'
)


class TestIdStr:
    def test_for_id_string_with_string(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/csv   |          | {path}        |            |
          |   |   | Region                            |            | code     |               |            |
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
            # Expected, CSV does not support getone operations

    def test_for_id_string_with_integer(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/csv   |          | {path}        |            |
          |   |   | Region                            |            | id       |               |            |
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
            # Expected, CSV does not support getone operations

    def test_for_id_string_with_uuid(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/csv   |          | {path}        |            |
          |   |   | Region                            |            | uuid_id  |               |            |
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
            # Expected, CSV does not support getone operations

    def test_for_id_string_with_string_correct_equal_sign(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/csv   |          | {path}        |            |
          |   |   | Region                            |            | code     |               |            |
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
            # Expected, CSV does not support getone operations


class TestIdInt:
    def test_for_id_integer(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/csv   |          | {path}        |            |
          |   |   | Region                            |            | id       |               |            |
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
            # Expected, CSV does not support getone operations

    def test_for_id_integer_errors_with_string(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/csv   |          |               |            |
              |   |   | Region                            |            | id       |               |            |
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
              | data                                      | dask/csv   |          |               |            |
              |   |   | Region                            |            | id       |               |            |
              |   |   |   | id                            | uuid       |          | id            |            | open
              |   |   |   | _id                           | integer    |          |               |            | open
            """,
                mode=Mode.external,
            )


class TestIdUuid:
    def test_for_id_uuid(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/csv   |          | {path}        |            |
          |   |   | Region                            |            | uuid_id  |               |            |
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
            # Expected, CSV does not support getone operations

    def test_for_id_uuid_errors_with_string(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref      | source        | level      | access
            example                                       |            |          |               |            |
              | data                                      | dask/csv   |          |               |            |
              |   |   | Region                            |            | id       |               |            |
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
              | data                                      | dask/csv   |          |               |            |
              |   |   | Region                            |            | id       |               |            |
              |   |   |   | id                            | integer    |          | id            |            | open
              |   |   |   | _id                           | uuid       |          |               |            | open
            """,
                mode=Mode.external,
            )


class TestIdComp:
    def test_for_id_comp(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/csv   |           | {path}        |            |
          |   |   | Region                            |            | id, code  |               |            |
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
            # Expected, CSV does not support getone operations

    def test_for_id_comp_error_with_integer(self, rc: RawConfig):
        with pytest.raises(ReservedPropertyTypeShouldMatchPrimaryKey):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/csv   |           |               |            |
              |   |   | Region                            |            | id, code  |               |            |
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
              | data                                      | dask/csv   |           |               |            |
              |   |   | Region                            |            | id, code  |               |            |
              |   |   |   | code                          | string     |           | code          |            | open
              |   |   |   | _id                           | uuid       |           |               |            | open
              |   |   |   | id                            | integer    |           | id            |            | open
            """,
                mode=Mode.external,
            )


class TestIdBase:
    def test_for_id_base32_with_string(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/csv   |           | {path}        |            |
          |   |   | Region                            |            | code      |               |            |
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
        assert first_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGE"
        assert second_row_data["_id"]["link"] == "/example/Region/=J5JEIMBQGI"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, CSV does not support getone operations

    def test_for_id_base32_with_integer(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/csv   |           | {path}        |            |
          |   |   | Region                            |            | id        |               |            |
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
        assert first_row_data["_id"]["value"] == "GEZDG"
        assert second_row_data["_id"]["value"] == "GEZDGNA"
        assert first_row_data["_id"]["link"] == "/example/Region/=GEZDG"
        assert second_row_data["_id"]["link"] == "/example/Region/=GEZDGNA"

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, CSV does not support getone operations

    def test_for_id_base32_with_uuid(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref       | source        | level      | access
        example                                       |            |           |               |            |
          | data                                      | dask/csv   |           | {path}        |            |
          |   |   | Region                            |            | uuid_id   |               |            |
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
            == "/example/Region/=MQ3DIMRQG44DMLJQHAZGMLJUMVSTILJZGYZDILJXME2TKOLGGMYWIMBTGI"
        )
        assert (
            second_row_data["_id"]["link"]
            == "/example/Region/=HBTDKNZXGNSDMLLBGVSWELJUGQYDSLJYMY4DQLLBMNQTQNZUMUZDOMRQGA"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, CSV does not support getone operations

    def test_for_id_base32_with_composite(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      | dask/csv   |                | {path}        |            |
          |   |   | Region                            |            | uuid_id, code  |               |            |
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
            "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTGJ5JEIMBQGE"
        )
        assert second_row_data["_id"]["link"] == (
            "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDGJ5JEIMBQGI"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, CSV does not support getone operations


class TestManifestLoading:
    def test_error_if_model_ref_and_source_empty(self, rc: RawConfig):
        with pytest.raises(ReservedPropertySourceOrModelRefShouldBeSet):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/csv   |           |               |            |
              |   |   | Region                            |            |           |               |            |
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
              | data                                      | dask/csv   |           |               |            |
              |   |   | Region                            |            | id        |               |            |
              |   |   |   | _id                           | base32     |           | id            |            | open
              |   |   |   | id                            | integer    |           |               |            | open

            """,
                mode=Mode.external,
            )

    def test_for_id_uuid_works_with_string_if_source_set(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref      | source        | level      | access
        example                                       |            |          |               |            |
          | data                                      | dask/csv   |          | {path}        |            |
          |   |   | Region                            |            |          |               |            |
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
            # Expected, CSV does not support getone operations

    def test_only_id_can_have_base32_type(self, rc: RawConfig):
        with pytest.raises(Base32TypeOnlyAllowedOnId):
            prepare_manifest(
                rc,
                """
            d | r | b | m | property                      | type       | ref       | source        | level      | access
            example                                       |            |           |               |            |
              | data                                      | dask/csv   |           |               |            |
              |   |   | Region                            |            | id        |               |            |
              |   |   |   | _id                           | base32     |           |               |            | open
              |   |   |   | id                            | base32     |           |               |            | open

            """,
                mode=Mode.external,
            )

    def test_composite_values_cant_have_commas(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      | dask/csv   |                | {path}        |            |
          |   |   | Region                            |            | uuid_id, code  |               |            |
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

    def test_composite_values_with_base32_can_have_commas(self, rc: RawConfig, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text(CSV_DATA)

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                      | type       | ref            | source        | level      | access
        example                                       |            |                |               |            |
          | data                                      | dask/csv   |                | {path}        |            |
          |   |   | Region                            |            | uuid_id, code  |               |            |
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
            == "/example/Region/=QJ4CIZBWGQZDANZYGYWTAOBSMYWTIZLFGQWTSNRSGQWTOYJVGU4WMMZRMQYDGMTHJ5JCYRBQGAYQ"
        )
        assert (
            second_row_data["_id"]["link"]
            == "/example/Region/=QJ4CIODGGU3TOM3EGYWWCNLFMIWTINBQHEWTQZRYHAWWCY3BHA3TIZJSG4ZDAMDHJ5JCYRBQGAZA"
        )

        with pytest.raises(NotImplementedError):
            app.get(first_row_data["_id"]["link"])
            # Expected, CSV does not support getone operations


class TestIdRef:
    def test_ref_to_string_id_model(self, rc: RawConfig, tmp_path: Path):
        region_path = tmp_path / "region.csv"
        city_path = tmp_path / "city.csv"
        region_path.write_text("code\nORD001\nORD002\n")
        city_path.write_text("id,region_code\n1,ORD001\n2,ORD002\n")

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property   | type    | ref    | source         | level | access
        example                    |         |        |                |       |
          | region_data             | dask/csv|        | {region_path}  |       |
          |   |   | Region          |         | code   | region         |       |
          |   |   |   | code        | string  |        | code           |       | open
          |   |   |   | _id         | string  |        |                |       | open
          | city_data               | dask/csv|        | {city_path}    |       |
          |   |   | City            |         | id     | city           |       |
          |   |   |   | id          | integer |        | id             |       | open
          |   |   |   | region      | ref     | Region | region_code    |       | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall"])
        app.authmodel("example/City", ["getall"])

        reg_resp = app.get("/example/Region")
        assert reg_resp.status_code == 200
        region_rows = {r["code"]: r["_id"] for r in reg_resp.json()["_data"]}

        resp = app.get("/example/City")
        assert resp.status_code == 200
        city_by_id = {r["id"]: r for r in resp.json()["_data"]}
        assert city_by_id[1]["region"]["_id"] == region_rows["ORD001"]
        assert city_by_id[2]["region"]["_id"] == region_rows["ORD002"]

    def test_ref_to_uuid_id_model(self, rc: RawConfig, tmp_path: Path):
        uuid1 = "d6420786-082f-4ee4-9624-7a559f31d032"
        uuid2 = "8f5773d6-a5eb-4409-8f88-aca874e27200"
        region_path = tmp_path / "region.csv"
        city_path = tmp_path / "city.csv"
        region_path.write_text(f"uuid_id\n{uuid1}\n{uuid2}\n")
        city_path.write_text(f"id,region_uuid\n1,{uuid1}\n2,{uuid2}\n")

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property   | type    | ref     | source         | level | access
        example                    |         |         |                |       |
          | region_data             | dask/csv|         | {region_path}  |       |
          |   |   | Region          |         | uuid_id | region         |       |
          |   |   |   | uuid_id     | uuid    |         | uuid_id        |       | open
          |   |   |   | _id         | uuid    |         |                |       | open
          | city_data               | dask/csv|         | {city_path}    |       |
          |   |   | City            |         | id      | city           |       |
          |   |   |   | id          | integer |         | id             |       | open
          |   |   |   | region      | ref     | Region  | region_uuid    |       | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
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

    def test_ref_to_base32_model_produces_correct_id(self, rc: RawConfig, tmp_path: Path):
        region_path = tmp_path / "region.csv"
        city_path = tmp_path / "city.csv"
        region_path.write_text("code\nORD001\nORD002\n")
        city_path.write_text("id,region_code\n1,ORD001\n2,ORD002\n")

        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property   | type    | ref    | source         | level | access
        example                    |         |        |                |       |
          | region_data             | dask/csv|        | {region_path}  |       |
          |   |   | Region          |         | code   | region         |       |
          |   |   |   | code        | string  |        | code           |       | open
          |   |   |   | _id         | base32  |        |                |       | open
          | city_data               | dask/csv|        | {city_path}    |       |
          |   |   | City            |         | id     | city           |       |
          |   |   |   | id          | integer |        | id             |       | open
          |   |   |   | region      | ref     | Region | region_code    |       | open
        """,
            mode=Mode.external,
        )
        context.loaded = True
        app = create_test_client(context)
        app.authmodel("example/Region", ["getall"])
        app.authmodel("example/City", ["getall"])

        reg_resp = app.get("/example/Region")
        assert reg_resp.status_code == 200
        region_rows = {r["code"]: r["_id"] for r in reg_resp.json()["_data"]}

        resp = app.get("/example/City")
        assert resp.status_code == 200
        city_by_id = {r["id"]: r for r in resp.json()["_data"]}
        assert city_by_id[1]["region"]["_id"] == region_rows["ORD001"]
        assert city_by_id[2]["region"]["_id"] == region_rows["ORD002"]
