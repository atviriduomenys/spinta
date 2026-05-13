from datetime import date, time, datetime
from decimal import Decimal

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.exceptions import InvalidBase64String, UnableToCast, NotImplementedFeature
from spinta.spyna import parse
from spinta.testing.manifest import load_manifest_and_context
from spinta.ufuncs.resultbuilder.components import ResultBuilder


class TestBase64:
    def test_decode_base64(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare 
            example                  |         |         
              |   |   | Data         |         |         
              |   |   |   | value    | string  | base64()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this="dGVzdA==",
            prop=model.properties["value"],
            data={"value": "dGVzdA=="},
            params={},
        )

        assert env.call("base64", asttoexpr(parse("base64()"))) == "test"

    def test_raise_error_if_cannot_decode_base64(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare      
            example                  |         |              
              |   |   | Data         |         |              
              |   |   |   | value    | string  | base64()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this="test",
            prop=model.properties["value"],
            data={"value": "dGVzdA=="},
            params={},
        )

        with pytest.raises(InvalidBase64String) as e:
            env.call("base64", asttoexpr(parse("base64")))

        assert e.value.message == (
            'Value of property "value" cannot be decoded because "test" is not valid base64 string.'
        )


class TestCast:
    def test_cast_string_to_string(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | string  | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this="abc",
            prop=model.properties["value"],
            data={"value": "abc"},
            params={},
        )

        assert env.call("cast") == "abc"
        assert env.call("cast", asttoexpr(parse("cast()"))) == "abc"

    def test_cast_int_to_string(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | string  | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=123,
            prop=model.properties["value"],
            data={"value": 123},
            params={},
        )

        assert env.call("cast") == "123"
        assert env.call("cast", asttoexpr(parse("cast()"))) == "123"

    def test_cast_none_to_string(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | string  | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=None,
            prop=model.properties["value"],
            data={"value": None},
            params={},
        )

        assert env.call("cast") == ""
        assert env.call("cast", asttoexpr(parse("cast()"))) == ""

    def test_cast_decimal_to_integer(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | integer | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=Decimal("10.00"),
            prop=model.properties["value"],
            data={"value": Decimal("10.00")},
            params={},
        )

        assert env.call("cast") == 10
        assert env.call("cast", asttoexpr(parse("cast()"))) == 10

    def test_cast_float_to_integer(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | integer | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=12.0,
            prop=model.properties["value"],
            data={"value": 12.0},
            params={},
        )

        assert env.call("cast") == 12
        assert env.call("cast", asttoexpr(parse("cast()"))) == 12

    @pytest.mark.parametrize("value, result", [("123", 123), ("+123", 123), ("-123", -123)])
    def test_cast_string_to_integer(self, rc: RawConfig, value: str, result: int):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | integer | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        assert env.call("cast") == result
        assert env.call("cast", asttoexpr(parse("cast()"))) == result

    @pytest.mark.parametrize("value", ["abc", "10.10", "-10,12"])
    def test_cast_invalid_string_to_integer(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | integer | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast")
        assert err_info.value.message == f"Unable to cast {value} to integer type."

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast", asttoexpr(parse("cast()")))
        assert err_info.value.message == f"Unable to cast {value} to integer type."

    @pytest.mark.parametrize("value, result", [("123", 123), ("+123", 123), ("-123", -123), ("1.12", 1.12)])
    def test_cast_string_to_number(self, rc: RawConfig, value: str, result: float):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type   | prepare
            example                  |        |
              |   |   | Data         |        |
              |   |   |   | value    | number | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        assert env.call("cast") == result
        assert env.call("cast", asttoexpr(parse("cast()"))) == result

    @pytest.mark.parametrize("value", ["abc", "123,1", "1.234.56", "1,234.56"])
    def test_cast_invalid_string_to_number(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type   | prepare
            example                  |        |
              |   |   | Data         |        |
              |   |   |   | value    | number | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast")
        assert err_info.value.message == f"Unable to cast {value} to number type."

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast", asttoexpr(parse("cast()")))
        assert err_info.value.message == f"Unable to cast {value} to number type."

    @pytest.mark.parametrize(
        "value, result",
        [
            ("true", True),
            ("1", True),
            ("on", True),
            ("yes", True),
            ("false", False),
            ("0", False),
            ("off", False),
            ("no", False),
        ],
    )
    def test_cast_string_to_boolean(self, rc: RawConfig, value: str, result: bool):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | boolean | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        assert env.call("cast") is result
        assert env.call("cast", asttoexpr(parse("cast()"))) is result

    @pytest.mark.parametrize("value", ["abc", "123"])
    def test_cast_invalid_string_to_boolean(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type    | prepare
            example                  |         |
              |   |   | Data         |         |
              |   |   |   | value    | boolean | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast")
        assert err_info.value.message == f"Unable to cast {value} to boolean type."

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast", asttoexpr(parse("cast()")))
        assert err_info.value.message == f"Unable to cast {value} to boolean type."

    @pytest.mark.parametrize("value", ["2025-05-05", "2025-05-05T13:50:13.401332"])
    def test_cast_string_to_date(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type | prepare
            example                  |      |
              |   |   | Data         |      |
              |   |   |   | value    | date | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        assert env.call("cast") == date(2025, 5, 5)
        assert env.call("cast", asttoexpr(parse("cast()"))) == date(2025, 5, 5)

    @pytest.mark.parametrize("value", ["2025-00-00", "abc", "12:00"])
    def test_cast_invalid_string_to_date(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type | prepare
            example                  |      |
              |   |   | Data         |      |
              |   |   |   | value    | date | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast")
        assert err_info.value.message == f"Unable to cast {value} to date type."

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast", asttoexpr(parse("cast()")))
        assert err_info.value.message == f"Unable to cast {value} to date type."

    @pytest.mark.parametrize("value", ["12:05", "2025-05-05T12:05:00.000000"])
    def test_cast_string_to_time(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type | prepare
            example                  |      |
              |   |   | Data         |      |
              |   |   |   | value    | time | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        assert env.call("cast") == time(12, 5)
        assert env.call("cast", asttoexpr(parse("cast()"))) == time(12, 5)

    @pytest.mark.parametrize("value", ["25:25", "abc"])
    def test_cast_invalid_string_to_time(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type | prepare
            example                  |      |
              |   |   | Data         |      |
              |   |   |   | value    | time | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast")
        assert err_info.value.message == f"Unable to cast {value} to time type."

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast", asttoexpr(parse("cast()")))
        assert err_info.value.message == f"Unable to cast {value} to time type."

    @pytest.mark.parametrize("value", ["2025-05-05T12:05:00.000000", "2025-05-05 12:05"])
    def test_cast_string_to_datetime(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type     | prepare
            example                  |          |
              |   |   | Data         |          |
              |   |   |   | value    | datetime | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        assert env.call("cast") == datetime(2025, 5, 5, 12, 5)
        assert env.call("cast", asttoexpr(parse("cast()"))) == datetime(2025, 5, 5, 12, 5)

    @pytest.mark.parametrize("value", ["2025-13-13T12:05:00.00000", "12:05"])
    def test_cast_invalid_string_to_datetime(self, rc: RawConfig, value: str):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type     | prepare
            example                  |          |
              |   |   | Data         |          |
              |   |   |   | value    | datetime | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=value,
            prop=model.properties["value"],
            data={"value": value},
            params={},
        )

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast")
        assert err_info.value.message == f"Unable to cast {value} to datetime type."

        with pytest.raises(UnableToCast) as err_info:
            env.call("cast", asttoexpr(parse("cast()")))
        assert err_info.value.message == f"Unable to cast {value} to datetime type."

    def test_cast_with_not_implemented_types(self, rc: RawConfig):
        context, manifest = load_manifest_and_context(
            rc,
            """
            d | r | b | m | property | type     | prepare
            example                  |          |
              |   |   | Data         |          |
              |   |   |   | value    | datetime | cast()
            """,
        )
        model = commands.get_model(context, manifest, "example/Data")
        env = ResultBuilder(context).init(
            this=date(2025, 5, 5),
            prop=model.properties["value"],
            data={"value": date(2025, 5, 5)},
            params={},
        )

        with pytest.raises(NotImplementedFeature) as err_info:
            env.call("cast")
        assert err_info.value.message == 'Prepare method "cast()" for data type datetime is not implemented yet.'

        with pytest.raises(NotImplementedFeature) as err_info:
            env.call("cast", asttoexpr(parse("cast()")))
        assert err_info.value.message == 'Prepare method "cast()" for data type datetime is not implemented yet.'
