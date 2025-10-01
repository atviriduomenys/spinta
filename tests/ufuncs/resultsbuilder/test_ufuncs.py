import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.exceptions import InvalidBase64String
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
