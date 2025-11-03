from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.core.ufuncs import asttoexpr
from spinta.spyna import parse
from spinta.testing.manifest import prepare_manifest


def test_get_param_http_headers(rc: RawConfig) -> None:
    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type      | ref   | source         | access | prepare
        example                  | dataset   |       |                |        |
          | resource             | dask/json |       | some_json.json |        | 
          |   |   |   |          | param     | test1 | header_name    |        | header("header_value")
          |   |   |   |          | param     | test2 | foo            |        | header("bar")
          |   |   |   |          | param     | test3 | foo2           |        | creds("bar2").header()
          |   |   |   |          | param     | name  | name_test      |        |
          |   |   | City         |           |       | /              | open   |
          |   |   |   | name     | string    |       | name           |        |
        """,
        mode=Mode.external,
    )

    resource = commands.get_dataset(context, manifest, "example").resources["resource"]
    assert resource.get_param_http_headers() == {
        "header_name": "header_value",
        "foo": "bar",
        "foo2": asttoexpr(parse('creds("bar2")')),
    }
