from spinta import exceptions, commands
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.utils.errors import report_error


def test_report_error__id(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property | type   | access
    datasets/gov/example     |        |
      |   |   | Country      |        |
      |   |   |   | name     | string | open
    """,
    )

    model = commands.get_model(context, manifest, "datasets/gov/example/Country")
    prop = model.properties["name"]

    exc = exceptions.InvalidValue(prop.dtype, value=42)
    exc = report_error(
        exc,
        {
            "_id": "a5c3bebb-3f49-4337-8264-4364a596f56b",
        },
        stop_on_error=False,
    )

    assert exc.context["id"] == "a5c3bebb-3f49-4337-8264-4364a596f56b"
