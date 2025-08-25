from spinta import commands
from spinta.core.enums import Mode
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context


def test_html(rc: RawConfig):
    table = """
    d | r | b | m | property | type   | ref  | source                         | access
    example                  |        |      |                                |
      | data                 | html   |      | https://example.com/index.html |
                             |        |      |                                |
      |   |   | City         |        | name | /xpath/to/table/tr             |
      |   |   |   | name     | string |      | td                             | open
    """
    context, manifest = load_manifest_and_context(rc, table, mode=Mode.external)
    backend = commands.get_model(context, manifest, "example/City").backend
    assert backend.type == "html"
    assert manifest == table

    commands.wait(context, backend)
