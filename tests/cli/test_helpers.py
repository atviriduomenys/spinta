from pathlib import Path

from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.config import ResourceTuple
from spinta.core.context import configure_context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import create_test_context
from spinta.testing.tabular import create_tabular_manifest


def test_configure(tmp_path: Path, rc: RawConfig):
    context: Context = create_test_context(rc)
    create_tabular_manifest(
        context,
        tmp_path / "m1.csv",
        striptable("""
    d | r | b | m | property | type   | source
    datasets/1               |        | 
      | data                 | sql    | 
                             |        | 
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    """),
    )

    create_tabular_manifest(
        context,
        tmp_path / "m2.csv",
        striptable("""
    d | r | b | m | property | type   | source
    datasets/2               |        | 
      | data                 | sql    | 
                             |        | 
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    """),
    )
    context = configure_context(
        context,
        [
            str(tmp_path / "m1.csv"),
            str(tmp_path / "m2.csv"),
        ],
    )
    store = prepare_manifest(context, verbose=False)
    assert (
        store.manifest
        == """
    d | r | b | m | property | type   | source
    datasets/1               |        |
      | data                 | sql    |
                             |        |
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    datasets/2               |        |
      | data                 | sql    |
                             |        |
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    """
    )


def test_configure_with_resource(rc: RawConfig):
    context: Context = create_test_context(rc)
    context = configure_context(
        context,
        resources=[
            ResourceTuple("sql", "sqlite://"),
        ],
    )
    store = prepare_manifest(context, verbose=False)
    assert (
        store.manifest
        == """
    d | r | b | m | property | type | ref  | source     | prepare
    datasets/gov/example     |      |      |            |
      | resource1            | sql  |      | sqlite://  |
    """
    )


def test_configure_with_resource_backend(rc: RawConfig):
    rc = rc.fork(
        {
            "backends": {
                "mydb": {
                    "type": "sql",
                    "dsn": "sqlite://",
                }
            }
        }
    )
    context: Context = create_test_context(rc)
    context = configure_context(
        context,
        resources=[
            ResourceTuple("sql", "mydb"),
        ],
    )
    store = prepare_manifest(context, verbose=False)
    assert (
        store.manifest
        == """
    d | r | b | m | property | type | ref  | source     | prepare
    datasets/gov/example     |      |      |            |
      | resource1            | sql  | mydb |            |
    """
    )


def test_ascii_manifest(tmp_path: Path, rc: RawConfig):
    manifest = """
    d | r | b | m | property | type   | source
    datasets/1               |        |
      | data                 | sql    |
                             |        |
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    """
    manifest_file = tmp_path / "ascii.txt"
    manifest_file.write_text(striptable(manifest), encoding="utf-8")
    context: Context = create_test_context(rc)
    context = configure_context(context, [str(manifest_file)])
    store = prepare_manifest(context, verbose=False)
    assert store.manifest == manifest
