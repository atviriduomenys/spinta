from pathlib import Path

import pytest

from spinta import commands
from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.admin.scripts.remove_local_ids import (
    remove_explicit_id_properties,
)
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest, load_manifest_and_context
from spinta.testing.tabular import create_tabular_manifest


def test_remove_local_ids_removes_explicit_id(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        striptable(
            """
        d | r | b | m | property  | type   | ref  | access
        ds/local_ids              |        |      |
          |   |   | Country       |        | code |
          |   |   |   | _id       | base32 |      | open
          |   |   |   | code      | string |      | open
        """
        ),
    )

    count_of_id = remove_explicit_id_properties(context, manifest)

    assert count_of_id == 1
    prop = commands.get_model(context, manifest, "ds/local_ids/Country").properties["_id"]
    assert prop.explicitly_given is False


def test_remove_local_ids_skips_model_without_explicit_id(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        striptable(
            """
        d | r | b | m | property  | type   | ref  | access
        ds/local_ids              |        |      |
          |   |   | Country       |        | code |
          |   |   |   | code      | string |      | open
        """
        ),
    )

    count_of_id = remove_explicit_id_properties(context, manifest)

    assert count_of_id == 0


@pytest.mark.parametrize(
    "id_type",
    ["integer", "base32", "uuid"],
)
def test_remove_local_ids_removes_explicit_id_of_any_type(rc: RawConfig, id_type: str):
    context, manifest = load_manifest_and_context(
        rc,
        striptable(
            f"""
        d | r | b | m | property  | type      | ref  | access
        ds/local_ids              |           |      |
          |   |   | Country       |           | code |
          |   |   |   | _id       | {id_type} |      |
          |   |   |   | code      | {id_type} |      | open
        """
        ),
    )

    count_of_id = remove_explicit_id_properties(context, manifest)

    assert count_of_id == 1
    prop = commands.get_model(context, manifest, "ds/local_ids/Country").properties["_id"]
    assert prop.explicitly_given is False


def test_remove_local_ids_no_manifest_errors(context: Context, cli: SpintaCliRunner):
    result = cli.invoke(
        context.get("rc"),
        ["admin", Script.REMOVE_LOCAL_IDS.value],
        fail=False,
    )
    assert result.exit_code == 1
    assert "`remove_local_ids` requires at least one source manifest path." in result.stderr


def test_remove_local_ids_cli_multiple_files_writes_to_first(
    context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    create_tabular_manifest(
        context,
        tmp_path / "a.csv",
        striptable("""
    d | r | b | m | property  | type   | ref  | access
    ds/a                      |        |      |
      |   |   | Alpha         |        | code |
      |   |   |   | _id       | base32 |      |
      |   |   |   | code      | string |      | open
    """),
    )
    create_tabular_manifest(
        context,
        tmp_path / "b.csv",
        striptable("""
    d | r | b | m | property  | type | ref | access
    ds/b                      |      |     |
      |   |   | Beta          |      | id  |
      |   |   |   | _id       | uuid |     |
      |   |   |   | id        | uuid |     | open
    """),
    )

    result = cli.invoke(
        rc,
        [
            "admin",
            Script.REMOVE_LOCAL_IDS.value,
            "--manifests",
            str(tmp_path / "a.csv"),
            "--manifests",
            str(tmp_path / "b.csv"),
        ],
    )

    assert result.exit_code == 0
    assert "Removed 2 `_id` row(s)" in result.output

    manifest = load_manifest(rc, tmp_path / "a.csv")
    assert (
        manifest
        == """
    d | r | b | m | property  | type   | ref  | access
    ds/a                      |        |      |
                              |        |      |
      |   |   | Alpha         |        | code |
      |   |   |   | code      | string |      | open
    ds/b                      |        |      |
                              |        |      |
      |   |   | Beta          |        | id   |
      |   |   |   | id        | uuid   |      | open
    """
    )


def test_remove_local_ids_cli_with_output_file(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type   | ref  | access
    ds/local_ids              |        |      |
      |   |   | Country       |        | code |
      |   |   |   | _id       | base32 |      |
      |   |   |   | code      | string |      | open
    """),
    )

    result = cli.invoke(
        rc,
        [
            "admin",
            Script.REMOVE_LOCAL_IDS.value,
            "--manifests",
            str(tmp_path / "manifest.csv"),
            "-o",
            str(tmp_path / "output.csv"),
        ],
    )

    assert result.exit_code == 0
    assert "Removed 1 `_id` row(s)" in result.output

    manifest = load_manifest(rc, tmp_path / "output.csv")
    assert (
        manifest
        == """
    d | r | b | m | property  | type   | ref  | access
    ds/local_ids              |        |      |
                              |        |      |
      |   |   | Country       |        | code |
      |   |   |   | code      | string |      | open
    """
    )


def test_remove_local_ids_cli_nothing_to_remove(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type   | ref  | access
    ds/local_ids              |        |      |
                              |        |      |
      |   |   | Country       |        | code |
      |   |   |   | code      | string |      | open
    """),
    )

    result = cli.invoke(
        rc,
        ["admin", Script.REMOVE_LOCAL_IDS.value, "--manifests", str(tmp_path / "manifest.csv")],
    )

    assert result.exit_code == 0
    assert "No `_id` rows to remove" in result.output
