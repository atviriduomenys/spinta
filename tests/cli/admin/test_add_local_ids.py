from pathlib import Path

import pytest

from spinta import commands
from spinta.cli.helpers.admin.components import Script
from spinta.cli.helpers.admin.scripts.add_local_ids import (
    add_explicit_id_properties,
)
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest, load_manifest_and_context
from spinta.testing.tabular import create_tabular_manifest
from spinta.types.datatype import Base32, String, Integer, UUID


@pytest.mark.parametrize(
    "pkey_type, expected_id_type, expected_required",
    [
        ("integer", Integer, False),
        ("string", Base32, False),
        ("uuid", UUID, False),
        ("integer required", Integer, True),
        ("string required", Base32, False),
        ("uuid required", UUID, True),
    ],
)
def test_add_local_ids_inserts_id_with_pkey_type(
    rc: RawConfig, pkey_type: str, expected_id_type: str, expected_required: bool
):
    context, manifest = load_manifest_and_context(
        rc,
        striptable(
            f"""
        d | r | b | m | property | type        | ref  | access
        ds/local_ids              |             |      |
          |   |   | Country       |             | code |
          |   |   |   | code      | {pkey_type} |      | open
          |   |   |   | name      | string      |      | open
        """
        ),
    )

    count_of_id = add_explicit_id_properties(context, manifest)

    assert count_of_id == 1
    prop = commands.get_model(context, manifest, "ds/local_ids/Country").id_prop
    assert prop.explicitly_given is True
    assert isinstance(prop.dtype, expected_id_type)
    assert prop.dtype.required is expected_required


def test_add_local_ids_inserts_base32_for_composite_pkey(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        striptable(
            """
        d | r | b | m | property | type    | ref        | access
        ds/local_ids              |         |            |
          |   |   | Country       |         | code, year |
          |   |   |   | code      | string  |            | open
          |   |   |   | year      | integer |            | open
        """
        ),
    )

    count_of_id = add_explicit_id_properties(context, manifest)

    assert count_of_id == 1
    prop = commands.get_model(context, manifest, "ds/local_ids/Country").id_prop
    assert prop.explicitly_given is True
    assert isinstance(prop.dtype, Base32)


def test_add_local_ids_skips_model_without_ref(rc: RawConfig, capsys: pytest.CaptureFixture):
    context, manifest = load_manifest_and_context(
        rc,
        striptable(
            """
        d | r | b | m | property | type   | ref | access
        ds/local_ids              |        |     |
          |   |   | Country       |        |     |
          |   |   |   | code      | string |     | open
        """
        ),
    )

    count_of_id = add_explicit_id_properties(context, manifest)

    assert count_of_id == 0
    err = capsys.readouterr().err
    assert 'Model "ds/local_ids/Country" has no `ref`, skipping.' in err


def test_add_local_ids_skips_model_with_existing_id(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        striptable(
            """
        d | r | b | m | property | type   | ref  | access
        ds/local_ids              |        |      |
          |   |   | Country       |        | code |
          |   |   |   | _id       | string |      | open
          |   |   |   | code      | string |      | open
        """
        ),
    )

    count_of_id = add_explicit_id_properties(context, manifest)

    assert count_of_id == 0
    prop = commands.get_model(context, manifest, "ds/local_ids/Country").id_prop
    assert prop.explicitly_given is True
    assert isinstance(prop.dtype, String)


def test_add_local_ids_no_manifest_errors(context: Context, cli: SpintaCliRunner):
    result = cli.invoke(
        context.get("rc"),
        ["admin", Script.ADD_LOCAL_IDS.value],
        fail=False,
    )
    assert result.exit_code == 1
    assert "`add_local_ids` requires at least one source manifest path." in result.stderr


def test_add_local_ids_cli_multiple_files_writes_to_first(
    context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path
):
    create_tabular_manifest(
        context,
        tmp_path / "a.csv",
        striptable("""
    d | r | b | m | property  | type   | ref  | access
    ds/a                      |        |      |
      |   |   | Alpha         |        | code |
      |   |   |   | code      | string |      | open
    """),
    )
    create_tabular_manifest(
        context,
        tmp_path / "b.csv",
        striptable("""
    d | r | b | m | property  | type | ref  | access
    ds/b                      |      |      |
      |   |   | Beta          |      | id   |
      |   |   |   | id        | uuid |      | open
    """),
    )

    result = cli.invoke(
        rc,
        [
            "admin",
            Script.ADD_LOCAL_IDS.value,
            "--manifests",
            str(tmp_path / "a.csv"),
            "--manifests",
            str(tmp_path / "b.csv"),
        ],
    )

    assert result.exit_code == 0
    assert "Added 2 `_id` row(s)" in result.output

    manifest = load_manifest(rc, tmp_path / "a.csv")
    assert (
        manifest
        == """
    d | r | b | m | property  | type   | ref  | access
    ds/a                      |        |      |
                              |        |      |
      |   |   | Alpha         |        | code |
      |   |   |   | _id       | base32 |      |
      |   |   |   | code      | string |      | open
    ds/b                      |        |      |
                              |        |      |
      |   |   | Beta          |        | id   |
      |   |   |   | _id       | uuid   |      |
      |   |   |   | id        | uuid   |      | open
    """
    )


def test_add_local_ids_cli_with_output_file(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type | ref | access
    ds/local_ids              |      |     |
      |   |   | Country       |      | id  |
      |   |   |   | id        | uuid |     | open
    """),
    )

    result = cli.invoke(
        rc,
        [
            "admin",
            Script.ADD_LOCAL_IDS.value,
            "--manifests",
            str(tmp_path / "manifest.csv"),
            "-o",
            str(tmp_path / "output.csv"),
        ],
    )

    assert result.exit_code == 0
    assert "Added 1 `_id` row(s)" in result.output

    manifest = load_manifest(rc, tmp_path / "output.csv")
    assert (
        manifest
        == """
    d | r | b | m | property  | type | ref | access
    ds/local_ids              |      |     |
                              |      |     |
      |   |   | Country       |      | id  |
      |   |   |   | _id       | uuid |     |
      |   |   |   | id        | uuid |     | open
    """
    )


def test_add_local_ids_cli_nothing_to_add(context: Context, rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property  | type   | ref  | access
    ds/local_ids              |        |      |
      |   |   | Country       |        | code |
      |   |   |   | _id       | base32 |      | open
      |   |   |   | code      | string |      | open
    """),
    )

    result = cli.invoke(
        rc,
        ["admin", Script.ADD_LOCAL_IDS.value, "--manifests", str(tmp_path / "manifest.csv")],
    )

    assert result.exit_code == 0
    assert "No `_id` rows to add" in result.output
