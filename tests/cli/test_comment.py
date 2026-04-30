from datetime import datetime
from pathlib import Path

from spinta.components import Context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest
from spinta.testing.tabular import create_tabular_manifest
from tests.cli.conftest import _read_csv


def test_comment_missing_external_refs(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """comment missing-external-refs downgrades undeclared refs to object (level=2) and adds a restore comment."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref              | access
    example                  |        |                  |
                             |        |                  |
      |   |   | City         |        |                  |
      |   |   |   | name     | string |                  | private
      |   |   |   | country  | ref    | example2/Country | private
    """),
    )

    result = cli.invoke(
        rc,
        [
            "comment",
            "missing-external-refs",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )
    assert result.exit_code == 0, result.output

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref  | source | prepare                                | level | access  | description
    example                  |         |      |        |                                        |       |         |
                             |         |      |        |                                        |       |         |
      |   |   | City         |         |      |        |                                        |       |         |
      |   |   |   | name     | string  |      |        |                                        |       | private |
      |   |   |   | country  | object  |      |        |                                        | 2     | private |
                             | comment | type | author | update(type:"ref", ref:"example2/Country") | 4     |         |
    """
    )

    # Verify the title column carries an ISO 8601 creation date
    rows = _read_csv(tmp_path / "result.csv")
    comment_row = next(row for row in rows if row.get("type") == "comment")
    assert comment_row["title"], "comment title (creation date) must be set"
    datetime.fromisoformat(comment_row["title"])  # Datetime needs to conform to ISO 8601


def test_comment_all_optional_values_given(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """--author, --uri, and --description are all stored on the comment row, along with an ISO 8601 title date."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref              | access
    example                  |        |                  |
                             |        |                  |
      |   |   | City         |        |                  |
      |   |   |   | country  | ref    | example2/Country | private
    """),
    )

    result = cli.invoke(
        rc,
        [
            "comment",
            "missing-external-refs",
            "--author",
            "john",
            "--uri",
            "http://example.com/issue/1",
            "--description",
            "Waiting for example2 dataset",
            "-o",
            tmp_path / "result.csv",
            tmp_path / "manifest.csv",
        ],
    )
    assert result.exit_code == 0, result.output

    manifest = load_manifest(rc, tmp_path / "result.csv")
    assert (
        manifest
        == """
    d | r | b | m | property | type    | ref  | source | prepare                                | level | access  | uri                         | description
    example                  |         |      |        |                                        |       |         |                             |
                             |         |      |        |                                        |       |         |                             |
      |   |   | City         |         |      |        |                                        |       |         |                             |
      |   |   |   | country  | object  |      |        |                                        | 2     | private |                             |
                             | comment | type | john   | update(type:"ref", ref:"example2/Country") | 4     |         | http://example.com/issue/1  | Waiting for example2 dataset
    """
    )

    rows = _read_csv(tmp_path / "result.csv")
    comment_rows = [row for row in rows if row.get("type") == "comment"]
    assert len(comment_rows) == 1
    comment_row = comment_rows[0]
    assert comment_row["title"], "comment title (creation date) must be set"
    datetime.fromisoformat(comment_row["title"])  # raises ValueError if not valid ISO 8601


def test_comment_idempotent(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """Running comment twice on the same file does not create duplicate restore comments."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref              | access
    example                  |        |                  |
                             |        |                  |
      |   |   | City         |        |                  |
      |   |   |   | country  | ref    | example2/Country | private
    """),
    )

    cli.invoke(
        rc,
        ["comment", "missing-external-refs", "-o", tmp_path / "commented.csv", tmp_path / "manifest.csv"],
    )
    cli.invoke(
        rc,
        ["comment", "missing-external-refs", "-o", tmp_path / "commented2.csv", tmp_path / "commented.csv"],
    )

    rows1 = _read_csv(tmp_path / "commented.csv")
    rows2 = _read_csv(tmp_path / "commented2.csv")

    comment_rows1 = [row for row in rows1 if row.get("type") == "comment"]
    comment_rows2 = [row for row in rows2 if row.get("type") == "comment"]

    assert len(comment_rows1) == 1
    assert len(comment_rows2) == 1, "second run must not duplicate the restore comment"
    assert comment_rows1[0]["prepare"] == comment_rows2[0]["prepare"]


def test_comment_unknown_part_raises(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """comment with an unsupported `part` argument should exit with an error."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type   | ref | access
    example                  |        |     |
    """),
    )

    result = cli.invoke(
        rc,
        [
            "comment",
            "unknown-part",
            tmp_path / "manifest.csv",
        ],
        fail=False,
    )
    assert result.exit_code != 0
