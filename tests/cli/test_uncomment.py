from pathlib import Path

from spinta.components import Context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.tabular import create_tabular_manifest
from tests.cli.conftest import _read_csv


def test_uncomment_direct_input(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """uncomment works directly on a manifest that already has type=comment restore rows."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref  | source | prepare                                | level | access
    example                  |         |      |        |                                        |       |
                             |         |      |        |                                        |       |
      |   |   | City         |         |      |        |                                        |       |
      |   |   |   | name     | string  |      |        |                                        |       | private
      |   |   |   | country  | object  |      |        |                                        | 2     | private
                             | comment | type | author | update(type:"ref", ref:"example2/Country") | 4     |
    """),
    )

    result = cli.invoke(
        rc,
        [
            "uncomment",
            "-o",
            tmp_path / "restored.csv",
            tmp_path / "manifest.csv",
        ],
    )
    assert result.exit_code == 0, result.output

    rows = _read_csv(tmp_path / "restored.csv")
    property_rows = [row for row in rows if row.get("property") == "country"]
    comment_rows = [row for row in rows if row.get("type") == "comment"]

    assert len(property_rows) == 1
    property_row = property_rows[0]
    assert property_row["type"] == "ref"
    assert property_row["ref"] == "example2/Country"
    assert property_row["level"] == "4"
    assert len(comment_rows) == 0, "restore comment should have been removed"


def test_comment_uncomment_roundtrip(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """Full round-trip: comment downgrades missing refs, uncomment restores them."""
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

    cli.invoke(
        rc,
        ["comment", "missing-external-refs", "-o", tmp_path / "commented.csv", tmp_path / "manifest.csv"],
    )
    result = cli.invoke(
        rc,
        ["uncomment", "-o", tmp_path / "restored.csv", tmp_path / "commented.csv"],
    )
    assert result.exit_code == 0, result.output

    rows = _read_csv(tmp_path / "restored.csv")
    property_rows = [row for row in rows if row.get("property") == "country"]
    comment_rows = [row for row in rows if row.get("type") == "comment"]

    assert len(property_rows) == 1
    property_row = property_rows[0]
    assert property_row["type"] == "ref"
    assert property_row["ref"] == "example2/Country"
    assert property_row["level"] == "4"
    assert len(comment_rows) == 0, "restore comment should have been removed"


def test_uncomment_preserves_non_restore_comments(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """Regular comment rows (no restore prepare) are kept; only restore comments are dropped."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref  | source | prepare                                | level | access  | description
    example                  |         |      |        |                                        |       |         |
                             |         |      |        |                                        |       |         |
      |   |   | City         |         |      |        |                                        |       |         |
      |   |   |   | country  | object  |      |        |                                        | 2     | private |
                             | comment | type | author | update(type:"ref", ref:"example2/Country") | 4     |         |
                             | comment |      | alice  |                                        |       |         | needs review
    """),
    )

    result = cli.invoke(
        rc,
        [
            "uncomment",
            "-o",
            tmp_path / "restored.csv",
            tmp_path / "manifest.csv",
        ],
    )
    assert result.exit_code == 0, result.output

    rows = _read_csv(tmp_path / "restored.csv")
    comment_rows = [row for row in rows if row.get("type") == "comment"]

    assert len(comment_rows) == 1, "plain comment should be preserved, only restore comment removed"
    comment_row = comment_rows[0]
    assert comment_row["source"] == "alice"
    assert comment_row["description"] == "needs review"
    assert not comment_row.get("prepare"), "preserved comment should have no prepare"


def test_uncomment_uri_filter(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """uncomment --uri only restores comments that carry the matching URI tag, when URI is given."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref  | source | prepare                                | level | access  | uri
    example                  |         |      |        |                                        |       |         |
                             |         |      |        |                                        |       |         |
      |   |   | City         |         |      |        |                                        |       |         |
      |   |   |   | country  | object  |      |        |                                        | 2     | private |
                             | comment | type | author | update(type:"ref", ref:"example2/Country") | 4     |         | http://foo
      |   |   |   | capital  | object  |      |        |                                        | 2     | private |
                             | comment | type | author | update(type:"ref", ref:"example3/Capital") | 4     |         | http://foo
    """),
    )

    # Restore only comments tagged with http://bar (should match nothing -> file unchanged)
    result = cli.invoke(
        rc,
        [
            "uncomment",
            "--uri",
            "http://bar",
            "-o",
            tmp_path / "restored_bar.csv",
            tmp_path / "manifest.csv",
        ],
    )
    assert result.exit_code == 0, result.output

    rows_bar = _read_csv(tmp_path / "restored_bar.csv")
    comment_rows_bar = [row for row in rows_bar if row.get("type") == "comment"]
    assert len(comment_rows_bar) == 2, "both comments should remain when URI doesn't match"

    # Restore only comments tagged with http://foo (should restore both props)
    result = cli.invoke(
        rc,
        [
            "uncomment",
            "--uri",
            "http://foo",
            "-o",
            tmp_path / "restored_foo.csv",
            tmp_path / "manifest.csv",
        ],
    )
    assert result.exit_code == 0, result.output

    rows_foo = _read_csv(tmp_path / "restored_foo.csv")
    comment_rows_foo = [row for row in rows_foo if row.get("type") == "comment"]
    assert len(comment_rows_foo) == 0, "all comments should be removed when URI matches"
    property_country = next(row for row in rows_foo if row.get("property") == "country")
    property_capital = next(row for row in rows_foo if row.get("property") == "capital")

    assert property_country["type"] == "ref"
    assert property_country["ref"] == "example2/Country"
    assert property_capital["type"] == "ref"
    assert property_capital["ref"] == "example3/Capital"
