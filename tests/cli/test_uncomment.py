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


def test_uncomment_base_direct_input(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | model    | property | type    | ref  | source | prepare                                        | level | access
    example              |          |         |      |        |                                                |       |
                         |          |         |      |        |                                                |       |
      |   |   | Country  |          |         |      |        |                                                |       |
                         |          | comment | base | author | insert(base: "dataset/gov/vssa/is/ds/Address") | 4     |
      |   |   |          | name     | string  |      |        |                                                |       | private
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

    base_rows = [row for row in rows if row.get("base") and not row.get("model") and not row.get("property")]
    comment_rows = [row for row in rows if row.get("type") == "comment"]

    assert len(base_rows) == 1, "base should be restored as its own row"
    assert base_rows[0]["base"] == "dataset/gov/vssa/is/ds/Address"
    assert base_rows[0]["level"] == ""
    assert len(comment_rows) == 0, "restore comment should have been removed"


def test_comment_uncomment_base_roundtrip(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """Full round-trip: comment drops a missing base, uncomment restores it."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b                              | m       | property | type   | ref | access
    example                                |         |          |        |     |
                                           |         |          |        |     |
      |   | dataset/gov/vssa/is/ds/Address |         |          |        |     |
      |   |                                | Country |          |        |     |
      |   |                                |         | name     | string |     | private
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

    base_rows = [row for row in rows if row.get("base") and not row.get("model") and not row.get("property")]
    comment_rows = [row for row in rows if row.get("type") == "comment"]

    assert len(base_rows) == 1, "base row should be restored above the model"
    assert base_rows[0]["base"] == "dataset/gov/vssa/is/ds/Address"
    assert base_rows[0]["level"] == ""
    assert len(comment_rows) == 0, "restore comment should have been removed"


def test_uncomment_base_uri_filter(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    """uncomment --uri only restores base comments that carry the matching URI tag."""
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | model    | property | type    | ref  | source | prepare                                        | level | access  | uri
    example              |          |         |      |        |                                                |       |         |
                         |          |         |      |        |                                                |       |         |
      |   |   | Country  |          |         |      |        |                                                |       |         |
                         |          | comment | base | author | insert(base: "dataset/gov/vssa/is/ds/Address") | 4     |         | http://foo
      |   |   |          | name     | string  |      |        |                                                |       | private |
      |   |   | City     |          |         |      |        |                                                |       |         |
                         |          | comment | base | author | insert(base: "dataset/gov/vssa/is/ds/Other")   | 4     |         | http://foo
      |   |   |          | name     | string  |      |        |                                                |       | private |
    """),
    )
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
    base_rows_bar = [row for row in rows_bar if row.get("base") and not row.get("model") and not row.get("property")]
    comment_rows_bar = [row for row in rows_bar if row.get("type") == "comment"]
    assert len(base_rows_bar) == 0, "no base should be restored when URI doesn't match"
    assert len(comment_rows_bar) == 2, "both comments should remain when URI doesn't match"

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
    base_rows_foo = [row for row in rows_foo if row.get("base") and not row.get("model") and not row.get("property")]
    comment_rows_foo = [row for row in rows_foo if row.get("type") == "comment"]
    assert len(comment_rows_foo) == 0, "all comments should be removed when URI matches"
    restored_bases = sorted(row["base"] for row in base_rows_foo)
    assert restored_bases == [
        "dataset/gov/vssa/is/ds/Address",
        "dataset/gov/vssa/is/ds/Other",
    ]


def test_uncomment_inserts_base_reset_between_restored_and_baseless_model(
    context: Context, rc, cli: SpintaCliRunner, tmp_path: Path
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | model    | property | type    | ref  | source | prepare                                      | level | access
    example              |          |         |      |        |                                              |       |
                         |          |         |      |        |                                              |       |
      |   |   | Building |          |         |      |        |                                              |       |
                         |          | comment | base | author | insert(base: "dataset/gov/vssa/is/ds/Other") | 4     |
      |   |   |          | name     | string  |      |        |                                              |       | private
      |   |   | Region   |          |         |      |        |                                              |       |
      |   |   |          | name     | string  |      |        |                                              |       | private
    """),
    )
    result = cli.invoke(
        rc,
        ["uncomment", "-o", tmp_path / "restored.csv", tmp_path / "manifest.csv"],
    )
    assert result.exit_code == 0, result.output

    rows = _read_csv(tmp_path / "restored.csv")

    base_or_model_rows = [
        row
        for row in rows
        if (row.get("base") and not row.get("model") and not row.get("property")) or row.get("model")
    ]
    sequence = [(row.get("base"), row.get("model")) for row in base_or_model_rows]

    assert sequence == [
        ("dataset/gov/vssa/is/ds/Other", ""),
        ("", "Building"),
        ("/", ""),
        ("", "Region"),
    ], f"unexpected base/model sequence: {sequence}"

    comment_rows = [row for row in rows if row.get("type") == "comment"]
    assert len(comment_rows) == 0, "restore comment should have been removed"


def test_uncomment_no_base_reset_between_models_with_own_restored_bases(
    context: Context, rc, cli: SpintaCliRunner, tmp_path: Path
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | model    | property | type    | ref  | source | prepare                                        | level | access
    example              |          |         |      |        |                                                |       |
                         |          |         |      |        |                                                |       |
      |   |   | Country  |          |         |      |        |                                                |       |
                         |          | comment | base | author | insert(base: "dataset/gov/vssa/is/ds/Address") | 4     |
      |   |   |          | name     | string  |      |        |                                                |       | private
      |   |   | City     |          |         |      |        |                                                |       |
                         |          | comment | base | author | insert(base: "dataset/gov/vssa/is/ds/Address") | 4     |
      |   |   |          | name     | string  |      |        |                                                |       | private
    """),
    )
    result = cli.invoke(
        rc,
        ["uncomment", "-o", tmp_path / "restored.csv", tmp_path / "manifest.csv"],
    )
    assert result.exit_code == 0, result.output

    rows = _read_csv(tmp_path / "restored.csv")

    reset_rows = [row for row in rows if row.get("base") == "/"]
    assert reset_rows == [], "no `/` reset should be inserted between models that both have their own base"

    base_rows = [
        row
        for row in rows
        if row.get("base") and row.get("base") != "/" and not row.get("model") and not row.get("property")
    ]
    assert len(base_rows) == 2, "both Country and City should have their bases restored"
    assert all(r["base"] == "dataset/gov/vssa/is/ds/Address" for r in base_rows)

    comment_rows = [row for row in rows if row.get("type") == "comment"]
    assert len(comment_rows) == 0, "all restore comments should have been removed"


def test_uncomment_ref_only_does_not_insert_base_resets(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | m | property | type    | ref  | source | prepare                                | level | access
    example                  |         |      |        |                                        |       |
                             |         |      |        |                                        |       |
      |   |   | City         |         |      |        |                                        |       |
      |   |   |   | country  | object  |      |        |                                        | 2     | private
                             | comment | type | author | update(type:"ref", ref:"example2/Country") | 4     |
    """),
    )
    cli.invoke(rc, ["uncomment", "-o", tmp_path / "restored.csv", tmp_path / "manifest.csv"])
    rows = _read_csv(tmp_path / "restored.csv")
    reset_rows = [row for row in rows if row.get("base") == "/"]
    assert reset_rows == [], "ref-only uncomment must not insert `/` rows"


def test_uncomment_base_with_ref_and_level_direct_input(context: Context, rc, cli: SpintaCliRunner, tmp_path: Path):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b | model   | property  | type    | ref  | source | prepare                                                              | level | access
    example              |          |         |      |        |                                                                      |       |
                         |          |         |      |        |                                                                      |       |
      |   |   | Country  |          |         |      |        |                                                                      |       |
                         |          | comment | base |        | insert(base: "dataset/gov/vssa/is/ds/Address", ref: "id", level: 4)  | 4     |
      |   |   |          | name     | string  |      |        |                                                                      |       | private
    """),
    )
    result = cli.invoke(
        rc,
        ["uncomment", "-o", tmp_path / "restored.csv", tmp_path / "manifest.csv"],
    )
    assert result.exit_code == 0, result.output

    rows = _read_csv(tmp_path / "restored.csv")
    base_rows = [row for row in rows if row.get("base") and not row.get("model") and not row.get("property")]
    comment_rows = [row for row in rows if row.get("type") == "comment"]

    assert len(base_rows) == 1
    assert base_rows[0]["base"] == "dataset/gov/vssa/is/ds/Address"
    assert base_rows[0]["ref"] == "id"
    assert base_rows[0]["level"] == "4"
    assert len(comment_rows) == 0


def test_comment_uncomment_base_with_ref_and_level_roundtrip(
    context: Context, rc, cli: SpintaCliRunner, tmp_path: Path
):
    create_tabular_manifest(
        context,
        tmp_path / "manifest.csv",
        striptable("""
    d | r | b                              | m       | property | type   | ref | level | access
    example                                |         |          |        |     |       |
                                           |         |          |        |     |       |
      |   | dataset/gov/vssa/is/ds/Address |         |          |        | id  | 4     |
      |   |                                | Country |          |        |     |       |
      |   |                                |         | name     | string |     |       | private
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
    base_rows = [row for row in rows if row.get("base") and not row.get("model") and not row.get("property")]
    comment_rows = [row for row in rows if row.get("type") == "comment"]

    assert len(base_rows) == 1
    assert base_rows[0]["base"] == "dataset/gov/vssa/is/ds/Address"
    assert base_rows[0]["ref"] == "id"
    assert base_rows[0]["level"] == "4"
    assert len(comment_rows) == 0
