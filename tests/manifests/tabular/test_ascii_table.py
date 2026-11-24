import random

import pytest

from spinta.manifests.tabular.ascii_table import ASCII_TABLE_WIDTH, escape, unescape, wrap
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import (
    read_ascii_tabular_table,
    render_tabular_manifest_rows,
    striptable,
    torow,
)


def _rows(*rows: dict) -> list:
    return [torow(DATASET, row) for row in rows]


def _render(*rows: dict, cols=None, width=ASCII_TABLE_WIDTH) -> str:
    return render_tabular_manifest_rows(_rows(*rows), cols, width=width)


def test_spec_example_round_trips():
    text = striptable(r"""
    d | r | m | property | type    | ref     | access | uri                        | title       | description
    locn                 |         |         | open   |                            | Location    | Location dataset
      |   |   |          | prefix  | locn    |        | http://www.w3.org/ns/locn# |             |
      |   |   |          |         | dct     |        | http://purl.org/dc/terms/  |             |
      |   |   |          |         | skos    |        | http://www.w3.org/2004/02/ |             | \
      :   :   :          :         :         :        : skos/core#                 :             :
      |   |   |          |         |         |        |                            |             |
      | locn_src         |         | default | open   |                            | Locn source |
      |   |   |          |         |         |        |                            |             |
      |   | Country      |         | id      |        | locn:Country               | Country     |
      |   |   | id       | integer |         |        | dct:identifier             | Identifier  |
      |   |   | code     | string  |         |        | skos:notation              | Code        |
    """)
    cols, rows = read_ascii_tabular_table(text)

    assert cols == ["dataset", "resource", "model", "property", "type", "ref", "access", "uri", "title", "description"]
    assert render_tabular_manifest_rows(rows, cols) == text
    assert render_tabular_manifest_rows(rows) == text


def test_columns_without_values_are_hidden():
    assert _render(
        {"dataset": "locn", "title": "Location"},
        {"dataset": "", "model": "Country", "title": "Country"},
    ) == striptable("""
        d | m       | title
        locn        | Location
          | Country | Country
    """)


def test_requested_columns_are_shown_even_when_empty_and_keep_dataset_order():
    assert _render({"dataset": "locn"}, cols=["title", "dataset", "uri"]) == striptable("""
        d    | uri | title
        locn |     |
    """)


def test_dimension_value_is_merged_up_to_type():
    assert _render(
        {"resource": "locn_src", "type": "", "ref": "default"},
        {"model": "Country", "ref": "id"},
        {"property": "id", "type": "integer"},
    ) == striptable("""
        r | m | property | type    | ref
        locn_src         |         | default
          | Country      |         | id
          |   | id       | integer |
    """)


def test_no_merge_when_dimensions_are_empty():
    assert _render(
        {"model": "M"},
        {"type": "prefix", "ref": "locn", "uri": "http://www.w3.org/ns/locn#"},
        {"type": "prefix", "ref": "dct", "uri": "http://purl.org/dc/terms/"},
    ) == striptable("""
        m | type   | ref  | uri
        M |        |      |
          | prefix | locn | http://www.w3.org/ns/locn#
          | prefix | dct  | http://purl.org/dc/terms/
    """)


def test_property_is_widened_for_long_merged_values():
    text = _render(
        {"dataset": "example", "title": "Example"},
        {"model": "Country", "property": "", "title": "Country"},
        {"property": "a_very_long_property_name", "type": "string"},
    )
    assert text == striptable("""
        d | m | property                  | type   | title
        example                           |        | Example
          | Country                       |        | Country
          |   | a_very_long_property_name | string |
    """)


def test_long_value_is_wrapped_into_continuation_lines():
    text = _render(
        {"model": "Country", "uri": "http://www.w3.org/2004/02/skos/core#"},
        {"model": "Other", "uri": "http://purl.org/dc/terms/"},
        width=40,
    )
    assert text == striptable(r"""
        m       | uri
        Country | http://www.w3.org/2004/02/ \
                : skos/core#
        Other   | http://purl.org/dc/terms/
    """)
    cols, rows = read_ascii_tabular_table(text)
    assert [row["uri"] for row in rows] == [
        "http://www.w3.org/2004/02/skos/core#",
        "http://purl.org/dc/terms/",
    ]


def test_separators_inside_values_are_escaped():
    text = _render({"model": "M", "title": "a | b : c", "description": "x|y"})
    assert text == striptable(r"""
        m | title       | description
        M | a \| b \: c | x\|y
    """)
    cols, rows = read_ascii_tabular_table(text)
    assert rows[0]["title"] == "a | b : c"
    assert rows[0]["description"] == "x|y"


@pytest.mark.parametrize(
    "value",
    [
        "plain",
        "a|b",
        "a : b",
        ":",
        "a:b",
        "back\\slash",
        "trailing\\",
        "\\|",
        "\\\\",
    ],
)
def test_escape_round_trips(value: str):
    assert unescape(escape(value)) == value


def test_escape_keeps_colons_inside_words():
    assert escape("locn:Country") == "locn:Country"
    assert escape("a : b") == "a \\: b"


def test_wrap_joins_back_to_the_value():
    value = "http://www.w3.org/2004/02/skos/core#"
    pieces = wrap(value, 26)
    assert pieces == ["http://www.w3.org/2004/02/", "skos/core#"]
    assert "".join(pieces) == value


def test_wrap_does_not_split_at_spaces():
    pieces = wrap("Location dataset of the country", 9)
    assert all(not piece.startswith(" ") and not piece.endswith(" ") for piece in pieces)
    assert "".join(pieces).replace(" ", "") == "Locationdatasetofthecountry"


def test_table_is_not_narrower_than_header():
    text = _render({"model": "M", "title": "t", "description": "d"}, width=1)
    header = text.splitlines()[0]
    assert len(header) == len("m | title | description")


def test_width_wraps_text_columns_but_keeps_short_ones():
    text = _render(
        {"model": "Country", "type": "string", "source": "https://example.com/" + "x" * 80},
        width=ASCII_TABLE_WIDTH,
    )
    assert all(len(line) <= ASCII_TABLE_WIDTH + 2 for line in text.splitlines())
    assert "Country" in text.splitlines()[1]
    assert "string" in text.splitlines()[1]


@pytest.mark.parametrize("seed", range(3))
def test_random_tables_round_trip(seed: int):
    alphabet = "ab |:\\/.#-_x1" + "  "
    rng = random.Random(seed)

    def value(max_len: int) -> str:
        return "".join(rng.choice(alphabet) for _ in range(rng.randint(1, max_len))).strip()

    rows = []
    for _ in range(rng.randint(1, 6)):
        row = {}
        if rng.random() < 0.6:
            row[rng.choice(["dataset", "resource", "base", "model", "property"])] = value(50)
        for col in ["type", "ref", "source", "access", "uri", "title", "description"]:
            if rng.random() < 0.4:
                row[col] = value(70)
        if rng.random() < 0.3:
            row["id"] = value(2)
        rows.append(row)
    if not any(v for row in rows for v in row.values()):
        rows[0]["title"] = "x"

    for width in (30, 60, ASCII_TABLE_WIDTH):
        text = render_tabular_manifest_rows(_rows(*rows), width=width)
        cols, back = read_ascii_tabular_table(text)
        assert render_tabular_manifest_rows(back, cols, width=width) == text
        for row, read in zip(rows, back):
            for col in cols:
                assert read[col] == row.get(col, "")
