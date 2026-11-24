"""Every inline table of the test suite round-trips through `simple` (4.8).

The tables are the ASCII manifests written as strings in `tests/**/*.py`. For
each table that reads without errors, the rows rebuilt from `simple` are the
rows of the table, or read back to the same document (equivalent). A table
with read errors only has to give a valid document with well-formed errors.
"""

import ast
import collections
import pathlib

from spinta.manifests.simple.from_tabular import tabular_to_simple
from spinta.manifests.simple.helpers import read_simple_rows
from spinta.manifests.simple.schema import document_columns, validate_document
from spinta.manifests.tabular.helpers import (
    _read_ascii_tabular_manifest,
    normalizes_columns,
    striptable,
)

ROOT = pathlib.Path(__file__).resolve().parents[3]


def _is_table(text: str) -> bool:
    lines = [line for line in striptable(text).splitlines() if line.strip()]
    if len(lines) < 2 or "|" not in lines[0]:
        return False
    try:
        columns = normalizes_columns(lines[0].split("|"), check_column_names=True)
    except Exception:
        return False
    return any(c in columns for c in ("dataset", "model", "property", "resource"))


def _harvest():
    for path in sorted(ROOT.joinpath("tests").rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and "|" in node.value:
                if _is_table(node.value):
                    yield f"{path.relative_to(ROOT)}:{node.lineno}", striptable(node.value)


def _rows(text):
    return _read_ascii_tabular_manifest(striptable(text).splitlines(), check_column_names=False)


def _non_blank(rows):
    return [[str(c) for c in r] for r in rows if any(r)]


def _semantic(doc):
    """Elements without `index`: what the document means, not where its rows are."""
    out = {}
    for type_ in doc:
        if type_ == "spinta":
            continue
        for path, element in doc[type_].items():
            parent = element["parent"]
            out[(type_, path)] = (
                (parent["type"], parent["path"]) if parent else None,
                element["given"],
            )
    return out


def _check_table(where: str, text: str) -> str | None:
    """`None` when the table round-trips, otherwise the reason it does not."""
    doc = tabular_to_simple(_rows(text), type="ascii", version="2.2.0")
    try:
        validate_document(doc)
    except Exception as e:  # noqa: BLE001 - reported with its table
        return f"{where}: invalid document: {e}"

    has_errors = any(element["errors"] for type_ in doc if type_ != "spinta" for element in doc[type_].values())
    if has_errors:
        return None

    header = document_columns(doc)
    rebuilt = _non_blank(cells for _line, cells in read_simple_rows(doc))
    original = _non_blank(cells for _line, cells in _rows(text))
    if rebuilt == original:
        return None

    # The same rows read back to the same meaning (repeated keywords are written
    # in continuation style, which reads the same way).
    body = [["" if c is None else str(c) for c in r] for r in rebuilt[1:]]
    again = tabular_to_simple(
        iter([("1", header)] + [(str(i + 2), r) for i, r in enumerate(body)]),
        type="ascii",
        version="2.2.0",
    )
    if _semantic(again) == _semantic(doc):
        return None
    return f"{where}: rows differ after the round trip"


def test_every_test_table_round_trips_through_simple():
    tables = list(_harvest())
    assert len(tables) > 1000, "the harvest should find the test suite's tables"

    outcome = collections.Counter()
    failures = []
    for where, text in tables:
        try:
            problem = _check_table(where, text)
        except Exception as e:  # noqa: BLE001 - a crash is a failure of the table
            problem = f"{where}: crashed with {type(e).__name__}: {e}"
        if problem:
            failures.append(problem)
            outcome["failed"] += 1
        else:
            outcome["ok"] += 1

    assert not failures, f"{dict(outcome)}\n" + "\n".join(failures[:20])
