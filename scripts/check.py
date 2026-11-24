#!/usr/bin/env python3
"""Round-trip check of manifest CSV files through the `simple` format.

For every `.csv` under INPUT_DIR whose header has the columns `dataset`,
`model`, `property`, `type`, `level` and `access` (others may be present):

1. `spinta.copy()` reads the CSV to `simple`, saved as OUTPUT_DIR/<rel>.simple.json.
2. The `simple` document is counted: elements and errors per type (document-level
   errors count as `manifest`), and stored errors per error code.
3. `spinta.copy()` writes the `simple` file back to CSV, saved as
   OUTPUT_DIR/<rel>. If the result differs from the original, a unified diff
   is saved as OUTPUT_DIR/<rel without .csv>.diff.

At the end a summary of all files is printed to stdout as Markdown tables.

Usage:
    nix develop --command .venv/bin/python scripts/check.py INPUT_DIR [OUTPUT_DIR]
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import logging
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from tqdm import tqdm

from spinta.lib import ELEMENT_TYPES, SPINTA_KEY, ManifestError, Spinta

REQUIRED_COLUMNS = ("dataset", "model", "property", "type", "level", "access")
DIFF_CODE = "(diff)"
NO_OUTPUT_CODE = "(no output)"
DEFAULT_OUTPUT = ".tmp/simple"


@dataclass
class FileResult:
    """What the checks found in one CSV file."""

    rel: Path
    elements: Counter = field(default_factory=Counter)  # type -> number of elements (`manifest`: documents)
    errors_by_type: Counter = field(default_factory=Counter)  # type -> number of errors
    errors: Counter = field(default_factory=Counter)  # error code -> number of errors
    differs: bool = False


class TqdmHandler(logging.Handler):
    """Log through `tqdm.write`, so log lines do not break the progress bar."""

    def emit(self, record: logging.LogRecord) -> None:
        tqdm.write(self.format(record), file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input_dir", type=Path, help="directory to search for CSV manifests")
    parser.add_argument("output_dir", type=Path, nargs="?", default=Path(DEFAULT_OUTPUT))
    args = parser.parse_args()

    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()
    if not input_dir.is_dir():
        parser.error(f"not a directory: {input_dir}")

    logging.basicConfig(level=logging.WARNING, handlers=[TqdmHandler()])
    spinta = Spinta()
    results: list[FileResult] = []
    sources = find_manifests(input_dir, output_dir)
    print(f"Found {len(sources)} manifest(s) in {input_dir}", file=sys.stderr)

    with tqdm(sources, unit="file", desc="Checking", file=sys.stderr) as bar:
        for src in bar:
            rel = src.relative_to(input_dir)
            bar.set_postfix_str(rel.as_posix(), refresh=False)
            result = FileResult(rel=rel)
            try:
                check_file(spinta, src, result, output_dir)
            except Exception as e:  # one broken file must not stop the others
                result.errors[type(e).__name__] += 1
                tqdm.write(f"failed {rel}: {type(e).__name__}: {e}", file=sys.stderr)
            results.append(result)

    print(render_summary(results, output_dir))
    return 0


def find_manifests(input_dir: Path, output_dir: Path) -> list[Path]:
    """The CSV files with the required header, sorted. Hidden paths and the output directory are skipped."""
    found = []
    for path in sorted(input_dir.rglob("*.csv")):
        rel = path.relative_to(input_dir)
        if any(part.startswith(".") for part in rel.parts):
            continue
        if path.resolve().is_relative_to(output_dir):
            continue
        if path.is_file() and has_required_header(path):
            found.append(path)
    return found


def has_required_header(path: Path) -> bool:
    try:
        with path.open(encoding="utf-8-sig", newline="") as f:
            header = next(csv.reader(f), [])
    except (OSError, UnicodeDecodeError, csv.Error):
        return False
    columns = {name.strip().lower() for name in header}
    return all(column in columns for column in REQUIRED_COLUMNS)


def check_file(spinta: Spinta, src: Path, result: FileResult, output_dir: Path) -> None:
    rel = result.rel
    simple_path = output_dir / rel.with_name(rel.name[: -len(".csv")] + ".simple.json")
    roundtrip_path = output_dir / rel
    diff_path = output_dir / rel.with_name(rel.name[: -len(".csv")] + ".diff")
    for stale in (roundtrip_path, diff_path):
        stale.unlink(missing_ok=True)

    # 1. CSV -> simple. Read errors are stored in the document, so the copy does not stop.
    doc = spinta.copy(str(src))
    simple_path.parent.mkdir(parents=True, exist_ok=True)
    simple_path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # 2. Count the saved simple document.
    count_document(spinta, doc, result)

    # 3. simple -> CSV, read back from the saved file.
    back = spinta.copy(str(simple_path))
    reports: list[tuple[ManifestError, bool]] = []
    out = spinta.copy(back, "csv", on_error=lambda error, raised: reports.append((error, raised)))
    if out is None:
        stopped = [error["code"] for error, raised in reports if raised]
        result.errors[stopped[0] if stopped else NO_OUTPUT_CODE] += 1
        return

    roundtrip_path.parent.mkdir(parents=True, exist_ok=True)
    roundtrip_path.write_bytes(out)

    # 4. Compare with the original; a diff is written only when the files differ.
    original = src.read_bytes()
    if original != out:
        result.differs = True
        diff = difflib.unified_diff(
            original.decode("utf-8", errors="replace").splitlines(keepends=True),
            out.decode("utf-8", errors="replace").splitlines(keepends=True),
            fromfile=str(rel),
            tofile=str(rel),
        )
        diff_path.write_text("".join(diff), encoding="utf-8")


def count_document(spinta: Spinta, doc: dict, result: FileResult) -> None:
    """Count elements and errors per type, and stored errors per code."""
    for type_ in element_types(doc):
        for element in doc[type_].values():
            result.elements[type_] += 1
            result.errors_by_type[type_] += len(element["errors"])
    # `errors` lists the errors of every element, the `manifest` elements included.
    for error in spinta.errors(doc):
        result.errors[error["code"]] += 1


def element_types(doc: dict) -> list[str]:
    """The element types of `doc`, in the order of `ELEMENT_TYPES`. Unknown types come last."""
    types = [key for key, value in doc.items() if key != SPINTA_KEY and isinstance(value, dict)]
    order = {name: i for i, name in enumerate(ELEMENT_TYPES)}
    return sorted(types, key=lambda name: (order.get(name, len(order)), name))


def render_summary(results: list[FileResult], output_dir: Path) -> str:
    lines = ["## Stats", "", "| type | elements | errors |", "| --- | --- | --- |"]

    elements: Counter = Counter()
    errors_by_type: Counter = Counter()
    for result in results:
        elements.update(result.elements)
        errors_by_type.update(result.errors_by_type)
    for type_ in sorted(elements, key=lambda name: (ELEMENT_TYPES.index(name) if name in ELEMENT_TYPES else 99, name)):
        lines.append(f"| {type_} | {elements[type_]} | {errors_by_type[type_]} |")

    # code -> file -> number of errors
    by_code: dict[str, dict[str, int]] = defaultdict(dict)
    for result in results:
        name = str(output_dir / simple_name(result.rel))
        for code, count in result.errors.items():
            by_code[code][name] = count
        if result.differs:
            by_code[DIFF_CODE][name] = 1

    lines += ["", "## Errors", "", "| code | errors | files |", "| --- | --- | --- |"]
    codes = sorted(code for code in by_code if code != DIFF_CODE)
    if DIFF_CODE in by_code:
        codes.append(DIFF_CODE)
    for code in codes:
        for i, (name, count) in enumerate(sorted(by_code[code].items())):
            lines.append(f"| {code if i == 0 else ''} | {count} | {name} |")
    return "\n".join(lines)


def simple_name(rel: Path) -> str:
    return rel.as_posix()[: -len(".csv")] + ".simple.json"


if __name__ == "__main__":
    sys.exit(main())
