import csv
import pathlib
import re

from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta.components import Context
from spinta.manifests.tabular.components import MANIFEST_COLUMNS
from spinta.manifests.tabular.components import ManifestRow
from spinta.manifests.tabular.helpers import render_tabular_manifest_rows
from spinta.manifests.tabular.helpers import torow
from spinta.manifests.tabular.helpers import write_tabular_manifest


UPDATE_FUNCTION = "update"
COLUMN_TYPE_COMMENT = "comment"


def uncomment(
    ctx: TyperContext,
    uri: str | None = Option(None, "--uri", help="Only restore comments that carry this URI tag"),
    output: str | None = Option(None, "-o", "--output", help="Output tabular manifest in a specified file"),
    manifests: list[str] = Argument(None, help="Source manifest files"),
):
    """Restore commented-out properties."""
    context: Context = ctx.obj
    uncomment_manifest(context, uri=uri, output=output, manifests=manifests)


def uncomment_manifest(
    context: Context,
    uri: str | None = None,
    output: str | None = None,
    manifests: list[str] | None = None,
) -> None:
    """Read raw CSV rows, remove `update` comments, and write the result."""
    rows = _read_raw_manifest_rows(manifests or [])
    result = _uncomment_rows(rows, uri_filter=uri)
    if output:
        write_tabular_manifest(context, output, iter(result))
    else:
        echo(render_tabular_manifest_rows(iter(result)))


def _read_raw_manifest_rows(manifests: list[str]) -> list[ManifestRow]:
    """Read CSV manifest files as raw dicts, normalising to MANIFEST_COLUMNS keys."""
    rows = []
    for path in manifests:
        with pathlib.Path(path).open(encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            for raw in reader:
                rows.append(torow(MANIFEST_COLUMNS, raw))
    return rows


def _parse_update_fields(prepare: str) -> dict[str, str]:
    """Parse 'update(key1:val1, key2:val2, ...)' into {'key1': 'val1', 'key2': 'val2', ...}.

    Splits on commas only when followed by a word-colon pattern, so values containing
    commas inside brackets (e.g. ref:example2/City[id, name]) are handled correctly.
    """
    match = re.match(rf"{UPDATE_FUNCTION}\((.+)\)\s*$", (prepare or "").strip())
    if not match:
        return {}
    parts = re.split(r",\s*(?=\w+:)", match.group(1))
    result = {}
    for part in parts:
        key, _, value = part.partition(":")
        result[key.strip()] = value.strip().strip('"')
    return result


def _is_restore_comment(row: ManifestRow) -> bool:
    prepare = row.get("prepare") or ""
    return row.get("type") == COLUMN_TYPE_COMMENT and prepare.startswith(UPDATE_FUNCTION)


def _uncomment_rows(rows: list[ManifestRow], uri_filter: str | None) -> list[ManifestRow]:
    """
    Walk rows in order. When an `update` comment is found (and passes the URI filter),
    patch the most recent property row in result and drop the comment row.
    """
    result: list[ManifestRow] = []
    last_property_index: int | None = None

    for row in rows:
        if _is_restore_comment(row):
            # Restore only rows that match the `uri` if it is given by input;
            if uri_filter is not None and row.get("uri") != uri_filter:
                result.append(row)
                continue

            # Parse the expression and apply field to the last seen row (comments go after the row they change);
            fields = _parse_update_fields(row.get("prepare", ""))
            if fields and last_property_index is not None:
                prop_row: dict = dict(result[last_property_index])
                for key, value in fields.items():
                    prop_row[key] = value
                prop_row["level"] = row.get("level") or ""
                result[last_property_index] = prop_row
        else:
            # If it's not an `update` comment, append to the result as-is;
            if row.get("property"):
                last_property_index = len(result)
            result.append(row)

    return result
