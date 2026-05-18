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
INSERT_FUNCTION = "insert"


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


def _parse_function_fields(prepare: str, func_name: str) -> dict[str, str]:
    """Parse 'func_name(key1:val1, key2:val2, ...)' into {'key1': 'val1', 'key2': 'val2', ...}.

    Splits on commas only when followed by a word-colon pattern, so values containing
    commas inside brackets (e.g. ref:example2/City[id, name]) are handled correctly.
    """
    match = re.match(rf"{func_name}\((.+)\)\s*$", (prepare or "").strip())
    if not match:
        return {}
    parts = re.split(r",\s*(?=\w+:)", match.group(1))
    result = {}
    for part in parts:
        key, _, value = part.partition(":")
        result[key.strip()] = value.strip().strip('"')
    return result


def _parse_update_fields(prepare: str) -> dict[str, str]:
    return _parse_function_fields(prepare, UPDATE_FUNCTION)


def _parse_insert_fields(prepare: str) -> dict[str, str]:
    return _parse_function_fields(prepare, INSERT_FUNCTION)


def _is_restore_comment(row: ManifestRow, func_name: str) -> bool:
    prepare = row.get("prepare") or ""
    return row.get("type") == COLUMN_TYPE_COMMENT and prepare.startswith(func_name)


def _insert_base_resets(result: list[ManifestRow]) -> list[ManifestRow]:
    """Insert `/` reset rows where base context would leak from a previous
    model into a model that has no base row directly above it."""
    fixed: list[ManifestRow] = []
    active_base: str | None = None
    base_seen_since_last_model: bool = False

    for row in result:
        is_base_row = row.get("base") and not row.get("model") and not row.get("property")
        is_model_row = bool(row.get("model"))

        if is_base_row:
            active_base = row["base"]
            base_seen_since_last_model = True
            fixed.append(row)
        elif is_model_row:
            if not base_seen_since_last_model and active_base and active_base != "/":
                # No base row immediately above this model, but a base context
                # is still active from a previous model. Insert `/` to break it.
                reset_row = {col: "" for col in row.keys()}
                reset_row["base"] = "/"
                fixed.append(reset_row)
                active_base = "/"
            base_seen_since_last_model = False
            fixed.append(row)
        else:
            fixed.append(row)

    return fixed


def _uncomment_rows(rows: list[ManifestRow], uri_filter: str | None) -> list[ManifestRow]:
    result: list[ManifestRow] = []
    last_property_index: int | None = None
    last_model_index: int | None = None
    base_was_inserted: bool = False

    for row in rows:
        if _is_restore_comment(row, UPDATE_FUNCTION):
            if uri_filter is not None and row.get("uri") != uri_filter:
                result.append(row)
                continue
            fields = _parse_update_fields(row.get("prepare", ""))
            if fields and last_property_index is not None:
                prop_row: dict = dict(result[last_property_index])
                for key, value in fields.items():
                    prop_row[key] = value
                prop_row["level"] = row.get("level") or ""
                result[last_property_index] = prop_row

        elif _is_restore_comment(row, INSERT_FUNCTION):
            if uri_filter is not None and row.get("uri") != uri_filter:
                result.append(row)
                continue
            fields = _parse_insert_fields(row.get("prepare", ""))
            if fields and last_model_index is not None:
                base_row: dict = {col: "" for col in row.keys()}
                for key, value in fields.items():
                    base_row[key] = value
                base_row["level"] = row.get("level") or ""
                result.insert(last_model_index, base_row)
                last_model_index += 1
                base_was_inserted = True

        else:
            # If it's not an `update` comment, append to the result as-is;
            if row.get("model"):
                last_model_index = len(result)
            if row.get("property"):
                last_property_index = len(result)
            result.append(row)

    if base_was_inserted:
        result = _insert_base_resets(result)

    return result
