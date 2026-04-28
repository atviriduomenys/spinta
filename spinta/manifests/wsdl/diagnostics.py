from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from spinta.cli.helpers.diagnostics import collect_diagnostics
from spinta.cli.helpers.message import cli_warning
from spinta.components import Context


WSDL_SCHEMA_DIAGNOSTICS_KEY = "wsdl.schema_diagnostics"


def emit_wsdl_schema_diagnostics(context: Context, key: str = WSDL_SCHEMA_DIAGNOSTICS_KEY) -> None:
    if not context.has(key, value=True):
        return

    seen: set[tuple[str, str]] = set()
    for wsdl_path, message in context.get(key):
        item = (wsdl_path, message)
        if item in seen:
            continue
        seen.add(item)
        cli_warning(f"Skipped referenced schema while reading WSDL {wsdl_path!r}. {message}")


@contextmanager
def wsdl_schema_diagnostics(context: Context) -> Iterator[None]:
    with collect_diagnostics(context, WSDL_SCHEMA_DIAGNOSTICS_KEY, emit_wsdl_schema_diagnostics):
        yield