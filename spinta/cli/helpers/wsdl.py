from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from spinta.cli.helpers.message import cli_warning
from spinta.components import Context


WSDL_SCHEMA_DIAGNOSTICS_KEY = "wsdl.schema_diagnostics"


def reset_wsdl_schema_diagnostics(context: Context) -> None:
    if context.has(WSDL_SCHEMA_DIAGNOSTICS_KEY, value=True):
        context.get(WSDL_SCHEMA_DIAGNOSTICS_KEY).clear()
        return
    context.set(WSDL_SCHEMA_DIAGNOSTICS_KEY, [])


def emit_wsdl_schema_diagnostics(context: Context) -> None:
    if not context.has(WSDL_SCHEMA_DIAGNOSTICS_KEY, value=True):
        return

    seen: set[tuple[str, str]] = set()
    for wsdl_path, message in context.get(WSDL_SCHEMA_DIAGNOSTICS_KEY):
        item = (wsdl_path, message)
        if item in seen:
            continue
        seen.add(item)
        cli_warning(f"Skipped referenced schema while reading WSDL {wsdl_path!r}. {message}")


@contextmanager
def wsdl_schema_diagnostics(context: Context) -> Iterator[None]:
    reset_wsdl_schema_diagnostics(context)
    try:
        yield
    except Exception:
        raise
    else:
        emit_wsdl_schema_diagnostics(context)
