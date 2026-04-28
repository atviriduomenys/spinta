from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Iterator

from spinta.components import Context


def get_diagnostics_storage(context: Context, key: str) -> list:
    if not context.has(key, value=True):
        context.set(key, [])
    return context.get(key)


def reset_diagnostics(context: Context, key: str) -> None:
    get_diagnostics_storage(context, key).clear()


@contextmanager
def collect_diagnostics(
    context: Context,
    key: str,
    emit_diagnostics: Callable[[Context, str], None],
) -> Iterator[None]:
    reset_diagnostics(context, key)
    try:
        yield
    except Exception:
        raise
    else:
        emit_diagnostics(context, key)
