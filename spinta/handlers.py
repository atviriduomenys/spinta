from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from typer import echo

from spinta.exceptions import BaseError, MultipleErrors


class ErrorHandler(ABC):
    @abstractmethod
    def handle_error(self, error: BaseError, file: str | None = None, *, raised: bool = False) -> None:
        """`raised` is set for an error that stopped a guarded block (see `ErrorManager.guard`)."""
        ...

    @abstractmethod
    def post_process(self) -> None: ...


class CLIErrorHandler(ErrorHandler):
    def __init__(self) -> None:
        self._error_counts: dict[str, int] = {}

    def handle_error(self, error: BaseError, file: str | None = None, *, raised: bool = False) -> None:
        key = file
        self._error_counts[key] = self._error_counts.get(key, 0) + 1
        echo(error)

    def get_counts(self) -> dict[str, int]:
        return self._error_counts

    def post_process(self) -> None:
        counts = self.get_counts()
        if not counts:
            return

        total_errors = sum(counts.values())
        echo(f"Total errors: {total_errors}")
        for file, cnt in counts.items():
            echo(f"- {file}: {cnt} error(s)")


class ErrorCollector(ErrorHandler):
    """Stores errors instead of printing them, used by `spinta.lib`.

    Each stored error is a `BaseError`, its `context` names the element it is
    about (see `spinta.manifests.simple.errors`).
    """

    def __init__(self) -> None:
        self.errors: list[BaseError] = []
        # The errors that stopped a guarded block. At HEAD these ended the
        # command with an exception, so they decide the exit status (3.5).
        self.raised: list[BaseError] = []

    def handle_error(self, error: BaseError, file: str | None = None, *, raised: bool = False) -> None:
        self.errors.append(error)
        if raised:
            self.raised.append(error)

    def post_process(self) -> None:
        pass

    def clear(self) -> None:
        self.errors = []
        self.raised = []

    def __iter__(self) -> Iterator[BaseError]:
        return iter(self.errors)


class ErrorManager:
    """Passes errors to a handler.

    With `re_raise` the error is raised immediately. With `collect` a guarded
    block stores its `BaseError`s in the handler and goes on, see `guard`.
    """

    def __init__(
        self,
        handler: ErrorHandler,
        re_raise: bool = False,
        collect: bool = False,
    ) -> None:
        self.handler: ErrorHandler = handler
        self.re_raise = re_raise
        self.collect = collect
        self.current_file: str | None = None

    def handle_error(self, error: Exception, file: str | None = None) -> None:
        target_file = file or self.current_file
        if self.re_raise:
            raise error

        self.handler.handle_error(error, file=target_file)

    @contextmanager
    def guard(self, node: Any = None) -> Iterator[None]:
        """Collect errors raised in the block, if `collect` is set.

        Without `collect` nothing is caught, so the first error is raised as
        before. With `collect`, each `BaseError` (and each error of
        `MultipleErrors`) is passed to the handler, and the block ends there.
        """
        if not self.collect:
            yield
            return
        try:
            yield
        except MultipleErrors as e:
            for error in e.errors:
                self.handler.handle_error(error, file=self.current_file, raised=True)
        except BaseError as e:
            self.handler.handle_error(e, file=self.current_file, raised=True)
