from __future__ import annotations

from abc import ABC, abstractmethod
from typer import echo

from spinta.exceptions import BaseError


class ErrorHandler(ABC):
    @abstractmethod
    def handle_error(self, error: BaseError, file: str | None = None) -> None: ...

    @abstractmethod
    def post_process(self) -> None: ...


class CLIErrorHandler(ErrorHandler):
    def __init__(self) -> None:
        self._error_counts: dict[str, int] = {}

    def handle_error(self, error: BaseError, file: str | None = None) -> None:
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


class ErrorManager:
    def __init__(self, handler: CLIErrorHandler, re_raise: bool = False) -> None:
        self.handler: CLIErrorHandler = handler
        self.re_raise = re_raise
        self.current_file: str | None = None

    def handle_error(self, error: Exception, file: str | None = None) -> None:
        target_file = file or self.current_file
        if self.re_raise:
            raise error

        self.handler.handle_error(error, file=target_file)
