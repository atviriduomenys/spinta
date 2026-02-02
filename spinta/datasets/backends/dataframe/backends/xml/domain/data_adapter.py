"""Domain data adapter contract for parsing source data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Mapping, Protocol

from sqlalchemy import String

from .model import Manifest


class DataAdapterError(ValueError):
    """Raised when adapter input cannot be converted into data rows."""


class DataAdapter(Protocol):
    """Protocol for client-provided data parsing adapters."""

    def load(self, manifest: Manifest, **kwargs: object) -> Iterable[Mapping[str, object]]:
        """Return row mappings aligned to manifest properties."""


@dataclass
class BaseDataAdapter:
    """Base adapter that defers parsing to client implementations."""

    def load(self, manifest: Manifest, source: object, selector: List[String]) -> Iterable[Mapping[str, object]]:
        raise DataAdapterError("Parsing deferred: data parsing must be implemented by client adapter")


__all__ = ["DataAdapter", "BaseDataAdapter", "DataAdapterError"]
