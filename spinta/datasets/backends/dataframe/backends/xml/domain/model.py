"""Domain entities and value objects for ftree."""

from __future__ import annotations

from dataclasses import dataclass
import enum

from typing import Any, Optional, Sequence, Tuple

from spinta.components import Component

class DataModel(Component):
    """Data model component"""
    schema = {
        "data": {"type": "dict", "required": True}
    }
    data: dict[str, Any]

    def __dict__(self):
        return self.data

@dataclass(frozen=True)
class ManifestRow:
    """Represents a row in a manifest after adapter normalization."""

    path: Tuple[str, ...]
    property: str
    type: str
    ref: str
    source: Optional[str] = None
    value: Optional[str] = None
    access: Optional[enum.IntEnum] = None


@dataclass
class Manifest:
    """Domain representation of a manifest."""

    rows: Sequence[ManifestRow]


@dataclass
class Model(Component):
    """Domain representation of model data."""
    manifest: Manifest
    data: dict[str, object]

    def __init__(self, manifest: Manifest, data: dict[str, object]) -> None:
        self.manifest = manifest
        self.data = data

    def __call__(self, data, manifest) -> Model:
        return Model(data=data, manifest=manifest)

__all__ = [
    "ManifestRow",
    "Manifest",
    "Model",
]
