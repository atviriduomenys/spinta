"""Domain services."""

from __future__ import annotations


class ManifestValidationError(ValueError):
    """Raised when a manifest violates domain validation rules."""


class SelectorError(ValueError):
    """Raised when a selector refers to unknown property paths."""


__all__ = [
    "ManifestValidationError",
    "SelectorError",
]
