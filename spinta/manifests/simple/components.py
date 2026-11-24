"""The `simple` manifest type (section 4).

A `simple` manifest is a JSON document of nested maps (D4). It is read from a
file (`path`), or from `file` as JSON text, the way CSV reads an in-memory file.
"""

from __future__ import annotations

from typing import IO

from spinta.manifests.components import Manifest


class SimpleFileManifest(Manifest):
    type = "simple"
    # A `simple` document read from `path` or from `file` (JSON text), as the
    # CSV manifest reads `file`. Values only: a JSON tree cannot be a config
    # value (the config treats a dict as a node).
    file: IO | None = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".simple.json")
