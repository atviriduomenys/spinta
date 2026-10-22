from pathlib import Path
from typing import Any

from spinta.manifests.components import Manifest


class YamlManifest(Manifest):
    type = "yaml"
    path: Path = None


InlineManifestData = list[dict[str, Any]]


class InlineManifest(YamlManifest):
    type = "inline"
    manifest: InlineManifestData
