from pathlib import Path
from typing import Any, Dict, List

from spinta.manifests.components import Manifest


class YamlManifest(Manifest):
    type = "yaml"
    path: Path = None


InlineManifestData = List[Dict[str, Any]]


class InlineManifest(YamlManifest):
    type = "inline"
    manifest: InlineManifestData
