from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

from spinta.manifests.components import Manifest


class YamlManifest(Manifest):
    path: Path = None


InlineManifestData = List[Dict[str, Any]]


class InlineManifest(YamlManifest):
    manifest: InlineManifestData
