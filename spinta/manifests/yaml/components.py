from pathlib import Path

from spinta.manifests.components import Manifest


class YamlManifest(Manifest):
    path: Path = None
