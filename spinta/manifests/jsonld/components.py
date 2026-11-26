from spinta.manifests.components import Manifest


class JsonLdManifest(Manifest):
    """The JSON-LD context documents of a manifest, one per model (`.tmp/specs/json-ld.md`, section 9)."""

    type: str = "jsonld"

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".jsonld")
