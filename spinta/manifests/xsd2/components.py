from spinta.manifests.components import Manifest


class XsdManifest2(Manifest):
    type = "xsd2"

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".xsd") and path.startswith("xsd2+file://")
