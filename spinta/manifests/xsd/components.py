from spinta.manifests.components import Manifest


class XsdManifest(Manifest):
    type = 'xsd'

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.xsd')
