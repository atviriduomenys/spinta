from spinta.manifests.components import Manifest


class XSDManifest(Manifest):
    type = 'xsd'

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.xsd')
