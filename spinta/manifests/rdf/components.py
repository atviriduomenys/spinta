from spinta.manifests.components import Manifest


class RdfManifest(Manifest):
    type: str = 'rdf'
    format: str

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.rdf') or path.endswith('.ttl')
