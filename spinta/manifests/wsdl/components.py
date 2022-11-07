from typing import Optional

from spinta.manifests.components import Manifest


class WsdlManifest(Manifest):
    type = 'wsdl'
    path: Optional[str] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('wsdl')
