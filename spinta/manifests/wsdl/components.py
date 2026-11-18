from spinta.manifests.components import Manifest
from spinta.manifests.wsdl.helpers import is_wsdl_path


class WsdlManifest(Manifest):
    type = "wsdl"

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return is_wsdl_path(path)
