from spinta.manifests.components import Manifest
from spinta.manifests.wsdl.helpers import is_wsdl_path


class WsdlManifest(Manifest):
    type = "wsdl"

    def __init__(self):
        super().__init__()
        self.schema_diagnostics = []

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return is_wsdl_path(path)
