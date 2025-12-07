from spinta.manifests.components import Manifest


class OpenAPIManifest(Manifest):
    type = "openapi"

    @staticmethod
    def detect_from_path(path: str) -> bool:
        # TODO: temporary detection for now. Later, we should try to distinguish OpenAPI & JSON Schema files.
        return path.endswith(".json") and path.startswith("openapi+file://")
