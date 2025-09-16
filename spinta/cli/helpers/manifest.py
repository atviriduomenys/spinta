from typing import List, Union

from spinta.core.config import RawConfig, Path, parse_manifest_path, check_if_manifest_valid
from spinta.exceptions import UnknownManifestType
from spinta.manifests.components import ManifestPath


def convert_str_to_manifest_path(manifests: Union[List[str], str]) -> Union[ManifestPath, List[ManifestPath]]:
    if not manifests:
        return []

    new_manifest = []
    rc = RawConfig()
    rc.read([Path('spinta', 'spinta.config:CONFIG')])
    if isinstance(manifests, list):
        for manifest in manifests:
            new_manifest.append(_parse_cli_manifest_path(rc, manifest))
        return new_manifest
    return _parse_cli_manifest_path(rc, manifests)


def _parse_cli_manifest_path(
    rc: RawConfig,
    path: Union[str, ManifestPath],
) -> ManifestPath:
    from spinta.manifests.components import ManifestPath
    if isinstance(path, ManifestPath):
        return path
    if ":" in path:
        split = path.split(":")
        if len(split) >= 2 and not split[1].startswith("/") and not split[1].startswith("\\"):
            if check_if_manifest_valid(rc, split[0]):
                return ManifestPath(type=split[0], path=":".join(split[1:]))
            else:
                raise UnknownManifestType(type=split[0])
    return parse_manifest_path(rc, path)
