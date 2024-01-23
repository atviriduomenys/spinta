from enum import Enum

import requests

from spinta.manifests.components import Manifest


class DictFormat(Enum):
    JSON = 'json'
    XML = 'xml'
    HTML = 'html'


class DictManifest(Manifest):
    format: DictFormat = None


class JsonManifest(DictManifest):
    type = 'json'
    format: DictFormat = DictFormat.JSON

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.json')


class XmlManifest(DictManifest):
    type = 'xml'
    format: DictFormat = DictFormat.XML

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith('.xml')
