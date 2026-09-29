import dataclasses
from enum import Enum

from spinta.manifests.components import Manifest
from spinta.manifests.helpers import TypeDetector


class DictFormat(Enum):
    JSON = "json"
    XML = "xml"
    HTML = "html"


class DictManifest(Manifest):
    format: DictFormat = None


class JsonManifest(DictManifest):
    type = "json"
    format: DictFormat = DictFormat.JSON

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".json") and not path.startswith("openapi+file://")


class XmlManifest(DictManifest):
    type = "xml"
    format: DictFormat = DictFormat.XML

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return path.endswith(".xml")


@dataclasses.dataclass
class MappedProperties:
    name: str
    source: str
    extra: str
    type_detector: TypeDetector


@dataclasses.dataclass
class MappedModels:
    name: str
    source: str
    properties: dict[str, MappedProperties]


@dataclasses.dataclass
class MappedDataset:
    dataset: str
    given_dataset_name: str
    resource: str
    resource_type: str
    resource_path: str
    models: dict[str, dict[str, MappedModels]]


@dataclasses.dataclass
class MappingMeta:
    is_blank_node: bool
    blank_node_name: str
    blank_node_source: str
    seperator: str
    recursive_descent: str
    remove_array_suffix: bool
    model_source_prefix: str
    check_namespace: bool
    namespace_prefixes: dict[str, list[str]]
    namespace_seperator: str
    manifest_type: DictFormat

    @classmethod
    def get_for(cls, manifest_type: DictFormat) -> "MappingMeta":
        if manifest_type == DictFormat.JSON:
            mapping_meta = MappingMeta.for_json()
        elif manifest_type in (DictFormat.XML, DictFormat.HTML):
            mapping_meta = MappingMeta.for_xml(manifest_type)
        else:
            mapping_meta = MappingMeta.default()

        return mapping_meta

    @classmethod
    def for_json(cls) -> "MappingMeta":
        return cls(
            is_blank_node=False,
            blank_node_name="model1",
            blank_node_source=".",
            seperator=".",
            recursive_descent=".",
            model_source_prefix="",
            namespace_seperator=":",
            remove_array_suffix=False,
            check_namespace=False,
            namespace_prefixes={},
            manifest_type=DictFormat.JSON,
        )

    @classmethod
    def for_xml(cls, manifest_type: DictFormat) -> "MappingMeta":
        return cls(
            is_blank_node=False,
            blank_node_name="model1",
            blank_node_source=".",
            seperator="/",
            recursive_descent="/..",
            model_source_prefix="/",
            namespace_seperator=":",
            remove_array_suffix=True,
            check_namespace=True,
            namespace_prefixes={"xmlns": ["xmlns", "@xmlns"]},
            manifest_type=manifest_type,
        )

    @classmethod
    def default(cls) -> "MappingMeta":
        return cls(
            is_blank_node=False,
            blank_node_name="model1",
            blank_node_source=".",
            seperator="",
            recursive_descent="",
            model_source_prefix="",
            namespace_seperator=":",
            remove_array_suffix=False,
            check_namespace=False,
            namespace_prefixes={},
        )


@dataclasses.dataclass
class MappingScope:
    parent_scope: str
    model_scope: str
    model_name: str
    property_name: str
