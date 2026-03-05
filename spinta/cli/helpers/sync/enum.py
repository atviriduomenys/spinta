from enum import Enum


class ResourceType(str, Enum):
    CATALOG = "catalog"
    DATASET = "dataset"
    DATASET_SERIES = "series"
    DATA_SERVICE = "service"
    INFORMATION_SYSTEM = "information_system"


class ResponseType(str, Enum):
    LIST = "list"
    DETAIL = "detail"
