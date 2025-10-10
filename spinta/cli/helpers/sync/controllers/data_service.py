from spinta.cli.helpers.sync.controllers.dataset import get_resources
from spinta.cli.helpers.sync.api_helpers import extract_identifier_from_response


def get_data_service_id(base_path: str, headers: dict[str, str], agent_name: str) -> str:
    response_get_data_service = get_resources(base_path, headers, {"name": agent_name})
    return extract_identifier_from_response(response_get_data_service, "list")


def get_data_service_children_dataset_ids(base_path: str, headers: dict[str, str], agent_name: str) -> list[str]:
    response_get_data_service = get_resources(base_path, headers, {"name": agent_name})
    data_service_id = extract_identifier_from_response(response_get_data_service, "list")

    response_get_datasets_for_data_service = get_resources(base_path, headers, {"parent_id": data_service_id})
    dataset_ids = [dataset["_id"] for dataset in response_get_datasets_for_data_service.json().get("_data", [{}])]

    return dataset_ids
