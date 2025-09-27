from spinta.cli.helpers.sync.controllers.dataset import get_resource, create_resource
from spinta.cli.helpers.sync.helpers import extract_identifier_from_response


def get_data_service_id(base_path: str, headers: dict[str, str], agent_name: str) -> str:
    response_get_data_service = get_resource(base_path, headers, agent_name, validate_response=False)
    if response_get_data_service.ok:
        return extract_identifier_from_response(response_get_data_service, "list")

    response_create_data_service = create_resource(base_path, headers, agent_name, resource_type="service")
    return extract_identifier_from_response(response_create_data_service, "detail")
