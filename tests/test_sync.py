import json
from http import HTTPStatus
from pathlib import Path, PosixPath
from typing import Iterator
from unittest.mock import patch, MagicMock

import pytest

from spinta.cli.helpers.sync.api_helpers import STATIC_BASE_PATH_TAIL
from spinta.cli.helpers.sync.controllers.synchronization.catalog_to_agent import (
    execute_synchronization_catalog_to_agent,
)
from spinta.client import RemoteClientCredentials
from spinta.core.config import RawConfig
from spinta.exceptions import InvalidCredentialsConfigurationException, AgentRelatedDataServiceDoesNotExist
from spinta.manifests.tabular.helpers import render_tabular_manifest, striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.tabular import convert_ascii_manifest_to_csv
from tests.conftest import get_request_context
from tests.test_api import ensure_temp_context_and_app

DATABASE_URL = "default"  # TODO: Replace when data source logic is introduced.


@pytest.fixture
def credentials() -> Iterator[RemoteClientCredentials]:
    credentials = RemoteClientCredentials(
        section="default",
        remote="origin",
        client="client_id",
        secret="secret",
        server="https://example.com",
        resource_server="https://example2.com",
        scopes="scope1 scope2",
        organization="vssa",
        organization_type="gov",
    )
    with patch("spinta.cli.sync.get_configuration_credentials", return_value=credentials):
        yield credentials


@pytest.fixture
def base_api_path(credentials: RemoteClientCredentials) -> str:
    return f"{credentials.resource_server}{STATIC_BASE_PATH_TAIL}"


@pytest.fixture
def configuration(local_manifest_path: PosixPath) -> Iterator[tuple[str, str]]:
    configuration_parameters = (DATABASE_URL, str(local_manifest_path))
    with patch("spinta.cli.sync.load_configuration_values", return_value=configuration_parameters):
        yield configuration_parameters


@pytest.fixture
def local_manifest_path(tmp_path: PosixPath) -> PosixPath:
    return tmp_path / "local_manifest.csv"


@pytest.fixture
def manifest() -> str:
    return """
    id                                   | dataset | resource   | model   | property      | type     | ref  | source                                                | level     | status    | visibility | access | title     | description
    f89e1015-c77c-4d81-958c-52f0120e44a1 | vssa    |            |         |               | dataset  | vssa | https://example.com                                   |           | open      |            |        | VSSA      | vssa
    c3caa75b-fbb6-4868-a366-e61e4f3225bf |         | geography  |         |               | dask/csv |      | https://get.data.gov.lt/datasets/org/vssa/example/:ns | 4         |           |            |        | Geography | geography
                                         |         |            |         |               |          |      |                                                       |           |           |            |        |           |
    7d5488e7-ce3c-4c64-90d8-f554a7721f20 |         |            | Country |               |          | id   | model_country                                         | 4         | completed | package    | open   | Country   | country
    de4107e4-7f9a-425e-ba7a-3626f59b360c |         |            |         | id            | integer  |      | id                                                    | 4         |           |            |        | Id        | id
    """


class TestSynchronization:
    def test_success_full_flow(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
        configuration: tuple[str, str],
    ):
        local_manifest_file_path = configuration[1]
        mock_auth_token_post = requests_mock.post(
            f"{credentials.server}/auth/token",
            status_code=HTTPStatus.OK,
            json={"access_token": "test-token"},
        )
        mock_data_service_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?name=client",
            status_code=HTTPStatus.OK,
            json={"_data": [{"_id": 1}]},
        )
        mock_data_service_dataset_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?parent_id=1",
            status_code=HTTPStatus.OK,
            json={"_data": [{"_id": 2}]},
        )
        mock_dataset_manifest_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/2/dsa",
            status_code=HTTPStatus.OK,
            text=(
                "id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,"
                "visibility,access,uri,eli,title,description"
            ),
            headers={"Content-Type": "text/csv"},
        )

        cli.invoke(rc, args=["sync"], catch_exceptions=True)

        assert Path(local_manifest_file_path).exists()

        assert get_request_context(mock_auth_token_post) == [
            {
                "method": "POST",
                "url": f"{credentials.server}/auth/token",
                "params": {},
                "data": {"grant_type": ["client_credentials"], "scope": ["scope1 scope2"]},
            }
        ]
        assert get_request_context(mock_data_service_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?name=client",
                "params": {"name": ["client"]},
                "data": {},
            }
        ]
        assert get_request_context(mock_data_service_dataset_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?parent_id=1",
                "params": {"parent_id": ["1"]},
                "data": {},
            }
        ]
        assert get_request_context(mock_dataset_manifest_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/2/dsa",
                "params": {},
                "data": {},
            }
        ]

    def test_failure_credentials_not_set(self, rc: RawConfig, cli: SpintaCliRunner):
        credentials = RemoteClientCredentials(
            section=None,
            remote=None,
            client=None,
            secret=None,
            server=None,
            resource_server=None,
            scopes=None,
            organization=None,
            organization_type=None,
        )
        with patch("spinta.cli.sync.get_configuration_credentials", return_value=credentials):
            with pytest.raises(InvalidCredentialsConfigurationException) as exception:
                cli.invoke(rc, args=["sync"], catch_exceptions=False)

        assert exception.value.status_code == HTTPStatus.BAD_REQUEST
        assert "Credentials.cfg is missing required configuration credentials." in exception.value.message

    def test_failure_auth_server_returned_unexpected_response(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
    ):
        mock_auth_token_post = requests_mock.post(
            f"{credentials.server}/auth/token",
            status_code=HTTPStatus.BAD_REQUEST,
            json={"error": "unexpected error"},
        )

        with pytest.raises(Exception) as exception:
            cli.invoke(rc, args=["sync"], catch_exceptions=False)

        assert exception.value.response.status_code == HTTPStatus.BAD_REQUEST
        assert exception.value.response.text == json.dumps({"error": "unexpected error"})

        assert get_request_context(mock_auth_token_post) == [
            {
                "method": "POST",
                "url": f"{credentials.server}/auth/token",
                "params": {},
                "data": {"grant_type": ["client_credentials"], "scope": ["scope1 scope2"]},
            }
        ]

    def test_failure_catalog_does_not_have_the_data_service_related_to_the_agent(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
    ):
        """Catalog must have a data service related to the Agent; it is created automatically during Agent creation."""
        mock_auth_token_post = requests_mock.post(
            f"{credentials.server}/auth/token",
            status_code=HTTPStatus.OK,
            json={"access_token": "test-token"},
        )
        mock_data_service_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?name=client",
            status_code=HTTPStatus.NOT_FOUND,
            json={"error": "data_service_not_found"},
        )

        with pytest.raises(AgentRelatedDataServiceDoesNotExist) as exception:
            cli.invoke(rc, args=["sync"], catch_exceptions=False)

        assert exception.value.status_code == HTTPStatus.BAD_REQUEST
        assert (
            "Data Service related to the Agent that is executing the synchronization request does not exist."
            in exception.value.message
        )

        assert get_request_context(mock_auth_token_post) == [
            {
                "method": "POST",
                "url": f"{credentials.server}/auth/token",
                "params": {},
                "data": {"grant_type": ["client_credentials"], "scope": ["scope1 scope2"]},
            }
        ]
        assert get_request_context(mock_data_service_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?name=client",
                "params": {"name": ["client"]},
                "data": {},
            }
        ]

    def test_success_agent_related_data_service_does_not_have_any_child_datasets(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
    ):
        mock_auth_token_post = requests_mock.post(
            f"{credentials.server}/auth/token",
            status_code=HTTPStatus.OK,
            json={"access_token": "test-token"},
        )
        mock_data_service_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?name=client",
            status_code=HTTPStatus.OK,
            json={"_data": [{"_id": 1}]},
        )
        mock_data_service_dataset_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?parent_id=1",
            status_code=HTTPStatus.OK,
            json={"_data": []},
        )

        cli.invoke(rc, args=["sync"], catch_exceptions=False)

        assert get_request_context(mock_auth_token_post) == [
            {
                "method": "POST",
                "url": f"{credentials.server}/auth/token",
                "params": {},
                "data": {"grant_type": ["client_credentials"], "scope": ["scope1 scope2"]},
            }
        ]
        assert get_request_context(mock_data_service_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?name=client",
                "params": {"name": ["client"]},
                "data": {},
            }
        ]
        assert get_request_context(mock_data_service_dataset_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/?parent_id=1",
                "params": {"parent_id": ["1"]},
                "data": {},
            }
        ]


class TestSynchronizationPathCatalogToAgent:
    def test_catalog_manifest_and_local_manifest_contents_are_identical(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        manifest: str,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
        configuration: tuple[str, str],
        base_api_path: str,
        tmp_path: PosixPath,
        local_manifest_path: PosixPath,
    ):
        """Agent & Catalog manifests are identical, no action required for Catalog -> Agent sync part."""
        # When;
        manifest_csv = convert_ascii_manifest_to_csv(manifest).decode("utf-8")
        local_manifest_path.write_text(manifest_csv)

        dataset_id = "2"
        mock_dataset_manifest_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
            status_code=HTTPStatus.OK,
            text=manifest_csv,
            headers={"Content-Type": "text/csv"},
        )

        context, _ = ensure_temp_context_and_app(rc, tmp_path)

        # Do;
        execute_synchronization_catalog_to_agent(
            context, base_api_path, {"Authorization": "Bearer <token>"}, str(local_manifest_path), [dataset_id]
        )

        # Check.
        context, final_manifest = load_manifest_and_context(rc, local_manifest_path)
        assert render_tabular_manifest(context, final_manifest) == striptable("""
            id | d | r | b | m | property | type     | ref | source                                                | source.type | prepare | origin | count | level | status    | visibility | access | uri | eli | title     | description
            f8 | vssa                     |          |     |                                                       |             |         |        |       |       |           |            |        |     |     | VSSA      | vssa
            c3 |   | geography            | dask/csv |     | https://get.data.gov.lt/datasets/org/vssa/example/:ns |             |         |        |       | 4     |           |            |        |     |     | Geography | geography
               |                          |          |     |                                                       |             |         |        |       |       |           |            |        |     |     |           |
            7d |   |   |   | Country      |          | id  | model_country                                         |             |         |        |       | 4     | completed | package    | open   |     |     | Country   | country
            de |   |   |   |   | id       | integer  |     | id                                                    |             |         |        |       | 4     |           |            |        |     |     | Id        | id
        """)

        assert get_request_context(mock_dataset_manifest_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
                "params": {},
                "data": {},
            }
        ]

    def test_local_manifest_empty_prefilling_content_from_catalog_manifest(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        manifest: str,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
        configuration: tuple[str, str],
        base_api_path: str,
        tmp_path: PosixPath,
        local_manifest_path: PosixPath,
    ):
        """If the Agent manifest is empty and Catalog manifest is filled, Agent manifest is filled with Catalog data."""
        # When;
        assert not local_manifest_path.is_file()
        local_manifest_path.touch()
        catalog_manifest_csv = convert_ascii_manifest_to_csv(manifest).decode("utf-8")
        dataset_id = "2"

        mock_dataset_manifest_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
            status_code=HTTPStatus.OK,
            text=catalog_manifest_csv,
            headers={"Content-Type": "text/csv"},
        )

        context, _ = ensure_temp_context_and_app(rc, tmp_path)

        # Do;
        execute_synchronization_catalog_to_agent(
            context, base_api_path, {"Authorization": "Bearer <token>"}, str(local_manifest_path), [dataset_id]
        )

        # Check.
        context, final_manifest = load_manifest_and_context(rc, local_manifest_path)
        assert render_tabular_manifest(context, final_manifest) == striptable("""
            id | d | r | b | m | property | type     | ref | source                                                | source.type | prepare | origin | count | level | status    | visibility | access | uri | eli | title     | description
            f8 | vssa                     |          |     |                                                       |             |         |        |       |       |           |            |        |     |     | VSSA      | vssa
            c3 |   | geography            | dask/csv |     | https://get.data.gov.lt/datasets/org/vssa/example/:ns |             |         |        |       | 4     |           |            |        |     |     | Geography | geography
               |                          |          |     |                                                       |             |         |        |       |       |           |            |        |     |     |           |
            7d |   |   |   | Country      |          | id  | model_country                                         |             |         |        |       | 4     | completed | package    | open   |     |     | Country   | country
            de |   |   |   |   | id       | integer  |     | id                                                    |             |         |        |       | 4     |           |            |        |     |     | Id        | id
        """)

        assert get_request_context(mock_dataset_manifest_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
                "params": {},
                "data": {},
            }
        ]

    def test_catalog_manifest_has_more_fields_than_agent_manifest(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        manifest: str,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
        configuration: tuple[str, str],
        base_api_path: str,
        tmp_path: PosixPath,
        local_manifest_path: PosixPath,
    ):
        """Fields that exist in Catalog manifest, but not in Agent manifest are added to the Agent manifest."""
        # When;
        catalog_manifest = """
        id                                   | dataset | resource   | model   | property      | type     | ref | source                                                | level     | status    | visibility | access | title     | description
        f89e1015-c77c-4d81-958c-52f0120e44a1 | vssa    |            |         |               |          |     | https://example.com                                   |           | open      |            |        | VSSA      | vssa
        c3caa75b-fbb6-4868-a366-e61e4f3225bf |         | geography  |         |               | dask/csv |     | https://get.data.gov.lt/datasets/org/vssa/example/:ns | 4         |           |            |        | Geography | geography
                                             |         |            |         |               |          |     |                                                       |           |           |            |        |           |
        7d5488e7-ce3c-4c64-90d8-f554a7721f20 |         |            | Country |               |          | id  | model_country                                         | 4         | completed | package    | open   | Country   | country
        de4107e4-7f9a-425e-ba7a-3626f59b360c |         |            |         | id            | integer  |     | id                                                    | 4         |           |            |        | Id        | id
        6c2fdd17-0408-44fc-b38d-613ca7f6e1c3 |         |            |         | size          | integer  |     | size                                                  | 4         |           |            |        | Size      | size
        """
        catalog_manifest_csv = convert_ascii_manifest_to_csv(catalog_manifest).decode("utf-8")
        manifest_csv = convert_ascii_manifest_to_csv(manifest).decode("utf-8")
        local_manifest_path.write_text(manifest_csv)

        dataset_id = "2"
        mock_dataset_manifest_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
            status_code=HTTPStatus.OK,
            text=catalog_manifest_csv,
            headers={"Content-Type": "text/csv"},
        )

        context, _ = ensure_temp_context_and_app(rc, tmp_path)

        # Do;
        execute_synchronization_catalog_to_agent(
            context, base_api_path, {"Authorization": "Bearer <token>"}, str(local_manifest_path), [dataset_id]
        )

        # Check.
        context, final_manifest = load_manifest_and_context(rc, local_manifest_path)
        assert render_tabular_manifest(context, final_manifest) == striptable("""
            id | d | r | b | m | property | type     | ref | source                                                | source.type | prepare | origin | count | level | status    | visibility | access | uri | eli | title     | description
            f8 | vssa                     |          |     |                                                       |             |         |        |       |       |           |            |        |     |     | VSSA      | vssa
            c3 |   | geography            | dask/csv |     | https://get.data.gov.lt/datasets/org/vssa/example/:ns |             |         |        |       | 4     |           |            |        |     |     | Geography | geography
               |                          |          |     |                                                       |             |         |        |       |       |           |            |        |     |     |           |
            7d |   |   |   | Country      |          | id  | model_country                                         |             |         |        |       | 4     | completed | package    | open   |     |     | Country   | country
            de |   |   |   |   | id       | integer  |     | id                                                    |             |         |        |       | 4     |           |            |        |     |     | Id        | id
            6c |   |   |   |   | size     | integer  |     | size                                                  |             |         |        |       | 4     |           |            |        |     |     | Size      | size
        """)

        assert get_request_context(mock_dataset_manifest_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
                "params": {},
                "data": {},
            }
        ]

    def test_catalog_fields_differ_from_agent_fields_catalog_fields_take_precedence_by_id(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        manifest: str,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
        configuration: tuple[str, str],
        base_api_path: str,
        tmp_path: PosixPath,
        local_manifest_path: PosixPath,
    ):
        catalog_manifest = """
            id                                   | dataset | resource   | model | property      | type     | ref  | source                                                | level     | status    | visibility | access | title      | description
            f89e1015-c77c-4d81-958c-52f0120e44a1 | cct     |            |       |               | dataset  | cct  | https://example.com                                   |           | open      |            |        | CCT        | cct
            c3caa75b-fbb6-4868-a366-e61e4f3225bf |         | technology |       |               | dask/csv |      | https://get.data.gov.lt/datasets/org/cct/example/:ns  | 4         |           |            |        | Technology | technology
                                                 |         |            |       |               |          |      |                                                       |           |           |            |        |            |
            7d5488e7-ce3c-4c64-90d8-f554a7721f20 |         |            | Item  |               |          | uuid | model_item                                            | 4         | completed | package    | open   | Item       | item
            de4107e4-7f9a-425e-ba7a-3626f59b360c |         |            |       | uuid          | integer  |      | uuid                                                  | 4         |           |            |        | Unique Id  | unique id
        """

        catalog_manifest_csv = convert_ascii_manifest_to_csv(catalog_manifest).decode("utf-8")
        manifest_csv = convert_ascii_manifest_to_csv(manifest).decode("utf-8")
        local_manifest_path.write_text(manifest_csv)

        dataset_id = "2"
        mock_dataset_manifest_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
            status_code=HTTPStatus.OK,
            text=catalog_manifest_csv,
            headers={"Content-Type": "text/csv"},
        )

        context, _ = ensure_temp_context_and_app(rc, tmp_path)

        # Do;
        execute_synchronization_catalog_to_agent(
            context, base_api_path, {"Authorization": "Bearer <token>"}, str(local_manifest_path), [dataset_id]
        )

        # Check.
        context, final_manifest = load_manifest_and_context(rc, local_manifest_path)
        assert render_tabular_manifest(context, final_manifest) == striptable("""
            id | d | r | b | m | property | type     | ref  | source                                               | source.type | prepare | origin | count | level | status    | visibility | access | uri | eli | title      | description
            f8 | cct                      |          |      |                                                      |             |         |        |       |       |           |            |        |     |     | CCT        | cct
            c3 |   | technology           | dask/csv |      | https://get.data.gov.lt/datasets/org/cct/example/:ns |             |         |        |       | 4     |           |            |        |     |     | Technology | technology
               |                          |          |      |                                                      |             |         |        |       |       |           |            |        |     |     |            |
            7d |   |   |   | Item         |          | uuid | model_item                                           |             |         |        |       | 4     | completed | package    | open   |     |     | Item       | item
            de |   |   |   |   | uuid     | integer  |      | uuid                                                 |             |         |        |       | 4     |           |            |        |     |     | Unique Id  | unique id
        """)

        assert get_request_context(mock_dataset_manifest_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
                "params": {},
                "data": {},
            }
        ]

    def test_catalog_fields_differ_from_agent_fields_catalog_fields_take_precedence_by_name(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
        configuration: tuple[str, str],
        base_api_path: str,
        tmp_path: PosixPath,
        local_manifest_path: PosixPath,
    ):
        local_manifest = """
            id | dataset | resource   | model   | property | type     | ref  | source                                                | level     | status    | visibility | access | title     | description
               | vssa    |            |         |          | dataset  | vssa | https://example.com                                   |           | open      |            |        | VSSA      | vssa
               |         | geography  |         |          | dask/csv |      | https://get.data.gov.lt/datasets/org/vssa/example/:ns | 4         |           |            |        | Geography | geography
               |         |            |         |          |          |      |                                                       |           |           |            |        |           |
               |         |            | Country |          |          | id   | model_country                                         | 4         | completed | package    | open   | Country   | country
               |         |            |         | id       | integer  |      | id                                                    | 4         |           |            |        | Id        | id
        """

        catalog_manifest = """
            id | dataset | resource  | model   | property | type     | ref   | source                                                | level     | status    | visibility | access | title      | description
               | vssa    |           |         |          | dataset  | vssa2 | https://example.com                                   |           | open      |            |        | VSSA2      | vssa2
               |         | geography |         |          | dask/csv |       | https://get.data.gov.lt/datasets/org/cct/example/:ns  | 4         |           |            |        | Geography2 | geography2
               |         |           |         |          |          |       |                                                       |           |           |            |        |            |
               |         |           | Country |          |          | id    | model_country2                                        | 4         | completed | package    | open   | Country2   | country2
               |         |           |         | id       | integer  |       | id2                                                   | 4         |           |            |        | id2        | id2
        """

        catalog_manifest_csv = convert_ascii_manifest_to_csv(catalog_manifest).decode("utf-8")
        manifest_csv = convert_ascii_manifest_to_csv(local_manifest).decode("utf-8")
        local_manifest_path.write_text(manifest_csv)

        dataset_id = "2"
        mock_dataset_manifest_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
            status_code=HTTPStatus.OK,
            text=catalog_manifest_csv,
            headers={"Content-Type": "text/csv"},
        )

        context, _ = ensure_temp_context_and_app(rc, tmp_path)

        # Do;
        execute_synchronization_catalog_to_agent(
            context, base_api_path, {"Authorization": "Bearer <token>"}, str(local_manifest_path), [dataset_id]
        )

        # Check.
        context, final_manifest = load_manifest_and_context(rc, local_manifest_path)
        assert render_tabular_manifest(context, final_manifest) == striptable("""
            id | d | r | b | m | property | type     | ref | source                                               | source.type | prepare | origin | count | level | status    | visibility | access | uri | eli | title      | description
               | vssa                     |          |     |                                                      |             |         |        |       |       |           |            |        |     |     | VSSA2      | vssa2
               |   | geography            | dask/csv |     | https://get.data.gov.lt/datasets/org/cct/example/:ns |             |         |        |       | 4     |           |            |        |     |     | Geography2 | geography2
               |                          |          |     |                                                      |             |         |        |       |       |           |            |        |     |     |            |
               |   |   |   | Country      |          | id  | model_country2                                       |             |         |        |       | 4     | completed | package    | open   |     |     | Country2   | country2
               |   |   |   |   | id       | integer  |     | id2                                                  |             |         |        |       | 4     |           |            |        |     |     | id2        | id2
        """)

        assert get_request_context(mock_dataset_manifest_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
                "params": {},
                "data": {},
            }
        ]

    def test_catalog_fields_differ_from_agent_fields_catalog_fields_take_precedence_by_source(
        self,
        rc: RawConfig,
        cli: SpintaCliRunner,
        requests_mock: MagicMock,
        credentials: RemoteClientCredentials,
        configuration: tuple[str, str],
        base_api_path: str,
        tmp_path: PosixPath,
        local_manifest_path: PosixPath,
    ):
        local_manifest = """
            id | dataset | resource   | model   | property | type     | ref  | source                                                   | level     | status    | visibility | access | title     | description
               | vssa    |            |         |          | dataset  | vssa | https://example.com                                      |           | open      |            |        | VSSA      | vssa
               |         | geography  |         |          | dask/csv |      | https://get.data.gov.lt/datasets/org/company/example/:ns | 4         |           |            |        | Geography | geography
               |         |            |         |          |          |      |                                                          |           |           |            |        |           |
               |         |            | Country |          |          | id   | model_a                                                  | 4         | completed | package    | open   | Country   | country
               |         |            |         | id       | integer  |      | property_a                                               | 4         |           |            |        | Id        | id
        """

        catalog_manifest = """
            id                                     | dataset | resource   | model   | property | type     | ref   | source                                                   | level     | status    | visibility | access | title      | description
            f89e1015-c77c-4d81-958c-52f0120e44a1   | cct     |            |         |          | dataset  | vssa2 | https://example.com                                      |           | open      |            |        | CCT        | cct
            c3caa75b-fbb6-4868-a366-e61e4f3225bf   |         | technology |         |          | dask/csv |       | https://get.data.gov.lt/datasets/org/company/example/:ns | 4         |           |            |        | Technology | technology
                                                   |         |            |         |          |          |       |                                                          |           |           |            |        |            |
            7d5488e7-ce3c-4c64-90d8-f554a7721f20   |         |            | Item    |          |          | uuid  | model_a                                                  | 4         | completed | package    | open   | Item       | item
            de4107e4-7f9a-425e-ba7a-3626f59b360c   |         |            |         | uuid     | integer  |       | property_a                                               | 4         |           |            |        | Unique ID  | unique id
        """

        catalog_manifest_csv = convert_ascii_manifest_to_csv(catalog_manifest).decode("utf-8")
        manifest_csv = convert_ascii_manifest_to_csv(local_manifest).decode("utf-8")
        local_manifest_path.write_text(manifest_csv)

        dataset_id = "2"
        mock_dataset_manifest_get = requests_mock.get(
            f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
            status_code=HTTPStatus.OK,
            text=catalog_manifest_csv,
            headers={"Content-Type": "text/csv"},
        )

        context, _ = ensure_temp_context_and_app(rc, tmp_path)

        # Do;
        execute_synchronization_catalog_to_agent(
            context, base_api_path, {"Authorization": "Bearer <token>"}, str(local_manifest_path), [dataset_id]
        )

        # Check.
        context, final_manifest = load_manifest_and_context(rc, local_manifest_path)
        assert render_tabular_manifest(context, final_manifest) == striptable("""
        id | d | r | b | m | property | type     | ref  | source                                                   | source.type | prepare | origin | count | level | status    | visibility | access | uri | eli | title      | description
        f8 | cct                      |          |      |                                                          |             |         |        |       |       |           |            |        |     |     | CCT        | cct
        c3 |   | technology           | dask/csv |      | https://get.data.gov.lt/datasets/org/company/example/:ns |             |         |        |       | 4     |           |            |        |     |     | Technology | technology
           |                          |          |      |                                                          |             |         |        |       |       |           |            |        |     |     |            |
        7d |   |   |   | Item         |          | uuid | model_a                                                  |             |         |        |       | 4     | completed | package    | open   |     |     | Item       | item
        de |   |   |   |   | uuid     | integer  |      | property_a                                               |             |         |        |       | 4     |           |            |        |     |     | Unique ID  | unique id
        """)

        assert get_request_context(mock_dataset_manifest_get) == [
            {
                "method": "GET",
                "url": f"{credentials.resource_server}/uapi/datasets/gov/vssa/ror/dcat/Dataset/{dataset_id}/dsa",
                "params": {},
                "data": {},
            }
        ]
