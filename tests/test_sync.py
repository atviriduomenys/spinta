from http import HTTPStatus
from pathlib import PosixPath
from typing import Any
from unittest.mock import patch, MagicMock, ANY
from urllib.parse import parse_qs, quote

import pytest
import sqlalchemy as sa
from requests_mock.adapter import _Matcher

from spinta.cli.helpers.sync.controllers.enum import ResourceType
from spinta.client import RemoteClientCredentials
from spinta.core.config import RawConfig
from spinta.exceptions import (
    NotImplementedFeature,
    UnexpectedAPIResponse,
    UnexpectedAPIResponseData,
    InvalidCredentialsConfigurationException,
)
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.context import ContextForTests
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest


def get_request_context(mocked_request: _Matcher, with_text: bool = False) -> list[dict[str, Any]]:
    """Helper method to build context of what the mocked URLs were called with (Content, query params, URL)."""
    calls = []
    for request in mocked_request.request_history:
        data = {"method": request.method, "url": request.url, "params": request.qs, "data": parse_qs(request.text)}
        if with_text:
            data.update({"text": request.text.replace("\r\n", "\n").rstrip("\n")})
        calls.append(data)
    return calls


@pytest.fixture
def patched_credentials():
    credentials = RemoteClientCredentials(
        section="default",
        remote="origin",
        client="client-id",
        secret="secret",
        server="http://example.com",
        resource_server="http://example.com",
        scopes="scope1 scope2",
        organization="vssa",
        organization_type="gov",
    )
    with patch("spinta.cli.helpers.sync.helpers.get_client_credentials", return_value=credentials):
        yield credentials


@pytest.fixture
def sqlite_instance(sqlite: Sqlite) -> Sqlite:
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("ID", sa.Integer, primary_key=True),
                sa.Column("CODE", sa.Text),
                sa.Column("NAME", sa.Text),
            ],
            "CITY": [
                sa.Column("NAME", sa.Text),
                sa.Column("COUNTRY_ID", sa.Integer, sa.ForeignKey("COUNTRY.ID")),
            ],
        }
    )
    return sqlite


@pytest.fixture
def base_uapi_url() -> str:
    """Static part of the UAPI type URL to reach open data catalog"""
    return "uapi/datasets/org/vssa/isris/dcat"


@pytest.fixture
def dataset_prefix(patched_credentials: RemoteClientCredentials) -> str:
    return f"datasets/{patched_credentials.organization_type}/{patched_credentials.organization}"


@pytest.fixture
def manifest_path(context: ContextForTests, tmp_path: PosixPath) -> PosixPath:
    """Build csv file and returns its path."""
    manifest = striptable("""
        id | d | r | b | m | property      | type    | ref     | source  | level | status    | visibility | access | title | description
           | example                       |         |         |         |       |           |            |        |       |
           |   | cities                    | sql     | default | default |       |           | public     |        |       |
           |                               |         |         |         |       |           |            |        |       |
           |   |   |   | City              |         | id      | users   | 4     | completed | public     | open   | Name  |
           |   |   |   |   | id            | integer |         | id      |       |           | public     |        |       |
           |   |   |   |   | full_name     | string  |         | name    |       |           | public     |        |       |
        """)
    manifest_path = tmp_path / "manifest.csv"
    create_tabular_manifest(context, manifest_path, manifest)
    return manifest_path


def test_success_existing_dataset(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_data_service_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.OK, "json": {"_data": [{"_id": 2}]}},
        ],
    )
    mock_dataset_put = requests_mock.put(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
        status_code=HTTPStatus.NOT_IMPLEMENTED,
        json={},
    )
    dataset_name = "example"
    agent_name = patched_credentials.client

    # Act
    with pytest.raises(NotImplementedFeature) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "status": HTTPStatus.NOT_IMPLEMENTED.value,
        "dataset_id": 2,
        "feature": "Updates on existing Datasets",
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_data_service_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        }
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name, safe='')}",
            "params": {"name": [dataset_name]},
            "data": {},
        },
    ]
    assert get_request_context(mock_dataset_put) == [
        {
            "method": "PUT",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
            "params": {},
            "data": {},
        }
    ]


def test_success_new_dataset(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
        ],
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 1}},  # Creates Data Service.
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 2}},  # Creates Dataset No. 1.
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 3}},  # Creates Dataset No. 2.
        ],
    )
    mock_dsa_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
        status_code=HTTPStatus.NO_CONTENT,
        json={},
    )
    mock_dsa_post_2 = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/3/dsa/",
        status_code=HTTPStatus.NO_CONTENT,
        json={},
    )

    agent_name = patched_credentials.client
    dataset_name_example = "example"
    dataset_name_sqlite = "db_sqlite"

    # Act
    cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_example}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_sqlite}",
            "params": {"name": [dataset_name_sqlite]},
            "data": {},
        },
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "service": ["True"],
                "subclass": [ResourceType.DATA_SERVICE],
            },
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "parent_id": ["1"],
                "subclass": [ResourceType.DATASET],
            },
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_sqlite],
                "title": [dataset_name_sqlite],
                "parent_id": ["1"],
                "subclass": [ResourceType.DATASET],
            },
        },
    ]
    assert get_request_context(mock_dsa_post, with_text=True) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
            "params": {},
            "data": {},
            "text": """id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description
,datasets/gov/vssa/example,,,,,,,,,,,,,,,,,,,
,,cities,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,
,,,,/example/City,,,id,users,,,,,4,completed,public,open,,,Name,
,,,,,id,integer,,id,,,,,,,public,,,,,
,,,,,full_name,string,,name,,,,,,,public,,,,,""",
        }
    ]
    assert get_request_context(mock_dsa_post_2, with_text=True) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/3/dsa/",
            "params": {},
            "data": {},
            "text": """id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description""",
        },
    ]


def test_success_private_fields_cleaned_successfully(
    rc: RawConfig,
    cli: SpintaCliRunner,
    context: ContextForTests,
    tmp_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
        ],
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 1}},  # Creates Data Service.
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 2}},  # Creates Dataset No. 1.
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 3}},  # Creates Dataset No. 2.
        ],
    )
    mock_dsa_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
        status_code=HTTPStatus.NO_CONTENT,
        json={},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/3/dsa/",
        status_code=HTTPStatus.NO_CONTENT,
        json={},
    )

    sqlite.init({})
    manifest = striptable("""
    id | dataset | resource  | base | model   | property   | type    | ref     | source                    | prepare | level | status    | visibility | access  | uri | eli | title     | description
       | example |           |      |         |            |         |         |                           |         |       |           |            |         |     |     |           |
       |         | countries |      |         |            | sql     | default | default                   |         |       |           |            |         |     |     |           |
       |         |           |      | Country |            |         |         | country_source            |         | 4     | completed | private    | open    |     |     | Countries |
       |         |           |      |         | id         | integer |         | country_id_source         |         |       |           | package    | open    |     |     |           |
       |         |           |      |         | name       | string  |         | country_name_source       |         |       |           | package    | open    |     |     |           |
       |         | cities    |      |         |            | sql     | default | default                   |         |       |           |            |         |     |     |           |
       |         |           |      | City    |            |         |         | city_source               |         | 4     | completed | private    | open    |     |     | Cities    |
       |         |           |      |         | id         | integer |         | city_id_source            |         |       |           | private    | open    |     |     |           |
       |         |           |      |         | size       | integer |         | city_size_source          |         |       |           | private    | open    |     |     |           |
       |         |           |      | Village |            |         |         | village_source            |         | 4     | completed | package    | open    |     |     | Villages  |
       |         |           |      |         | id         | integer |         | village_id_source         |         |       |           | package    | open    |     |     |           |
       |         |           |      |         | population | integer |         | village_population_source |         |       |           | private    | private |     |     |           |
    """)
    manifest_path = tmp_path / "manifest.csv"
    create_tabular_manifest(context, manifest_path, manifest)

    # Act
    cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite.dsn], catch_exceptions=False)

    # Assert
    assert get_request_context(mock_dsa_post, with_text=True) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
            "params": {},
            "data": {},
            "text": """id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description
,datasets/gov/vssa/example,,,,,,,,,,,,,,,,,,,
,,cities,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,
,,,,/example/Village,,,,village_source,,,,,4,completed,package,open,,,Villages,
,,,,,id,integer,,village_id_source,,,,,,,package,open,,,,""",
        }
    ]


def test_failure_configuration_invalid(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    credentials = RemoteClientCredentials(
        section="default",
        remote="origin",
        secret="secret",
        scopes="scope1 scope2",
        client="client",
        server="https://example.com",
    )
    requests_mock.post(
        f"{credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    with patch("spinta.cli.helpers.sync.helpers.get_client_credentials", return_value=credentials):
        # Act
        with pytest.raises(InvalidCredentialsConfigurationException) as exception:
            cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

        # Assert
        assert exception.value.status_code == 400
        assert exception.value.context == {"missing_credentials": "resource_server, organization_type, organization"}


def test_failure_get_access_token_api_call(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    sqlite_instance: Sqlite,
):
    # Arrange
    token_url = f"{patched_credentials.server}/auth/token"
    mock_auth_token_post = requests_mock.post(
        token_url,
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={"error": "server error"},
    )

    # Act
    with pytest.raises(Exception) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]


def test_failure_post_data_service_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    mock_data_service_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )

    agent_name = patched_credentials.client

    # Act
    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert exception.value.context == {
        "operation": "Create resource",
        "expected_status_code": str({HTTPStatus.CREATED.value}),
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        "response_data": str({}),
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
    ]
    assert get_request_context(mock_data_service_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        }
    ]


def test_failure_post_data_service_returns_invalid_data(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_data_service_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    mock_data_service_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={},
    )

    agent_name = patched_credentials.client

    # Act
    with pytest.raises(UnexpectedAPIResponseData) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert exception.value.context == {
        "operation": "Retrieve dataset `_id`",
        "context": "Dataset did not return the `_id` field which can be used to identify the dataset.",
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_data_service_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
    ]
    assert get_request_context(mock_data_service_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        }
    ]


def test_failure_get_dataset_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_data_service_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {
                "status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
                "json": {
                    "code": "dataset_not_found",
                    "type": "DatasetNotFound",
                    "template": "The requested Dataset could not be found.",
                    "message": "No dataset matched the provided query.",
                    "status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
                },
            },
        ],
    )

    dataset_name_example = "example"
    agent_name = patched_credentials.client

    # Act
    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert exception.value.context == {
        "operation": "Get resource",
        "expected_status_code": str({HTTPStatus.OK.value, HTTPStatus.NOT_FOUND.value}),
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        "response_data": str(
            {
                "code": "dataset_not_found",
                "type": "DatasetNotFound",
                "template": "The requested Dataset could not be found.",
                "message": "No dataset matched the provided query.",
                "status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
            }
        ),
    }
    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_data_service_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        }
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_example}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        },
    ]


def test_failure_get_dataset_returns_invalid_data(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_data_service_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.OK, "json": {}},
        ],
    )

    agent_name = patched_credentials.client
    dataset_name_example = "example"

    # Act
    with pytest.raises(UnexpectedAPIResponseData) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Retrieve dataset `_id`",
        "context": "Dataset did not return the `_id` field which can be used to identify the dataset.",
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_data_service_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        }
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_example}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        },
    ]


def test_failure_put_dataset_returns_invalid_data(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    """Check the workflow, when DSA put endpoint returns an invalid response.

    Since it is not implemented, it will return an internal server error for now, but when it is implemented, this
    test will need to be updated.
    """
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_data_service_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.OK, "json": {"_data": [{"_id": 2}]}},
        ],
    )
    mock_dsa_put = requests_mock.put(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    agent_name = patched_credentials.client
    dataset_name_example = "example"

    # Act
    with pytest.raises(NotImplementedFeature) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "status": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        "dataset_id": 2,
        "feature": "Updates on existing Datasets",
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_data_service_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        }
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_example}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        },
    ]
    assert get_request_context(mock_dsa_put) == [
        {
            "method": "PUT",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
            "params": {},
            "data": {},
        },
    ]


def test_failure_post_dataset_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 1}},
            {"status_code": HTTPStatus.INTERNAL_SERVER_ERROR, "json": {}},
        ],
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
        ],
    )

    agent_name = patched_credentials.client
    dataset_name_example = "example"

    # Act
    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Create resource",
        "expected_status_code": str({HTTPStatus.CREATED.value}),
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        "response_data": str({}),
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "parent_id": ["1"],
                "subclass": [ResourceType.DATASET],
            },
        },
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_example}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        },
    ]


def test_failure_post_dataset_returns_invalid_data(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
        ],
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 1}},
            {"status_code": HTTPStatus.CREATED, "json": {}},
        ],
    )

    agent_name = patched_credentials.client
    dataset_name_example = "example"

    # Act
    with pytest.raises(UnexpectedAPIResponseData) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Retrieve dataset `_id`",
        "context": "Dataset did not return the `_id` field which can be used to identify the dataset.",
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "parent_id": ["1"],
                "subclass": [ResourceType.DATASET],
            },
        },
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_example}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        },
    ]


def test_failure_post_dsa_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
    sqlite_instance: Sqlite,
    dataset_prefix: str,
):
    # Arrange
    mock_auth_token_post = requests_mock.post(
        f"{patched_credentials.server}/auth/token",
        status_code=HTTPStatus.OK,
        json={"access_token": "test-token"},
    )
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
            {"status_code": HTTPStatus.NOT_FOUND, "json": {}},
        ],
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        [
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 1}},
            {"status_code": HTTPStatus.CREATED, "json": {"_id": 2}},
        ],
    )
    mock_dsa_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )

    agent_name = patched_credentials.client
    dataset_name_example = "example"

    # Act
    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Create DSA",
        "expected_status_code": str({HTTPStatus.NO_CONTENT.value}),
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        "response_data": str({}),
    }

    assert get_request_context(mock_auth_token_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/auth/token",
            "params": {},
            "data": {
                "grant_type": ["client_credentials"],
                "scope": [patched_credentials.scopes],
            },
        }
    ]
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={agent_name}",
            "params": {"name": [agent_name]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={dataset_name_example}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        },
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [patched_credentials.client],
                "title": [patched_credentials.client],
                "subclass": [ResourceType.DATA_SERVICE.value],
                "service": ["True"],
            },
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "parent_id": ["1"],
                "subclass": [ResourceType.DATASET],
            },
        },
    ]
    assert get_request_context(mock_dsa_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/2/dsa/",
            "params": {},
            "data": ANY,  # DSA Content from file.
        },
    ]
