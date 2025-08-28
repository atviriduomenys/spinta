from http import HTTPStatus
from pathlib import PosixPath
from typing import Any
from unittest.mock import patch, MagicMock, ANY
from urllib.parse import parse_qs, quote

import pytest
import sqlalchemy as sa
from requests_mock.adapter import _Matcher

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


def get_full_dataset_name(dataset_prefix: str, dataset_name: str) -> str:
    """Helper method to build a full dataset name: `prefix + dataset name`."""
    return f"{dataset_prefix}/{dataset_name}"


@pytest.fixture
def patched_credentials():
    credentials = RemoteClientCredentials(
        section="default",
        remote="origin",
        client="client-id",
        secret="secret",
        server="http://example.com",
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
    manifest = striptable(f"""
        id | d | r | b | m | property      | type    | ref     | source  | level | status    | visibility | access | title | description
           | example                       |         |         |         |       |           |            |        |       |
           |   | cities                    | sql     | default | default |       |           | private    |        |       |
           |                               |         |         |         |       |           |            |        |       |
           |   |   |   | City              |         | id      | users   | 4     | completed | private    | open   | Name  |
           |   |   |   |   | id            | integer |         | id      |       |           | private    |        |       |
           |   |   |   |   | full_name     | string  |         | name    |       |           | private    |        |       |
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
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.OK,
        json={"_data": [{"_id": 1}]},
    )
    mock_dataset_put = requests_mock.put(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.NOT_IMPLEMENTED,
        json={},
    )
    dataset_name = f"{dataset_prefix}/example"

    # Act
    with pytest.raises(NotImplementedFeature) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "status": HTTPStatus.NOT_IMPLEMENTED.value,
        "dataset_id": 1,
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
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name, safe='')}",
            "params": {"name": [dataset_name]},
            "data": {},
        }
    ]
    assert get_request_context(mock_dataset_put) == [
        {
            "method": "PUT",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
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
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_distribution_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_dsa_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.NO_CONTENT,
        json={},
    )
    dataset_name_example = f"{dataset_prefix}/example"
    dataset_name_sqlite = f"{dataset_prefix}/db_sqlite"

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
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [f"{dataset_prefix}/example"]},
            "data": {},
        },
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_sqlite, safe='')}",
            "params": {"name": [f"{dataset_prefix}/db_sqlite"]},
            "data": {},
        },
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "service": ["True"],
                "subclass": ["service"],
            },
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_sqlite],
                "title": [dataset_name_sqlite],
                "service": ["True"],
                "subclass": ["service"],
            },
        },
    ]
    assert get_request_context(mock_distribution_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
            "params": {},
            "data": ANY,  # DSA content + SQLite content.
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
            "params": {},
            "data": ANY,  # DSA content + SQLite content.
        },
    ]
    assert get_request_context(mock_dsa_post, with_text=True) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
            "params": {},
            "data": {},
            "text": """id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description
,example,,,,,,,,,,,,,,,,,,,
,,cities,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,
,,,,City,,,id,,,,,,4,completed,private,open,,,Name,
,,,,,id,integer,,,,,,,,,private,,,,,
,,,,,full_name,string,,,,,,,,,private,,,,,""",
        },
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
            "params": {},
            "data": {},
            "text": """id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description
,db_sqlite,,,,,,,,,,,,,,,,,,,
,,resource1,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,
,,,,City,,,,,,,,,,develop,private,,,,,
,,,,,country_id,ref,Country,,,,,,,develop,private,,,,,
,,,,,name,string,,,,,,,,develop,private,,,,,
,,,,,,,,,,,,,,,,,,,,
,,,,Country,,,id,,,,,,,develop,private,,,,,
,,,,,code,string,,,,,,,,develop,private,,,,,
,,,,,id,integer,,,,,,,,develop,private,,,,,
,,,,,name,string,,,,,,,,develop,private,,,,,""",
        },
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
    # No `organization` or `organization_type`
    credentials = RemoteClientCredentials(
        section="default",
        remote="origin",
        client="client-id",
        secret="secret",
        server="http://example.com",
        scopes="scope1 scope2",
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
        assert exception.value.context == {"required_credentials": str(["organization_type", "organization"])}


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
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={
            "code": "dataset_not_found",
            "type": "DatasetNotFound",
            "template": "The requested Dataset could not be found.",
            "message": "No dataset matched the provided query.",
            "status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        },
    )
    dataset_name_example = f"{dataset_prefix}/example"

    # Act
    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert exception.value.context == {
        "operation": "Get dataset",
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
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        }
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
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.OK,
        json={},
    )
    dataset_name_example = f"{dataset_prefix}/example"

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
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        }
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
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.OK,
        json={"_data": [{"_id": 1}]},
    )
    mock_dsa_put = requests_mock.put(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
    )
    dataset_name_example = f"{dataset_prefix}/example"

    # Act
    with pytest.raises(NotImplementedFeature) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "status": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        "dataset_id": 1,
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
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        }
    ]
    assert get_request_context(mock_dsa_put) == [
        {
            "method": "PUT",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
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
    mock_dataset_get = requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )
    dataset_name_example = f"{dataset_prefix}/example"

    # Act
    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Create dataset",
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
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        }
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "service": ["True"],
                "subclass": ["service"],
            },
        }
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
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={},
    )
    dataset_name_example = f"{dataset_prefix}/example"

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
    assert get_request_context(mock_dataset_get) == [
        {
            "method": "GET",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        }
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "service": ["True"],
                "subclass": ["service"],
            },
        }
    ]


def test_failure_post_distribution_returns_unexpected_status_code(
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
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_distribution_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )
    dataset_name_example = f"{dataset_prefix}/example"

    # Act
    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path, "-r", "sql", sqlite_instance.dsn], catch_exceptions=False)

    # Assert
    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Create distribution",
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
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        }
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "service": ["True"],
                "subclass": ["service"],
            },
        }
    ]
    assert get_request_context(mock_distribution_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
            "params": {},
            "data": ANY,  # DSA content + SQLite content.
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
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    mock_dataset_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_distribution_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    mock_dsa_post = requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )
    dataset_name_example = f"{dataset_prefix}/example"

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
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/?name={quote(dataset_name_example, safe='')}",
            "params": {"name": [dataset_name_example]},
            "data": {},
        }
    ]
    assert get_request_context(mock_dataset_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
            "params": {},
            "data": {
                "name": [dataset_name_example],
                "title": [dataset_name_example],
                "service": ["True"],
                "subclass": ["service"],
            },
        }
    ]
    assert get_request_context(mock_distribution_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
            "params": {},
            "data": ANY,  # DSA content + SQLite content.
        },
    ]
    assert get_request_context(mock_dsa_post) == [
        {
            "method": "POST",
            "url": f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
            "params": {},
            "data": ANY,  # DSA Content from file.
        },
    ]
