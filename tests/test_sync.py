from http import HTTPStatus
from pathlib import PosixPath
from unittest.mock import patch, MagicMock

import pytest

from spinta.client import RemoteClientCredentials
from spinta.core.config import RawConfig
from spinta.exceptions import NotImplementedFeature, UnexpectedAPIResponse, UnexpectedAPIResponseData
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.context import ContextForTests
from spinta.testing.tabular import create_tabular_manifest


@pytest.fixture
def patched_credentials():
    credentials = RemoteClientCredentials(
        section='default',
        remote='origin',
        client="client-id",
        secret="secret",
        server="http://example.com",
        scopes="scope1 scope2",
    )
    with patch('spinta.cli.sync.get_client_credentials', return_value=credentials):
        yield credentials


@pytest.fixture
def base_uapi_url() -> str:
    """Static part of the UAPI type URL to reach open data catalog"""
    return "uapi/datasets/org/vssa/isris/dcat"


@pytest.fixture
def manifest_path(context: ContextForTests, tmp_path: PosixPath) -> PosixPath:
    """Build csv file and returns its path."""
    manifest = striptable("""
        id | d | r | b | m | property      | type    | ref     | source | level | status    | visibility | access | title | description
           | example                       |         |         |        |       |           |            |        |       |
           |   | cities                    |         | default |        |       |           |            |        |       |
           |                               |         |         |        |       |           |            |        |       |
           |   |   |   | City              |         | id      | users  | 4     | completed | package    | open   | Name  |
           |   |   |   |   | id            | integer |         | id     |       |           |            |        |       |
           |   |   |   |   | full_name     | string  |         | name   |       |           |            |        |       |
        """)
    manifest_path = tmp_path / 'manifest.csv'
    create_tabular_manifest(context, manifest_path, manifest)
    return manifest_path


def test_success_existing_dataset(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK, json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code = HTTPStatus.OK,
        json = {"_data": [{"_id": 1}]}
    )
    requests_mock.put(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.NOT_IMPLEMENTED,
        json={},
    )

    with pytest.raises(NotImplementedFeature) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "status": HTTPStatus.NOT_IMPLEMENTED,
        "dataset_id": 1,
        "feature": "Updates on existing Datasets"
    }


def test_success_new_dataset(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK, json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.NO_CONTENT,
        json={},
    )

    # Should not raise any error.
    cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)


def test_failure_multiple_datasets(context: ContextForTests, rc: RawConfig, cli: SpintaCliRunner, tmp_path: PosixPath):
    """Checks that multiple dataset support is not yet implemented."""
    manifest = striptable("""
        id | d | r | b | m | property      | type    | ref     | source | level | status    | visibility | access | title | description
           | example                       |         |         |        |       |           |            |        |       |
           |   | cities                    |         | default |        |       |           |            |        |       |
           |                               |         |         |        |       |           |            |        |       |
           |   |   |   | City              |         | id      | users  | 4     | completed | package    | open   | Name  |
           |   |   |   |   | id            | integer |         | id     |       |           |            |        |       |
           |   |   |   |   | full_name     | string  |         | name   |       |           |            |        |       |
           | example2                      |         |         |        |       |           |            |        |       |
    """)
    manifest_path = tmp_path / 'manifest.csv'
    create_tabular_manifest(context, manifest_path, manifest)

    with pytest.raises(NotImplementedFeature) as exc_info:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert "Synchronizing more than 1 dataset at a time" in str(exc_info.value)


def test_failure_get_access_token_api_call(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
):
    token_url = f"{patched_credentials.server}/auth/token"
    requests_mock.post(token_url, status_code=HTTPStatus.INTERNAL_SERVER_ERROR, json={"error": "server error"})

    with pytest.raises(Exception) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR


def test_failure_get_dataset_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK, json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={
            "code": "dataset_not_found",
            "type": "DatasetNotFound",
            "template": "The requested Dataset could not be found.",
            "message": f"No dataset matched the provided query.",
            "status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
        }
    )

    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert exception.value.context == {
        "operation": "Get dataset",
        "expected_status_code": str({HTTPStatus.OK, HTTPStatus.NOT_FOUND}),
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
        "response_data": str({
            "code": "dataset_not_found",
            "type": "DatasetNotFound",
            "template": "The requested Dataset could not be found.",
            "message": "No dataset matched the provided query.",
            "status_code": HTTPStatus.INTERNAL_SERVER_ERROR.value,
        }),
    }


def test_failure_get_dataset_returns_invalid_data(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK, json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code = HTTPStatus.OK,
        json = {}
    )

    with pytest.raises(UnexpectedAPIResponseData) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert exception.value.context == {
        "operation": "Retrieve dataset `_id`",
        "context": "Dataset did not return the `_id` field which can be used to identify the dataset."
    }


def test_failure_put_dataset_returns_invalid_data(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    """Check the workflow, when DSA put endpoint returns an invalid response.

    Since it is not implemented, it will return an internal server error for now, but when it will be implemented, this
    test will need to be updated.
    """
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK, json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code = HTTPStatus.OK,
        json = {"_data": [{"_id": 1}]}
    )
    requests_mock.put(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    with pytest.raises(NotImplementedFeature) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "status": HTTPStatus.INTERNAL_SERVER_ERROR,
        "dataset_id": 1,
        "feature": "Updates on existing Datasets"
    }


def test_failure_post_dataset_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK,
                       json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )

    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Create dataset",
        "expected_status_code": HTTPStatus.CREATED,
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
        "response_data": str({})
    }


def test_failure_post_dataset_returns_invalid_data(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK,
                       json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={},
    )

    with pytest.raises(UnexpectedAPIResponseData) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": f"Retrieve dataset `_id`",
        "context": f"Dataset did not return the `_id` field which can be used to identify the dataset."
    }


def test_failure_post_distribution_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK,
                       json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )

    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Create dataset distribution",
        "expected_status_code": HTTPStatus.CREATED,
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
        "response_data": str({})
    }


def test_failure_post_dsa_returns_unexpected_status_code(
    rc: RawConfig,
    cli: SpintaCliRunner,
    manifest_path: PosixPath,
    requests_mock: MagicMock,
    patched_credentials: RemoteClientCredentials,
    base_uapi_url: str,
):
    requests_mock.post(f"{patched_credentials.server}/auth/token", status_code=HTTPStatus.OK,
                       json={"access_token": "test-token"})
    requests_mock.get(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.NOT_FOUND,
        json={},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Distribution/",
        status_code=HTTPStatus.CREATED,
        json={"_id": 1},
    )
    requests_mock.post(
        f"{patched_credentials.server}/{base_uapi_url}/Dataset/1/dsa/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        json={},
    )

    with pytest.raises(UnexpectedAPIResponse) as exception:
        cli.invoke(rc, args=["sync", manifest_path], catch_exceptions=False)

    assert exception.value.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
    assert exception.value.context == {
        "operation": "Create DSA for dataset",
        "expected_status_code": HTTPStatus.CREATED,
        "response_status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
        "response_data": str({})
    }
