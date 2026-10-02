import configparser
from pathlib import Path
from textwrap import dedent
from urllib.parse import parse_qs

import pytest
from responses import POST, RequestsMock

from spinta.client import add_client_credentials, get_access_token, get_client_credentials
from spinta.exceptions import RemoteClientCredentialsNotFound


@pytest.mark.parametrize(
    "url,scopes",
    [
        ("example", "uapi:/:getall uapi:/:getone"),
        ("example.com", "uapi:/:getall uapi:/:getone"),
        ("spinta@example.com", "uapi:/:getall uapi:/:getone"),
        ("https://spinta@example.com", "uapi:/:getall uapi:/:getone"),
        ("https://example.com", "uapi:/:getall uapi:/:getone"),
    ],
)
def test_get_access_token(
    responses: RequestsMock,
    tmp_path: Path,
    url: str,
    scopes: str,
):
    credsfile = Path(tmp_path / "credentials.cfg")
    credsfile.write_text(
        dedent(f"""
    [example]
    server = https://example.com
    client = spinta
    secret = verysecret
    scopes = {scopes}
    
    [example.com]
    client = spinta
    secret = verysecret
    scopes = {scopes}
        
    [spinta@example.com]
    client = spinta
    secret = verysecret
    scopes = {scopes}
    """)
    )
    responses.add(
        POST,
        "https://example.com/auth/token",
        json={
            "access_token": "TOKEN",
        },
    )
    creds = get_client_credentials(credsfile, url)
    token = get_access_token(creds)
    assert token == "TOKEN"


def test_get_access_token_no_credsfile(tmp_path: Path):
    credsfile = Path(tmp_path / "credentials.cfg")
    with pytest.raises(RemoteClientCredentialsNotFound):
        creds = get_client_credentials(credsfile, "https://example.com")
        get_access_token(creds)


@pytest.mark.parametrize("scopes", ["spinta_getall spinta_getone", "uapi:/:getall uapi:/:getone"])
def test_get_access_token_no_section(tmp_path: Path, scopes: str):
    credsfile = Path(tmp_path / "credentials.cfg")
    credsfile.write_text(
        dedent(f"""
    [test.example.com]
    client = spinta
    secret = verysecret
    scopes = {scopes}
    """)
    )
    with pytest.raises(RemoteClientCredentialsNotFound):
        creds = get_client_credentials(credsfile, "https://example.com")
        get_access_token(creds)


def test_add_client_credentials(tmp_path: Path):
    credsfile = Path(tmp_path / "credentials.cfg")

    add_client_credentials(credsfile, "example.com")
    add_client_credentials(credsfile, "spinta@example.com")
    add_client_credentials(credsfile, "spinta@example.com", section="example")

    creds = configparser.ConfigParser()
    creds.read(credsfile)

    assert dict(creds["example.com"]) == {
        "server": "https://example.com",
        "client": "",
        "secret": "",
        "scopes": "",
    }

    assert dict(creds["spinta@example.com"]) == {
        "server": "https://example.com",
        "client": "spinta",
        "secret": "",
        "scopes": "",
    }

    assert dict(creds["example"]) == {
        "server": "https://example.com",
        "client": "spinta",
        "secret": "",
        "scopes": "",
    }


@pytest.mark.parametrize("scopes", [["spinta_getall spinta_getone"], ["uapi:/:getall uapi:/:getone"]])
def test_add_client_credentials_kwargs(tmp_path: Path, scopes: list):
    credsfile = Path(tmp_path / "credentials.cfg")

    add_client_credentials(credsfile, "https://example.com", client="spinta", secret="verysecret", scopes=scopes)
    creds = configparser.ConfigParser()
    creds.read(credsfile)

    expected_scopes = "\n" + "\n".join(scopes)
    assert dict(creds["example.com"]) == {
        "server": "https://example.com",
        "client": "spinta",
        "secret": "verysecret",
        "scopes": expected_scopes,
    }


@pytest.mark.parametrize(
    "name, remote",
    [
        ("example", "example"),
        ("example.com", "example_com"),
        ("spinta@example.com", "example_com"),
        ("https://spinta@example.com", "example_com"),
        ("https://example.com", "example_com"),
        ("https://example.com:80", "example_com"),
        ("https://example.com:443", "example_com"),
        ("https://example.com:8000", "example_com_8000"),
    ],
)
def test_get_client_credentials_remote(name: str, remote: str):
    creds = get_client_credentials(None, name, check=False)
    assert creds.remote == remote


def test_get_client_credentials_new_keys(tmp_path: Path):
    credsfile = Path(tmp_path / "credentials.cfg")
    credsfile.write_text(
        dedent("""
    [katalogas]
    auth_server_url = https://auth.example.com
    server = https://old-auth.example.com
    resource_server_url = https://data.gov.lt/uapi/
    resource_server = https://old.data.gov.lt/uapi/
    resource_server_id = https://data.gov.lt/uapi/
    client = spinta
    secret = verysecret
    scopes = uapi:/:getall
    """)
    )
    creds = get_client_credentials(credsfile, "katalogas")
    assert creds.server == "https://auth.example.com"
    assert creds.resource_server == "https://data.gov.lt/uapi/"
    assert creds.resource_server_id == "https://data.gov.lt/uapi/"


def test_get_client_credentials_old_keys(tmp_path: Path):
    credsfile = Path(tmp_path / "credentials.cfg")
    credsfile.write_text(
        dedent("""
    [default]
    server = https://auth.example.com
    resource_server = https://data.gov.lt
    client = spinta
    secret = verysecret
    scopes = uapi:/:getall
    """)
    )
    creds = get_client_credentials(credsfile, "default")
    assert creds.server == "https://auth.example.com"
    assert creds.resource_server == "https://data.gov.lt"
    assert creds.resource_server_id is None


@pytest.mark.parametrize("resource_server_id", [None, "https://data.gov.lt/uapi/"])
def test_get_access_token_sends_resource(responses: RequestsMock, tmp_path: Path, resource_server_id):
    credsfile = Path(tmp_path / "credentials.cfg")
    resource_line = f"resource_server_id = {resource_server_id}" if resource_server_id else ""
    credsfile.write_text(
        dedent(f"""
    [katalogas]
    auth_server_url = https://auth.example.com
    {resource_line}
    client = spinta
    secret = verysecret
    scopes = uapi:/:getall
    """)
    )
    responses.add(POST, "https://auth.example.com/auth/token", json={"access_token": "TOKEN"})

    get_access_token(get_client_credentials(credsfile, "katalogas"))

    body = parse_qs(responses.calls[0].request.body)
    assert body.get("resource") == ([resource_server_id] if resource_server_id else None)
