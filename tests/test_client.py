import configparser
from pathlib import Path
from textwrap import dedent

import pytest
from responses import POST
from responses import RequestsMock

from spinta.client import add_client_credentials
from spinta.client import get_access_token
from spinta.client import get_client_credentials
from spinta.exceptions import RemoteClientCredentialsNotFound


@pytest.mark.parametrize(
    "url,scope",
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
    scope: str,
):
    credsfile = Path(tmp_path / "credentials.cfg")
    credsfile.write_text(
        dedent(f"""
    [example]
    server = https://example.com
    client = spinta
    secret = verysecret
    scopes = {scope}
    
    [example.com]
    client = spinta
    secret = verysecret
    scopes = {scope}
        
    [spinta@example.com]
    client = spinta
    secret = verysecret
    scopes = {scope}
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


@pytest.mark.parametrize("scope", ["spinta_getall spinta_getone", "uapi:/:getall uapi:/:getone"])
def test_get_access_token_no_section(tmp_path: Path, scope: str):
    credsfile = Path(tmp_path / "credentials.cfg")
    credsfile.write_text(
        dedent(f"""
    [test.example.com]
    client = spinta
    secret = verysecret
    scopes = {scope}
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


@pytest.mark.parametrize("scope", [["spinta_getall spinta_getone"], ["uapi:/:getall uapi:/:getone"]])
def test_add_client_credentials_kwargs(tmp_path: Path, scope: list):
    credsfile = Path(tmp_path / "credentials.cfg")

    add_client_credentials(credsfile, "https://example.com", client="spinta", secret="verysecret", scopes=scope)
    creds = configparser.ConfigParser()
    creds.read(credsfile)

    expected_scopes = "\n" + "\n".join(scope)
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
