import configparser
from pathlib import Path
from textwrap import dedent

import pytest
from py._path.common import PathBase
from responses import POST
from responses import RequestsMock

from spinta.client import add_client_credentials
from spinta.client import get_access_token
from spinta.exceptions import RemoteClientCredentialsNotFound


@pytest.mark.parametrize('url', [
    'example',
    'example.com',
    'spinta@example.com',
    'https://spinta@example.com',
    'https://example.com',
])
def test_get_access_token(responses: RequestsMock, tmpdir: PathBase, url: str):
    credsfile = Path(tmpdir / 'credentials.cfg')
    credsfile.write_text(dedent('''
    [example]
    server = https://example.com
    client = spinta
    secret = verysecret
    scopes =
        spinta_getall
        spinta_getone
    
    [example.com]
    client = spinta
    secret = verysecret
    scopes =
        spinta_getall
        spinta_getone
        
    [spinta@example.com]
    client = spinta
    secret = verysecret
    scopes =
        spinta_getall
        spinta_getone
    '''))
    responses.add(POST, 'https://example.com/auth/token', json={
        'access_token': 'TOKEN',
    })
    token = get_access_token(url, credsfile)
    assert token == 'TOKEN'


def test_get_access_token_no_credsfile(tmpdir: PathBase):
    credsfile = Path(tmpdir / 'credentials.cfg')
    with pytest.raises(RemoteClientCredentialsNotFound):
        get_access_token('https://example.com', credsfile)


def test_get_access_token_no_section(tmpdir: PathBase):
    credsfile = Path(tmpdir / 'credentials.cfg')
    credsfile.write_text(dedent('''
    [test.example.com]
    client = spinta
    secret = verysecret
    scopes =
        spinta_getall
        spinta_getone
    '''))
    with pytest.raises(RemoteClientCredentialsNotFound):
        get_access_token('https://example.com', credsfile)


def test_add_client_credentials(tmpdir: PathBase):
    credsfile = Path(tmpdir / 'credentials.cfg')

    add_client_credentials(credsfile, 'example.com')
    add_client_credentials(credsfile, 'spinta@example.com')
    add_client_credentials(credsfile, 'spinta@example.com', section='example')

    creds = configparser.ConfigParser()
    creds.read(credsfile)

    assert dict(creds['example.com']) == {
        'server': 'https://example.com',
        'client': '',
        'secret': '',
        'scopes': '',
    }

    assert dict(creds['spinta@example.com']) == {
        'server': 'https://example.com',
        'client': 'spinta',
        'secret': '',
        'scopes': '',
    }

    assert dict(creds['example']) == {
        'server': 'https://example.com',
        'client': 'spinta',
        'secret': '',
        'scopes': '',
    }


def test_add_client_credentials_kwargs(tmpdir: PathBase):
    credsfile = Path(tmpdir / 'credentials.cfg')

    add_client_credentials(
        credsfile, 'https://example.com',
        client='spinta',
        secret='verysecret',
        scopes=[
            'spinta_getall',
            'spinta_getone',
        ],
    )

    creds = configparser.ConfigParser()
    creds.read(credsfile)

    assert dict(creds['example.com']) == {
        'server': 'https://example.com',
        'client': 'spinta',
        'secret': 'verysecret',
        'scopes': '\nspinta_getall\nspinta_getone',
    }
