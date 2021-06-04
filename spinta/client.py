import configparser
import pathlib
import urllib.parse
from typing import List
from typing import NamedTuple

import requests

from spinta.exceptions import RemoteClientCredentialsNotFound
from spinta.exceptions import RemoteClientCredentialsNotGiven
from spinta.exceptions import RemoteClientScopesNotGiven


class _ParsedUrl(NamedTuple):
    section: str
    client: str
    secret: str
    server: str


def _parse_url(url: str) -> _ParsedUrl:
    if url.startswith(('https://', 'https://')):
        url_ = url
    else:
        url_ = 'https://' + url

    purl = urllib.parse.urlparse(url_)
    section = purl.hostname
    if purl.port:
        section += f':{purl.port}'
    if purl.username:
        section = f'{purl.username}@{purl.hostname}'
    client = purl.username
    secret = purl.password
    server = f'{purl.scheme}://{purl.hostname}'
    return _ParsedUrl(section, client, secret, server)


def add_client_credentials(
    credsfile: pathlib.Path,
    server: str,
    *,
    client: str = None,
    secret: str = None,
    scopes: List[str] = None,
    section: str = None,
) -> None:
    creds = configparser.ConfigParser()
    if credsfile.exists():
        creds.read(credsfile)

    purl = _parse_url(server)
    scopes = scopes or []
    scopes = '\n' + '\n'.join(f'  {s}' for s in scopes)

    section = section or purl.section

    creds[section] = {
        'server': purl.server,
        'client': client or purl.client or '',
        'secret': secret or purl.secret or '',
        'scopes': scopes,

    }

    with credsfile.open('w') as f:
        creds.write(f)


def get_access_token(url: str, credsfile: pathlib.Path) -> str:
    purl: _ParsedUrl = _parse_url(url)

    if not credsfile.exists():
        raise RemoteClientCredentialsNotFound(
            url=url,
            section=purl.section,
            credentials=credsfile,
        )

    creds = configparser.ConfigParser()
    creds.read(credsfile)

    if not creds.has_section(purl.section):
        raise RemoteClientCredentialsNotFound(
            url=url,
            section=purl.section,
            credentials=credsfile,
        )

    server = creds.get(purl.section, 'server', fallback=purl.server)
    client = creds.get(purl.section, 'client', fallback=purl.client)
    secret = purl.secret or creds.get(purl.section, 'secret', fallback=None)

    if not client or not secret:
        raise RemoteClientCredentialsNotGiven(
            url=url,
            section=purl.section,
            credentials=credsfile,
        )

    scopes = creds.get(purl.section, 'scopes', fallback=None)
    if not scopes:
        raise RemoteClientScopesNotGiven(
            url=url,
            section=purl.section,
            credentials=credsfile,
        )

    auth = (client, secret)
    resp = requests.post(f'{server}/auth/token', auth=auth, data={
        'grant_type': 'client_credentials',
        'scope': scopes,
    })
    resp.raise_for_status()
    return resp.json()['access_token']
