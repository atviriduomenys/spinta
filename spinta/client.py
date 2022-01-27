import configparser
import dataclasses
import pathlib
import urllib.parse
from typing import List
from typing import Optional

import requests

from spinta.exceptions import RemoteClientCredentialsNotFound
from spinta.exceptions import RemoteClientCredentialsNotGiven
from spinta.exceptions import RemoteClientScopesNotGiven


@dataclasses.dataclass
class RemoteClientCredentials:
    section: str        # client section in credentials.cfg
    client: str         # client username
    secret: str         # client secret
    server: str         # server URL
    remote: str         # remote name given in credentials.cfg section
    scopes: List[str]   # list of client scopes


def _parse_client_handle(
    handle: str,  # can be server URL or section name in credentials.cfg
) -> RemoteClientCredentials:
    # XXX: Probably this should be dropped, because push should be only
    #      possible to another Spinta or compatible server.
    if handle.startswith('spinta+'):
        handle = handle[len('spinta+'):]

    if handle.startswith(('https://', 'http://')):
        url = handle
    else:
        url = 'https://' + handle

    purl = urllib.parse.urlparse(url)
    section = purl.hostname
    server = f'{purl.scheme}://{purl.hostname}'
    remote = purl.hostname.replace('.', '_').replace('-', '_')
    if purl.port:
        section += f':{purl.port}'
        server += f':{purl.port}'
        if purl.port != 80 and purl.port != 443:
            remote += f'_{purl.port}'
    if purl.username:
        section = f'{purl.username}@{purl.hostname}'
    return RemoteClientCredentials(
        section=section,
        client=purl.username,
        secret=purl.password,
        server=server,
        remote=remote,
        scopes=[],
    )


def get_client_credentials(
    credsfile: Optional[pathlib.Path],  # path to credentials.cfg
    name: str,                  # section name from credentials.cfg or a server
                                # URL
    *,
    check: bool = True,         # check if credentials.cfg exists, section
                                # exists if client username, password and
                                # scopes are given
) -> RemoteClientCredentials:
    creds = _parse_client_handle(name)

    if credsfile:
        if credsfile.exists():
            config = configparser.ConfigParser()
            config.read(credsfile)

            if config.has_section(creds.section):
                creds.server = config.get(creds.section, 'server', fallback=creds.server)
                creds.client = creds.client or config.get(creds.section, 'client', fallback=None)
                creds.secret = creds.secret or config.get(creds.section, 'secret', fallback=None)
                creds.scopes = config.get(creds.section, 'scopes', fallback=[])

            elif check:
                raise RemoteClientCredentialsNotFound(
                    name=name,
                    section=creds.section,
                    credentials=credsfile,
                )

        elif check:
            raise RemoteClientCredentialsNotFound(
                name=name,
                section=creds.section,
                credentials=credsfile,
            )

    if check:
        if not creds.client or not creds.secret:
            raise RemoteClientCredentialsNotGiven(
                name=name,
                section=creds.section,
                credentials=credsfile,
            )

        if not creds.scopes:
            raise RemoteClientScopesNotGiven(
                name=name,
                section=creds.section,
                credentials=credsfile,
            )

    return creds


def add_client_credentials(
    credsfile: pathlib.Path,
    server: str,
    *,
    client: str = None,
    secret: str = None,
    scopes: List[str] = None,
    section: str = None,
) -> None:
    creds = get_client_credentials(credsfile, server, check=False)

    scopes = scopes or creds.scopes
    scopes = '\n' + '\n'.join(f'  {s}' for s in scopes)

    section = section or creds.section

    config = configparser.ConfigParser()
    if credsfile.exists():
        config.read(credsfile)
    config[section] = {
        'server': creds.server,
        'client': client or creds.client or '',
        'secret': secret or creds.secret or '',
        'scopes': scopes,

    }

    with credsfile.open('w') as f:
        config.write(f)


def get_access_token(creds: RemoteClientCredentials) -> str:
    auth = (creds.client, creds.secret)
    resp = requests.post(f'{creds.server}/auth/token', auth=auth, data={
        'grant_type': 'client_credentials',
        'scope': creds.scopes,
    })
    resp.raise_for_status()
    return resp.json()['access_token']
