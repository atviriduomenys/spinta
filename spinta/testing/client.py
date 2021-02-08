from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Optional, List, Union

import dataclasses
import datetime
import pathlib
import re
import urllib.parse

import pprintpp as pprint
import requests
import starlette.testclient

from responses import RequestsMock
from responses import POST

from spinta import auth
from spinta import commands
from spinta import api
from spinta.core.config import RawConfig
from spinta.testing.context import TestContext
from spinta.testing.context import create_test_context
from spinta.auth import create_client_file
from spinta.testing.config import create_config_path


def create_test_client(
    rc_or_context: Union[RawConfig, TestContext],
    config: Dict[str, Any] = None,
    *,
    scope: List[str] = None,
) -> TestClient:
    if isinstance(rc_or_context, RawConfig):
        rc = rc_or_context
        if config:
            rc = rc.fork(config)
        context = create_test_context(rc, name='pytest/client')
    else:
        if config is not None:
            raise NotImplementedError()
        context = rc_or_context
    if context.loaded:
        if config is not None:
            raise RuntimeError("Context already loaded, can't override config.")
    else:
        context.load()
    app = api.init(context)
    client = TestClient(context, app, base_url='https://testserver')
    if scope:
        client.authorize(scope)
    return client


def create_remote_server(
    rc: RawConfig,
    tmpdir: pathlib.Path,
    responses: RequestsMock,
    *,
    url: str = 'https://example.com/',
    client: str = 'client',
    secret: str = 'secret',
    scopes: List[str] = None,
    credsfile: Union[bool, pathlib.Path] = None,
) -> RemoteServer:

    def remote(request):
        path = request.url[len(url.rstrip('/')):]
        resp = app.request(
            request.method,
            path,
            headers=request.headers,
            data=request.body,
        )
        return resp.status_code, resp.headers, resp.content

    confdir = create_config_path(tmpdir / 'config')
    rc = rc.fork({
        'config_path': confdir,
        'default_auth_client': None,
    })
    context = create_test_context(rc)
    app = create_test_client(context)

    if scopes:
        client_file, client = create_client_file(
            confdir / 'clients',
            client=client,
            secret=secret,
            scopes=scopes,
            add_secret=True,
        )
        secret = client['client_secret']
        client = client['client_id']

    if credsfile:
        if credsfile is True:
            credsfile = tmpdir / 'credentials.cfg'
        urlp = urllib.parse.urlparse(url)
        create_client_creentials_file(
            credsfile,
            client=client,
            secret=secret,
            server=urlp.netloc,
            scopes=scopes,
        )

    responses.add_callback(
        POST, re.compile(re.escape(url) + r'.*'),
        callback=remote,
        content_type='application/json',
    )

    return RemoteServer(
        app=app,
        url=url,
        client=client,
        secret=secret,
        credsfile=credsfile,
    )


@dataclasses.dataclass
class RemoteServer:
    app: TestClient
    url: str
    client: str = None
    secret: str = None
    credsfile: pathlib.Path = None


def create_client_creentials_file(
    path: pathlib.Path,
    # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
    client: str = '3388ea36-4a4f-4821-900a-b574c8829d52',
    secret: str = 'b5DVbOaEY1BGnfbfv82oA0-4XEBgLQuJ',
    server: str = 'example.com',
    scopes: list = None,
):
    scopes = scopes or []
    scopes = '\n' + '\n'.join(f'  {s}' for s in scopes)
    path.write_text(
        f'[{client}@{server}]\n'
        f'client_id = {client}\n'
        f'client_secret = {secret}\n'
        f'scopes = {scopes}\n'
    )


class TestClient(starlette.testclient.TestClient):

    def __init__(self, context, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = context
        self._requests_session = None
        self._requests_session_base_url = None
        self._scopes = []

    def start_session(self, base_url):
        self._requests_session = requests.Session()
        self._requests_session_base_url = base_url.rstrip('/')

    def authmodel(self, model: str, actions: List[str], creds=None):
        scopes = commands.get_model_scopes(self.context, model, actions)
        self.authorize(scopes, creds=creds)

    def authorize(self, scopes: Optional[list] = None, creds=None):
        # Calling `authorize` multiple times, will preserve previous scopes.
        self._scopes += [s for s in (scopes or []) if s not in self._scopes]

        if creds:
            # Request access token using /auth/token endpoint.
            resp = self.request('POST', '/auth/token', auth=creds, data={
                'grant_type': 'client_credentials',
                'scope': ' '.join(self._scopes),
            })
            assert resp.status_code == 200, resp.text
            token = resp.json()['access_token']
        else:
            # Create access token using private key.
            context = self.context
            private_key = auth.load_key(context, auth.KeyType.private)
            client = 'test-client'
            expires_in = int(datetime.timedelta(days=10).total_seconds())
            token = auth.create_access_token(context, private_key, client, expires_in, scopes=self._scopes)

        if self._requests_session:
            session = self._requests_session
        else:
            session = self

        session.headers.update({
            'Authorization': f'Bearer {token}'
        })

    def request(self, method: str, url: str, *args, **kwargs):
        if self._requests_session:
            url = self._requests_session_base_url + url
            return self._requests_session.request(method, url, *args, **kwargs)
        else:
            return super().request(method, url, *args, **kwargs)

    def getdata(self, *args, **kwargs):
        resp = self.get(*args, **kwargs)
        assert resp.status_code == 200, f'status_code: {resp.status_code}, response: {resp.text}'
        resp = resp.json()
        assert '_data' in resp, pprint.pformat(resp)
        return resp['_data']
