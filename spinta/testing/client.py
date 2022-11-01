from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Optional, List, Union

import dataclasses
import datetime
import pathlib
import re

import lxml.html
import pprintpp as pprint
import requests
import httpx
import starlette.testclient
from pytest import FixtureRequest
from lxml.etree import _Element
from requests import PreparedRequest

from responses import RequestsMock
from responses import POST

from spinta import auth
from spinta import commands
from spinta import api
from spinta.client import add_client_credentials
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.testing.context import TestContext
from spinta.testing.context import create_test_context
from spinta.auth import create_client_file
from spinta.testing.config import create_config_path


def create_test_client(
    rc_or_context: Union[RawConfig, TestContext],
    request: FixtureRequest = None,
    *,
    config: Dict[str, Any] = None,
    scope: List[str] = None,
    raise_server_exceptions: bool = True,
) -> TestClient:
    if isinstance(rc_or_context, RawConfig):
        rc = rc_or_context
        if config:
            rc = rc.fork(config)
        context = create_test_context(rc, request, name='pytest/client')
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
    client = TestClient(
        context,
        app,
        base_url='https://testserver',
        raise_server_exceptions=raise_server_exceptions,
    )
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

    def remote(request: PreparedRequest):
        path = request.url[len(url.rstrip('/')):]
        resp = app.request(
            request.method,
            path,
            headers=dict(request.headers),
            content=request.body,
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
        add_client_credentials(
            credsfile, url,
            client=client,
            secret=secret,
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


class TestClientResponse(httpx.Response):
    template: str
    context: Dict[str, Any]


class TestClient(starlette.testclient.TestClient):
    context: Union[Context, TestContext]

    def __init__(self, context, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = context
        self._scopes = []

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

        self.headers.update({
            'Authorization': f'Bearer {token}'
        })

    def getdata(self, *args, **kwargs):
        resp = self.get(*args, **kwargs)
        assert resp.status_code == 200, f'status_code: {resp.status_code}, response: {resp.text}'
        resp = resp.json()
        assert '_data' in resp, pprint.pformat(resp)
        return resp['_data']


def get_html_tree(resp: requests.Response) -> Union[
    _Element,
    lxml.html.HtmlMixin,
]:
    return lxml.html.fromstring(resp.text)
