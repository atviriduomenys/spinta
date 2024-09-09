from __future__ import annotations

import dataclasses
import datetime
import pathlib
import re
import uuid
from typing import Any, Tuple, Callable
from typing import Dict
from typing import Optional, List, Union
from uuid import uuid4

import httpx
import lxml.html
import pprintpp as pprint
import requests
import starlette.testclient
from lxml.etree import _Element
from pytest import FixtureRequest
from requests import PreparedRequest
from responses import POST
from responses import RequestsMock, CallbackResponse, FalseBool

from spinta import api
from spinta import auth
from spinta import commands
from spinta.auth import create_client_file, get_clients_path, yaml, yml
from spinta.client import add_client_credentials
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.testing.config import create_config_path
from spinta.testing.context import TestContext
from spinta.testing.context import create_test_context
from spinta.testing.datasets import Sqlite


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
    tmp_path: pathlib.Path,
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

    confdir = create_config_path(tmp_path / 'config')
    rc = rc.fork({
        'config_path': confdir,
        'default_auth_client': None,
    })
    context = create_test_context(rc)
    app = create_test_client(context)

    if scopes:
        client_file, client = create_client_file(
            get_clients_path(confdir),
            client_id=str(uuid.uuid4()),
            name=client,
            secret=secret,
            scopes=scopes,
            add_secret=True,
        )
        secret = client['client_secret']
        client = client['client_name']

    if credsfile:
        if credsfile is True:
            credsfile = tmp_path / 'credentials.cfg'
        add_client_credentials(
            credsfile, url,
            client=client,
            secret=secret,
            scopes=scopes,
        )

    responses._registry.add(
        CustomCallbackResponse(
            url=re.compile(re.escape(url) + r'.*'),
            method=POST,
            callback=remote,
            content_type='application/json',
            match_querystring=FalseBool(),
            match=(),
        )
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

    # So pytest does not treat this as a test collection (since it starts with Test prefix)
    __test__ = False


class TestClient(starlette.testclient.TestClient):
    context: Union[Context, TestContext]

    def __init__(self, context, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = context
        self._scopes = []

    def authmodel(self, model: str, actions: List[str], creds=None):
        scopes = commands.get_model_scopes(self.context, model, actions)
        self.authorize(scopes, creds=creds)

    def authorize(self, scopes: Optional[list] = None, creds=None, strict_set: bool = False):
        # Calling `authorize` multiple times, will preserve previous scopes if strict_set is False.
        if strict_set:
            self._scopes = scopes if scopes is not None else []
        else:
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

    def unauthorize(self):
        self._scopes = []
        if 'Authorization' in self.headers:
            del self.headers['Authorization']

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


def configure_remote_server(
    cli,
    local_rc: RawConfig,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    responses,
    remove_source: bool = True
):
    invoke_props = [
        'copy',
        '--access', 'open',
        '-o', tmp_path / 'remote.csv',
        tmp_path / 'manifest.csv',
    ]
    if remove_source:
        invoke_props.append('--no-source')
    cli.invoke(local_rc, invoke_props)

    # Create remote server with PostgreSQL backend
    remote_rc = rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmp_path / 'remote.csv'),
                'backend': 'default',
                'mode': local_rc.get('manifests', 'default', 'mode')
            },
        },
        'backends': ['default'],
    })
    return create_remote_server(
        remote_rc,
        tmp_path,
        responses,
        scopes=[
            'spinta_set_meta_fields',
            'spinta_getone',
            'spinta_getall',
            'spinta_search',
            'spinta_insert',
            'spinta_patch',
            'spinta_delete',
            'spinta_changes'
        ],
        credsfile=True,
    )


def create_rc(rc: RawConfig, tmp_path: pathlib.Path, db: Sqlite, mode: str = 'internal') -> RawConfig:
    return rc.fork({
        'manifests': {
            'default': {
                'type': 'tabular',
                'path': str(tmp_path / 'manifest.csv'),
                'backend': 'sql',
                'keymap': 'default',
                'mode': mode
            },
        },
        'backends': {
            'sql': {
                'type': 'sql',
                'dsn': db.dsn,
            },
        },
        # tests/config/clients/3388ea36-4a4f-4821-900a-b574c8829d52.yml
        'default_auth_client': '3388ea36-4a4f-4821-900a-b574c8829d52',
    })


def create_client(rc: RawConfig, tmp_path: pathlib.Path, geodb: Sqlite):
    rc = create_rc(rc, tmp_path, geodb)
    context = create_test_context(rc)
    return create_test_client(context)


class CustomCallbackResponse(CallbackResponse):
    def __init__(
        self,
        method: str,
        url: Union[re.Pattern[str], str],
        callback: Callable[[Any], Any],
        stream: Optional[bool] = None,
        content_type: Optional[str] = "text/plain",
        **kwargs: Any,
    ) -> None:
        super().__init__(method, url, callback, stream, content_type, **kwargs)

    def matches(self, request: "PreparedRequest") -> Tuple[bool, str]:
        self.method = request.method

        if not self._url_matches(self.url, str(request.url)):
            return False, "URL does not match"

        valid, reason = self._req_attr_matches(self.match, request)
        if not valid:
            return False, reason

        return True, ""


def get_yaml_data(path: pathlib.Path) -> dict:
    data = {}
    if path.exists():
        data = yaml.load(path)
    return data


def create_old_client_file(client_path: pathlib.Path, data: dict, file_name: str = None):
    if file_name is None:
        file_name = data.get('client_id', str(uuid4()))

    yml.dump(data, client_path / f'{file_name}.yml')

