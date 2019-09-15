import datetime

import pprintpp as pprint
import requests
import starlette.testclient

from spinta import auth
from spinta.testing.utils import get_model_scopes


class TestClient(starlette.testclient.TestClient):

    def __init__(self, context, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._spinta_context = context
        self._requests_session = None
        self._requests_session_base_url = None

    def start_session(self, base_url):
        self._requests_session = requests.Session()
        self._requests_session_base_url = base_url.rstrip('/')

    def authmodel(self, model, actions, creds=None):
        scopes = get_model_scopes(self._spinta_context, model, actions)
        self.authorize(scopes, creds=creds)

    def authorize(self, scopes=None, creds=None):
        if creds:
            # Request access token using /auth/token endpoint.
            resp = self.request('POST', '/auth/token', auth=creds, data={
                'grant_type': 'client_credentials',
                'scope': ' '.join(scopes),
            })
            assert resp.status_code == 200, resp.text
            token = resp.json()['access_token']

        else:
            # Create access token using private key.
            context = self._spinta_context
            context.load_if_not_loaded()
            private_key = auth.load_key(context, 'private.json')
            client_id = 'baa448a8-205c-4faa-a048-a10e4b32a136'
            client = auth.query_client(context, client_id)
            grant_type = 'client_credentials'
            expires_in = int(datetime.timedelta(days=10).total_seconds())
            token = auth.create_access_token(context, private_key, client, grant_type, expires_in, scopes=scopes)

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
            self._spinta_context.load_if_not_loaded()
            return super().request(method, url, *args, **kwargs)

    def getdata(self, *args, **kwargs):
        resp = self.get(*args, **kwargs)
        assert resp.status_code == 200, f'status_code: {resp.status_code}, response: {resp.text}'
        resp = resp.json()
        assert 'data' in resp, pprint.pformat(resp)
        return resp['data']
