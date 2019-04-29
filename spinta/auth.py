import json

from starlette.responses import JSONResponse

from authlib.jose import jwk
from authlib.jose import jwt
from authlib.oauth2 import rfc6749
from authlib.oauth2 import OAuth2Request
from authlib.oauth2.rfc6749 import grants
from authlib.oauth2.rfc6750 import BearerToken


class AuthorizationServer(rfc6749.AuthorizationServer):

    def __init__(self, context):
        super().__init__(
            query_client=self._query_client,
            generate_token=BearerToken(self._generate_token),
            save_token=self._save_token,
        )
        self.register_grant(grants.ClientCredentialsGrant)
        self._context = context
        self._private_key = self._load_key('private.json')

    def create_oauth2_request(self, request):
        return OAuth2Request(request['method'], request['url'], request['body'], request['headers'])

    def handle_response(self, status_code, payload, headers):
        return JSONResponse(payload, status_code=status_code, headers=dict(headers))

    def send_signal(self, *args, **kwargs):
        pass

    def _query_client(self, client_id):
        return Client()

    def _save_token(self, token, request):
        pass

    def _generate_token(self, client, grant_type, user, scope, **kwargs):
        header = {'alg': 'RS256'}
        payload = {'iss': 'Authlib', 'sub': '123'}
        return jwt.encode(header, payload, self._private_key).decode('ascii')

    def _load_key(self, filename):
        config = self._context.get('config')
        with (config.keys_dir / filename).open() as f:
            key = json.load(f)
        key = jwk.loads(key)
        return key


class Client(rfc6749.ClientMixin):

    def check_client_secret(self, client_secret):
        return True

    def check_token_endpoint_auth_method(self, method: str):
        return method == 'client_secret_basic'

    def check_grant_type(self, grant_type: str):
        return grant_type == 'client_credentials'

    def check_requested_scopes(self, scopes: set):
        allowed = {
            'profile',
        }
        return allowed.issuperset(scopes)


class Token(rfc6749.TokenMixin):
    pass
