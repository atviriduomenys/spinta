import datetime
import json
import time

import passwords
import ruamel.yaml

from starlette.responses import JSONResponse

from authlib.jose import jwk
from authlib.jose import jwt
from authlib.oauth2 import rfc6749
from authlib.oauth2 import OAuth2Request
from authlib.oauth2.rfc6749 import grants
from authlib.oauth2.rfc6750 import BearerToken

yaml = ruamel.yaml.YAML(typ='safe')


class AuthorizationServer(rfc6749.AuthorizationServer):

    def __init__(self, context):
        super().__init__(
            query_client=self._query_client,
            generate_token=BearerToken(
                access_token_generator=self._generate_token,
                expires_generator=self._get_expires_in,
            ),
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
        config = self._context.get('config')
        client_file = config.config_path / 'clients' / f'{client_id}.yml'
        data = yaml.load(client_file)
        client = Client(
            id=client_id,
            secret_hash=data['client_secret_hash'],
            scopes=data['scopes'],
        )
        return client

    def _save_token(self, token, request):
        pass

    def _get_expires_in(self, client, grant_type):
        return int(datetime.timedelta(days=10).total_seconds())

    def _generate_token(self, client, grant_type, user, scope, **kwargs):
        config = self._context.get('config')

        header = {
            'typ': 'JWT',
            'alg': 'RS512',
        }

        iat = int(time.time())
        exp = iat + self._get_expires_in(client, grant_type)
        payload = {
            'iss': config.server_url,
            'sub': client.id,
            'aud': client.id,
            'iat': iat,
            'exp': exp,
        }
        return jwt.encode(header, payload, self._private_key).decode('ascii')

    def _load_key(self, filename):
        config = self._context.get('config')
        with (config.config_path / 'keys' / filename).open() as f:
            key = json.load(f)
        key = jwk.loads(key)
        return key


class Client(rfc6749.ClientMixin):

    def __init__(self, *, id, secret_hash, scopes):
        self.id = id
        self.secret_hash = secret_hash
        self.scopes = set(scopes)

    def check_client_secret(self, client_secret):
        return passwords.verify(client_secret, self.secret_hash)

    def check_token_endpoint_auth_method(self, method: str):
        return method == 'client_secret_basic'

    def check_grant_type(self, grant_type: str):
        return grant_type == 'client_credentials'

    def check_requested_scopes(self, scopes: set):
        return self.scopes.issuperset(scopes)


class Token(rfc6749.TokenMixin):
    pass
