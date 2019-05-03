import datetime
import json
import time

import ruamel.yaml

from starlette.requests import Request
from starlette.responses import JSONResponse

from authlib.jose import jwk
from authlib.jose import jwt
from authlib.oauth2 import OAuth2Request
from authlib.oauth2 import rfc6749
from authlib.oauth2 import rfc6750
from authlib.oauth2.rfc6749 import grants
from authlib.oauth2.rfc6750.errors import InsufficientScopeError

from spinta.commands import authorize
from spinta.components import Context
from spinta.dispatcher import Command
from spinta.utils import passwords
from spinta.utils.scopes import name_to_scope
from spinta.components import Model
from spinta.backends import Backend

yaml = ruamel.yaml.YAML(typ='safe')


class AuthorizationServer(rfc6749.AuthorizationServer):

    def __init__(self, context):
        super().__init__(
            query_client=self._query_client,
            generate_token=rfc6750.BearerToken(
                access_token_generator=self._generate_token,
                expires_generator=self._get_expires_in,
            ),
            save_token=self._save_token,
        )
        self.register_grant(grants.ClientCredentialsGrant)
        self._context = context
        self._private_key = self._load_key('private.json')

    def create_oauth2_request(self, request):
        return get_auth_request(request)

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


class ResourceProtector(rfc6749.ResourceProtector):

    def __init__(self, context: Context, Validator: type):
        self.TOKEN_VALIDATORS = {
            Validator.TOKEN_TYPE: Validator(context),
        }


class BearerTokenValidator(rfc6750.BearerTokenValidator):

    def __init__(self, context):
        super().__init__()
        self._context = context
        self._private_key = self._load_key('private.json')

    def authenticate_token(self, token_string: str):
        return Token(self)

    def request_invalid(self, request):
        return False

    def token_revoked(self, token):
        return False


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

    def __init__(self, validator: BearerTokenValidator):
        self._validator = validator

    def check_scope(self, scope):
        if isinstance(scope, str):
            scope = {scope}
        if self.scope_insufficient(self, scope):
            raise InsufficientScopeError()


def get_auth_token(context: Context) -> Token:
    scope = None  # Scopes will be validated later using Token.check_scope
    request = context.get('auth.request')
    resource_protector = context.get('auth.resource_protector')
    token = resource_protector.validate_request(scope, request)
    return token


def get_auth_request(request: Request) -> OAuth2Request:
    return OAuth2Request(
        request['method'],
        request['url'],
        request['body'],
        request['headers'],
    )


@authorize.register()
def authorize(command: Command, context: Context, model: Model, backend: Backend, data: dict, **kwargs):
    return authorize(command, context, model)


@authorize.register()
def authorize(command: Command, context: Context, model: Model, *, _wrapped, **kwargs):
    config = context.get('config')
    token = context.get('auth.token')
    scope = name_to_scope('{prefix}{name}_{action}', model.name, maxlen=config.scope_max_length, params={
        'prefix': config.scope_prefix,
        'action': command.name,
    })
    token.check_scope(scope)
    wrapped()
