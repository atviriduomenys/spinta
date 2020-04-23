from __future__ import annotations

import datetime
import enum
import json
import logging
import time

import ruamel.yaml

from starlette.responses import JSONResponse
from starlette.exceptions import HTTPException
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

from authlib.jose import jwk
from authlib.jose import jwt
from authlib.jose.errors import JoseError
from authlib.oauth2 import OAuth2Request
from authlib.oauth2 import rfc6749
from authlib.oauth2 import rfc6750
from authlib.oauth2.rfc6749 import grants
from authlib.oauth2.rfc6750.errors import InsufficientScopeError
from authlib.oauth2.rfc6749.errors import InvalidClientError

from spinta.components import Context
from spinta.exceptions import InvalidToken, NoTokenValidationKey
from spinta.utils import passwords
from spinta.utils.scopes import name_to_scope

log = logging.getLogger(__name__)
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
        self._private_key = load_key(context, KeyType.private, required=False)

    def enabled(self):
        return self._private_key is not None

    def create_oauth2_request(self, request):
        return get_auth_request(request)

    def handle_response(self, status_code, payload, headers):
        return JSONResponse(payload, status_code=status_code, headers=dict(headers))

    def send_signal(self, *args, **kwargs):
        pass

    def _query_client(self, client_id):
        return query_client(self._context, client_id)

    def _save_token(self, token, request):
        pass

    def _get_expires_in(self, client, grant_type):
        return int(datetime.timedelta(days=10).total_seconds())

    def _generate_token(self, client: Client, grant_type, user, scope, **kwargs):
        expires_in = self._get_expires_in(client, grant_type)
        scopes = scope.split() if scope else []
        return create_access_token(self._context, self._private_key, client.id, expires_in, scopes)


class ResourceProtector(rfc6749.ResourceProtector):

    def __init__(self, context: Context, Validator: type):
        self.TOKEN_VALIDATORS = {
            Validator.TOKEN_TYPE: Validator(context),
        }


class BearerTokenValidator(rfc6750.BearerTokenValidator):

    def __init__(self, context):
        super().__init__()
        self._context = context
        self._public_key = load_key(context, KeyType.public)

    def authenticate_token(self, token_string: str):
        return Token(token_string, self)

    def request_invalid(self, request):
        return False

    def token_revoked(self, token):
        return False


class Client(rfc6749.ClientMixin):

    def __init__(self, *, id: str, secret_hash: str, scopes: list):
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
        unknown_scopes = scopes - self.scopes
        if unknown_scopes:
            log.warning(f"requested unknown scopes: %s", ', '.join(sorted(unknown_scopes)))
            return False
        else:
            return True


class Token(rfc6749.TokenMixin):

    def __init__(self, token_string, validator: BearerTokenValidator):
        try:
            self._token = jwt.decode(token_string, validator._public_key)
        except JoseError as e:
            raise InvalidToken(error=str(e))

        self._validator = validator

    def valid_scope(self, scope, *, operator='AND'):
        if self._validator.scope_insufficient(self, scope, operator):
            return False
        else:
            return True

    def check_scope(self, scope, *, operator='AND'):
        if not self.valid_scope(scope, operator=operator):
            client_id = self._token['aud']

            if isinstance(scope, str):
                scope = [scope]

            missing_scopes = ', '.join(sorted(scope))
            log.error(f"client {client_id!r} is missing required scope: %s", missing_scopes)
            # FIXME: this should be wrapped into UserError.
            raise InsufficientScopeError(description=f"Missing scope: {missing_scopes}")

    def get_expires_at(self):
        return self._token['exp']

    def get_scope(self):
        return self._token.get('scope', '')

    def get_sub(self):
        return self._token.get('sub', '')


class AdminToken(rfc6749.TokenMixin):

    def valid_scope(self, scope, **kwargs):
        return True

    def check_scope(self, scope, **kwargs):
        pass

    def get_sub(self):
        return 'admin'


def get_auth_token(context: Context) -> Token:
    scope = None  # Scopes will be validated later using Token.check_scope
    request = context.get('auth.request')

    config = context.get('config')
    if config.default_auth_client and 'authorization' not in request.headers:
        token = create_client_access_token(context, config.default_auth_client)
        request.headers = request.headers.mutablecopy()
        request.headers['authorization'] = f'Bearer {token}'

    resource_protector = context.get('auth.resource_protector')
    try:
        token = resource_protector.validate_request(scope, request)
    except JoseError as e:
        raise HTTPException(status_code=400, detail=e.error)
    return token


def get_auth_request(request: dict) -> OAuth2Request:
    return OAuth2Request(
        request['method'],
        request['url'],
        request['body'],
        request['headers'],
    )


def create_key_pair():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())


class KeyType(enum.Enum):
    public = 'public'
    private = 'private'


def load_key(context: Context, key_type: KeyType, *, required: bool = True):
    key = None
    config = context.get('config')

    # Public key can be set via configuration.
    if key_type == KeyType.public:
        key = config.token_validation_key

    # Load key from a file.
    if key is None:
        keypath = config.config_path / 'keys' / f'{key_type.value}.json'
        if keypath.exists():
            with keypath.open() as f:
                key = json.load(f)

    if required and key is None:
        raise NoTokenValidationKey(key_type=key_type.value)

    if isinstance(key, dict) and 'keys' in key:
        # XXX: Maybe I should load all keys and then pick right one by algorithm
        #      used in token?
        keys = [k for k in key['keys'] if k['alg'] == 'RS512']
        key = keys[0]

    key = jwk.loads(key)
    return key


def create_client_access_token(context: Context, client: str):
    private_key = load_key(context, KeyType.private)
    client = query_client(context, client)
    expires_in = int(datetime.timedelta(days=10).total_seconds())
    return create_access_token(context, private_key, client.id, expires_in, client.scopes)


def create_access_token(
    context: Context,
    private_key,
    client: str,
    expires_in: int = None,
    scopes: list = None,
):
    config = context.get('config')

    if expires_in is None:
        expires_in = int(datetime.timedelta(minutes=10).total_seconds())

    header = {
        'typ': 'JWT',
        'alg': 'RS512',
    }

    iat = int(time.time())
    exp = iat + expires_in
    scopes = ' '.join(sorted(scopes)) if scopes else ''
    payload = {
        'iss': config.server_url,
        'sub': client,
        'aud': client,
        'iat': iat,
        'exp': exp,
        'scope': scopes,
    }
    return jwt.encode(header, payload, private_key).decode('ascii')


def query_client(context: Context, client: str):
    config = context.get('config')
    client_file = config.config_path / 'clients' / f'{client}.yml'
    try:
        data = yaml.load(client_file)
    except FileNotFoundError:
        raise (InvalidClientError(description='Invalid client id or secret'))

    if not isinstance(data['scopes'], list):
        raise Exception(f'Client {client_file} scopes must be list of scopes.')
    client = Client(
        id=client,
        secret_hash=data['client_secret_hash'],
        scopes=data['scopes'],
    )
    return client


def check_generated_scopes(context: Context,
                           name: str,
                           action: str,
                           prop: str = None,
                           prop_hidden: bool = False) -> None:
    config = context.get('config')
    token = context.get('auth.token')
    prefix = config.scope_prefix

    # If global scope is available, no need to check anything else
    global_scope = f'{prefix}{action}'
    if token.valid_scope(global_scope):
        return

    model_scope = name_to_scope(
        '{prefix}{name}_{action}',
        name,
        maxlen=config.scope_max_length,
        params={
            'prefix': prefix,
            'action': action,
        },
    )

    # if no property - then check for model scopes
    if prop is None:
        token.check_scope(model_scope)
    else:
        prop_scope = name_to_scope(
            '{prefix}{name}_{action}',
            prop,
            maxlen=config.scope_max_length,
            params={
                'prefix': prefix,
                'action': action,
            },
        )
        # if prop is hidden - explicit property scope is required,
        # otherwise either model or property scope can be used
        if prop_hidden:
            token.check_scope(prop_scope)
        else:
            token.check_scope([model_scope, prop_scope], operator='OR')


def check_scope(context: Context, scope: str):
    config = context.get('config')
    token = context.get('auth.token')
    token.check_scope(f'{config.scope_prefix}{scope}')
