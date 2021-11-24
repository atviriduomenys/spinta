from __future__ import annotations

from typing import Set
from typing import Type
from typing import Union, List, Tuple

import datetime
import enum
import json
import logging
import time
import pathlib
import base64

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
from authlib.oauth2 import OAuth2Error

from spinta.components import Config
from spinta.components import ScopeFormatterFunc
from spinta.core.enums import Access
from spinta.components import Context, Action, Namespace, Model, Property
from spinta.exceptions import InvalidToken, NoTokenValidationKey
from spinta.exceptions import AuthorizedClientsOnly
from spinta.exceptions import BasicAuthRequired
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

    def handle_response(self, status_code, payload, headers) -> JSONResponse:
        return JSONResponse(payload, status_code=status_code, headers=dict(headers))

    def handle_error_response(
        self,
        request: OAuth2Request,
        error: OAuth2Error,
    ) -> JSONResponse:
        log.exception('Authorization server error: %s', error)
        return super().handle_error_response(request, error)

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

    def __init__(
        self,
        context: Context,
        Validator: Type[rfc6750.BearerTokenValidator],
    ):
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
    id: str
    secret_hash: str
    scopes: Set[str]

    def __init__(self, *, id_: str, secret_hash: str, scopes: List[str]):
        self.id = id_
        self.secret_hash = secret_hash
        self.scopes = set(scopes)

    def __repr__(self):
        cls = type(self)
        return f'{cls.__module__}.{cls.__name__}(id={self.id!r})'

    def check_client_secret(self, client_secret):
        log.debug(f"Incorrect client {self.id!r} secret hash.")
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

            # FIXME: this should be wrapped into UserError.
            if operator == 'AND':
                log.error(f"client {client_id!r} is missing required scopes: %s", missing_scopes)
                raise InsufficientScopeError(description=f"Missing scopes: {missing_scopes}")
            elif operator == 'OR':
                log.error(f"client {client_id!r} is missing one of required scopes: %s", missing_scopes)
                raise InsufficientScopeError(description=f"Missing one of scopes: {missing_scopes}")
            else:
                raise Exception(f"Unknown operator {operator}.")

    def get_expires_at(self):
        return self._token['exp']

    def get_scope(self):
        return self._token.get('scope', '')

    def get_sub(self):  # User.
        return self._token.get('sub', '')

    def get_aud(self):  # Client.
        return self._token.get('aud', '')

    def get_client_id(self):
        return self.get_aud()


class AdminToken(rfc6749.TokenMixin):

    def valid_scope(self, scope, **kwargs):
        return True

    def check_scope(self, scope, **kwargs):
        pass

    def get_sub(self):  # User.
        return 'admin'

    def get_aud(self):  # Client.
        return 'admin'

    def get_client_id(self):
        return self.get_aud()


def get_auth_token(context: Context) -> Token:
    scope = None  # Scopes will be validated later using Token.check_scope
    request: OAuth2Request = context.get('auth.request')

    config = context.get('config')
    if config.default_auth_client and 'authorization' not in request.headers:
        token = create_client_access_token(context, config.default_auth_client)
        request.headers = request.headers.mutablecopy()
        request.headers['authorization'] = f'Bearer {token}'

    elif config.http_basic_auth:
        token = get_token_from_http_basic_auth(context, request)
        if token is not None:
            request.headers = request.headers.mutablecopy()
            request.headers['authorization'] = f'Bearer {token}'

    resource_protector = context.get('auth.resource_protector')
    try:
        token = resource_protector.validate_request(scope, request)
    except JoseError as e:
        raise HTTPException(status_code=400, detail=e.error)
    return token


def get_token_from_http_basic_auth(context: Context, request: OAuth2Request):
    if 'authorization' not in request.headers:
        raise BasicAuthRequired()

    auth = request.headers['authorization']
    if ' ' not in auth:
        raise BasicAuthRequired()

    method, value = request.headers['authorization'].split(None, 1)
    method = method.lower()
    if method != 'basic':
        # Pass authentication to authlib.
        return

    value = base64.b64decode(value).decode()
    client, secret = value.split(':', 1)

    # Get client.
    try:
        client = query_client(context, client)
    except InvalidClientError:
        raise BasicAuthRequired()

    # Check secret.
    if not client.check_client_secret(secret):
        raise BasicAuthRequired()

    return create_client_access_token(context, client)


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

    if key is None:
        if required:
            raise NoTokenValidationKey(key_type=key_type.value)
        else:
            return

    if isinstance(key, dict) and 'keys' in key:
        # XXX: Maybe I should load all keys and then pick right one by algorithm
        #      used in token?
        keys = [k for k in key['keys'] if k['alg'] == 'RS512']
        key = keys[0]

    key = jwk.loads(key)
    return key


def create_client_access_token(context: Context, client: Union[str, Client]):
    private_key = load_key(context, KeyType.private)
    if isinstance(client, str):
        client = query_client(context, client)
    expires_in = int(datetime.timedelta(days=10).total_seconds())
    return create_access_token(context, private_key, client.id, expires_in, client.scopes)


def create_access_token(
    context: Context,
    private_key,
    client: str,
    expires_in: int = None,
    scopes: Set[str] = None,
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


def query_client(context: Context, client: str) -> Client:
    config = context.get('config')
    client_file = config.config_path / 'clients' / f'{client}.yml'
    try:
        data = yaml.load(client_file)
    except FileNotFoundError:
        raise (InvalidClientError(description='Invalid client id or secret'))

    if not isinstance(data['scopes'], list):
        raise Exception(f'Client {client_file} scopes must be list of scopes.')
    client = Client(id_=client, secret_hash=data['client_secret_hash'],
                    scopes=data['scopes'])
    return client


def check_scope(context: Context, scope: str):
    config = context.get('config')
    token = context.get('auth.token')
    token.check_scope(f'{config.scope_prefix}{scope}')


def get_scope_name(
    context: Context,
    node: Union[Namespace, Model, Property],
    action: Action,
) -> str:
    config = context.get('config')

    if isinstance(node, Namespace):
        name = node.name
    elif isinstance(node, Model):
        name = node.model_type()
    elif isinstance(node, Property):
        name = node.model.model_type() + '_' + node.place
    else:
        raise Exception(f"Unknown node type {node}.")

    return name_to_scope(
        '{prefix}{name}_{action}' if name else '{prefix}{action}',
        name,
        maxlen=config.scope_max_length,
        params={
            'prefix': config.scope_prefix,
            'action': action.value,
        },
    )


def authorized(
    context: Context,
    node: Union[Namespace, Model, Property],
    action: Action,
    *,
    throw: bool = False,
    scope_formatter: ScopeFormatterFunc = None,
):
    config: Config = context.get('config')
    token = context.get('auth.token')

    # Unauthorized clients can only access open nodes.
    unauthorized = token.get_client_id() == config.default_auth_client

    open_node = node.access >= Access.open
    if unauthorized and not open_node:
        if throw:
            raise AuthorizedClientsOnly()
        else:
            return False

    # Private nodes can only be accessed with explicit node scope.
    scopes = [node]

    # Protected and higher level nodes can be accessed with parent nodes scopes.
    if node.access > Access.private:
        ns = None

        if isinstance(node, Property):
            # Hidden nodes also require explicit scope.
            # XXX: `hidden` parameter should only be used for API control, not
            #      access control. See docs.
            if not node.hidden:
                scopes.append(node.model)
                scopes.append(node.model.ns)
                ns = node.model.ns
        elif isinstance(node, Model):
            scopes.append(node.ns)
            ns = node.ns
        elif isinstance(node, Namespace):
            ns = node

        # Add all parent namespace scopes too.
        if ns:
            scopes.extend(ns.parents())

    # Build scope names.
    scope_formatter = scope_formatter or config.scope_formatter
    if not isinstance(action, (list, tuple)):
        action = [action]
    scopes = [
        scope_formatter(context, scope, act)
        for act in action
        for scope in scopes
    ]

    # Check if client has at least one of required scopes.
    if throw:
        token.check_scope(scopes, operator='OR')
    else:
        return token.valid_scope(scopes, operator='OR')


def auth_server_keys_exists(path: pathlib.Path):
    return (
        (path / 'keys/private.json').exists() and
        (path / 'keys/public.json').exists()
    )


def gen_auth_server_keys(
    path: pathlib.Path,
    *,
    overwrite: bool = False,
    exist_ok: bool = False,
) -> Tuple[pathlib.Path, pathlib.Path]:
    path = path / 'keys'
    path.mkdir(exist_ok=True)

    files = (
        path / 'private.json',
        path / 'public.json',
    )

    if overwrite:
        create = True
    else:
        create = False
        for file in files:
            if file.exists():
                if not exist_ok:
                    raise KeyFileExists(f"{file} file already exists.")
            else:
                create = True

    if create:
        key = create_key_pair()
        keys = (key, key.public_key())
        for k, file in zip(keys, files):
            with file.open('w') as f:
                json.dump(jwk.dumps(k), f, indent=4, ensure_ascii=False)

    return files


class KeyFileExists(Exception):
    pass


def client_exists(path: pathlib.Path, client: str) -> bool:
    client_file = path / f'{client}.yml'
    return client_file.exists()


def create_client_file(
    path: pathlib.Path,
    client: str,
    secret: str = None,
    scopes: List[str] = None,
    *,
    add_secret: bool = False,
) -> Tuple[pathlib.Path, dict]:
    client_file = path / f'{client}.yml'

    if client_file.exists():
        raise ClientExist(f"{client_file} file already exists.")

    secret = secret or passwords.gensecret(32)
    secret_hash = passwords.crypt(secret)

    data = write = {
        'client_id': client,
        'client_secret': secret,
        'client_secret_hash': secret_hash,
        'scopes': scopes or [],
    }

    if not add_secret:
        write = data.copy()
        del write['client_secret']

    yml = ruamel.yaml.YAML()
    yml.indent(mapping=2, sequence=4, offset=2)
    yml.width = 80
    yml.explicit_start = False
    yml.dump(write, client_file)

    return client_file, data


class ClientExist(Exception):
    pass
