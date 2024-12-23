from __future__ import annotations

import base64
import datetime
import enum
import json
import logging
import os
import pathlib
import time
import uuid
from threading import Lock
from typing import Set
from typing import Type
from typing import Union, List, Tuple

import ruamel.yaml
from authlib.jose import jwk
from authlib.jose import jwt
from authlib.jose.errors import JoseError
from authlib.oauth2 import OAuth2Error
from authlib.oauth2 import OAuth2Request
from authlib.oauth2 import rfc6749
from authlib.oauth2 import rfc6750
from authlib.oauth2.rfc6749 import grants
from authlib.oauth2.rfc6749.errors import InvalidClientError, UnsupportedTokenTypeError
from authlib.oauth2.rfc6750.errors import InsufficientScopeError
from cachetools import cached, LRUCache
from cachetools.keys import hashkey
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from multipledispatch import dispatch
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse

from spinta.components import Config
from spinta.components import Context, Action, Namespace, Model, Property
from spinta.components import ScopeFormatterFunc
from spinta.core.enums import Access
from spinta.exceptions import AuthorizedClientsOnly
from spinta.exceptions import BasicAuthRequired
from spinta.exceptions import InvalidToken, NoTokenValidationKey, ClientWithNameAlreadyExists, ClientAlreadyExists, \
    ClientsKeymapNotFound, ClientsIdFolderNotFound, InvalidClientsKeymapStructure, InvalidScopes, \
    InvalidClientFileFormat
from spinta.utils import passwords
from spinta.utils.config import get_clients_path, get_keymap_path, get_id_path, get_helpers_path
from spinta.utils.scopes import name_to_scope
from spinta.utils.types import is_str_uuid

log = logging.getLogger(__name__)
yaml = ruamel.yaml.YAML(typ="safe")

yml = ruamel.yaml.YAML()
yml.indent(mapping=2, sequence=4, offset=2)
yml.width = 80
yml.explicit_start = False

# Cache limits
CLIENT_FILE_CACHE_SIZE_LIMIT = 1000
KEYMAP_CACHE_SIZE_LIMIT = 1
DEFAULT_CLIENT_ID_CACHE_SIZE_LIMIT = 1


class KeyType(enum.Enum):
    public = 'public'
    private = 'private'


class Scopes(enum.Enum):
    """
    These are special scopes, that are not created from `Action` enum using authorize function.
    """

    # Grants access to manipulate client files through API
    AUTH_CLIENTS = "auth_clients"

    # Grants access to generate inspect files through API
    INSPECT = "inspect"

    # Grants access to manipulate backend schema through API
    SCHEMA_WRITE = "schema_write"

    # Grants access to change meta fields (like _id) through request
    SET_META_FIELDS = "set_meta_fields"

    def __str__(self):
        return self.value


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

    def _query_client(self, client_name):
        path = get_clients_path(self._context.get('config'))
        return query_client(path, client_name, is_name=True)

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
    name: str
    secret_hash: str
    scopes: Set[str]

    def __init__(self, *, id_: str, name_: str, secret_hash: str, scopes: List[str]):
        self.id = id_
        self.name = name_
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
            unknown_scopes = ', '.join(sorted(unknown_scopes))
            raise InvalidScopes(scopes=unknown_scopes)
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

    def get_jti(self):
        return self._token.get('jti', '')

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

    def get_jti(self):
        return 'admin'

    def get_client_id(self):
        return self.get_aud()


def authenticate_token(protector: ResourceProtector, token: str, type_: str) -> Token:
    type_ = type_.lower()
    if type_ not in protector.TOKEN_VALIDATORS:
        raise UnsupportedTokenTypeError()

    return protector.TOKEN_VALIDATORS[type_].authenticate_token(token)


def get_auth_token(context: Context) -> Token:
    scope = None  # Scopes will be validated later using Token.check_scope
    request: OAuth2Request = context.get('auth.request')

    config = context.get('config')
    if config.default_auth_client and 'authorization' not in request.headers:
        default_id = get_default_auth_client_id(context)
        if default_id:
            token = create_client_access_token(context, default_id)
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
        config = context.get('config')
        client = query_client(get_clients_path(config), client, is_name=True)
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
        client = query_client(get_clients_path(context.get('config')), client)
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
    jti = str(uuid.uuid4())
    payload = {
        'iss': config.server_url,
        'sub': client,
        'aud': client,
        'iat': iat,
        'exp': exp,
        'scope': scopes,
        'jti': jti
    }
    return jwt.encode(header, payload, private_key).decode('ascii')


def get_client_file_path(
    path: pathlib.Path,
    client: str
) -> pathlib.Path:
    is_uuid = is_str_uuid(client)
    client_file = path / 'unknown' / f'{client}.yml'
    if is_uuid:
        client_file = get_id_path(path) / client[:2] / client[2:4] / f'{client[4:]}.yml'
    return client_file


def check_scope(context: Context, scope: Union[Scopes, str]):
    config = context.get('config')
    token = context.get('auth.token')

    if isinstance(scope, Scopes):
        scope = scope.value

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


def get_clients_list(path: pathlib.Path) -> list:
    id_path = get_id_path(path)
    validate_id_path(id_path)

    ids = []

    id_items = os.listdir(id_path)
    for id0 in id_items:
        if len(id0) != 2:
            continue

        id0_items = os.listdir(id_path / id0)
        for id1 in id0_items:
            if len(id1) != 2:
                continue

            id1_items = os.listdir(id_path / id0 / id1)
            for uuid_item in id1_items:
                if uuid_item.endswith('.yml') and len(uuid_item) == 36:
                    ids.append(f'{id0}{id1}{uuid_item[:-4]}')
    return ids


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
    unauthorized = token.get_client_id() == get_default_auth_client_id(context)
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
    client_file = get_client_file_path(path, client)
    if client_file.exists():
        return True
    return False


@dispatch(pathlib.Path, str)
def client_name_exists(path: pathlib.Path, client_name: str) -> bool:
    keymap_path = get_keymap_path(path)
    validate_keymap_path(keymap_path)

    keymap = _load_keymap_data(keymap_path)
    return client_name_exists(keymap, client_name)


@dispatch(dict, str)
def client_name_exists(keymap: dict, client_name: str) -> bool:
    if client_name in keymap.keys():
        return True
    return False


def create_client_file(
    path: pathlib.Path,
    name: str,
    client_id: str,
    secret: str = None,
    scopes: List[str] = None,
    *,
    add_secret: bool = False,
) -> Tuple[pathlib.Path, dict]:
    client_file = get_client_file_path(path, client_id)
    if client_file.exists():
        raise ClientAlreadyExists(client_id=client_file)

    keymap_path = get_keymap_path(path)
    validate_keymap_path(keymap_path)

    id_path = get_id_path(path)
    validate_id_path(id_path)

    keymap = _load_keymap_data(keymap_path)

    if client_name_exists(keymap, name):
        raise ClientWithNameAlreadyExists(client_name=name)

    os.makedirs(id_path / client_id[:2] / client_id[2:4], exist_ok=True)

    secret = secret or passwords.gensecret(32)
    secret_hash = passwords.crypt(secret)

    data = write = {
        'client_id': client_id,
        'client_name': name,
        'client_secret': secret,
        'client_secret_hash': secret_hash,
        'scopes': scopes or [],
    }
    keymap[name] = client_id

    if not add_secret:
        write = data.copy()
        del write['client_secret']
    yml.dump(write, client_file)
    yml.dump(keymap, keymap_path)

    return client_file, data


def delete_client_file(path: pathlib.Path, client_id: str):
    if client_exists(path, client_id):
        keymap_path = get_keymap_path(path)
        validate_keymap_path(keymap_path)

        remove_path = get_client_file_path(path, client_id)
        keymap = _load_keymap_data(keymap_path)
        changed = False
        keymap_values = list(keymap.values())
        keymap_keys = list(keymap.keys())
        if client_id in keymap_values:
            del keymap[keymap_keys[keymap_values.index(client_id)]]
            changed = True

        if changed:
            yml.dump(keymap, keymap_path)
        os.remove(remove_path)
        try:
            level_1 = remove_path.parent
            level_2 = remove_path.parent.parent
            level_1.rmdir()
            level_2.rmdir()
        except Exception:
            '''
                Remove only empty folders
            '''
    else:
        raise (InvalidClientError(description='Invalid client id or secret'))


def update_client_file(
    context: Context,
    path: pathlib.Path,
    client_id: str,
    name: str,
    secret: str,
    scopes: list
):
    if client_exists(path, client_id):
        config = context.get("config")
        client = query_client(get_clients_path(config), client_id)
        keymap_path = get_keymap_path(path)
        validate_keymap_path(keymap_path)

        new_name = name if name else client.name
        new_secret_hash = passwords.crypt(secret) if secret else client.secret_hash
        new_scopes = scopes if scopes is not None else client.scopes

        client_path = get_client_file_path(path, client_id)
        keymap = _load_keymap_data(keymap_path)
        if new_name != client.name:
            if client_name_exists(keymap, new_name):
                raise ClientWithNameAlreadyExists(client_name=new_name)

        new_data = {
            "client_id": client.id,
            "client_name": new_name,
            "client_secret_hash": new_secret_hash,
            "scopes": list(new_scopes)
        }

        yml.dump(new_data, client_path)
        if keymap:
            changed = False
            # Check if client changed name
            if client.name != new_name and client.name in keymap.keys():
                del keymap[client.name]
                keymap[new_name] = client.id
                changed = True

            if changed:
                yml.dump(keymap, keymap_path)
        return new_data
    else:
        raise (InvalidClientError(description='Invalid client id or secret'))


def get_client_id_from_name(
    path: pathlib.Path,
    client_name: str
):
    keymap_path = get_keymap_path(path)
    validate_keymap_path(keymap_path)

    keymap = _load_keymap_data(keymap_path)
    if client_name in keymap.keys():
        return keymap[client_name]
    return None


def validate_keymap_path(keymap_path: pathlib.Path):
    if not keymap_path.exists():
        raise ClientsKeymapNotFound()


def validate_id_path(id_path: pathlib.Path):
    if not id_path.exists():
        raise ClientsIdFolderNotFound()


def ensure_client_folders_exist(clients_path: pathlib.Path):
    # Ensure clients folder exist
    clients_path.mkdir(parents=True, exist_ok=True)

    # Ensure clients/helpers directory
    helpers_path = get_helpers_path(clients_path)
    helpers_path.mkdir(parents=True, exist_ok=True)

    # Ensure clients/helpers/keymap.yml exists
    keymap_path = get_keymap_path(clients_path)
    keymap_path.touch(exist_ok=True)

    # Ensure clients/id directory
    id_path = get_id_path(clients_path)
    id_path.mkdir(parents=True, exist_ok=True)


def _keymap_file_cache_key(path: pathlib.Path, *args, **kwargs):
    """
    Creates keymap file cache key using
    keymap path and keymap file update time.
    """
    key = hashkey(path, *args, **kwargs)
    time_ = os.path.getmtime(path)
    key += tuple([time_])
    return key


def _default_client_id_cache_key(context: Context, *args, **kwargs):
    """
    Creates default client id cache key using
    client folder path, default client name and keymap update time.
    """
    key = hashkey(*args, **kwargs)
    config: Config = context.get('config')
    path = get_clients_path(config.config_path)
    client = config.default_auth_client
    keymap_path = get_keymap_path(path)
    validate_keymap_path(keymap_path)
    time_ = os.path.getmtime(keymap_path)
    key += tuple([path, client, time_])
    return key


def _client_file_cache_key(path: pathlib.Path, client: str, *args, is_name: bool = False, **kwargs):
    """
    Creates client file cache key using
    client folder path, client id and client file update time.
    """
    id_path = get_id_path(path)
    validate_id_path(id_path)

    key = hashkey(path, client, *args, **kwargs)
    if is_name:
        client_id = get_client_id_from_name(path, client)
        if client_id is None:
            raise (InvalidClientError(description='Invalid client name'))

        client = client_id

    client_file = get_client_file_path(path, client)
    if not client_file.exists():
        raise (InvalidClientError(description='Invalid client id or secret'))

    time_ = os.path.getmtime(client_file)
    key += tuple([time_])
    return key


@cached(LRUCache(KEYMAP_CACHE_SIZE_LIMIT), key=_keymap_file_cache_key)
def _load_keymap_data(keymap_path: pathlib.Path) -> dict:
    keymap = yaml.load(keymap_path)
    # This could mean keymap is empty, or keymap has bad yml structure
    if keymap is None:
        if os.stat(keymap_path).st_size == 0:
            return {}
        raise InvalidClientsKeymapStructure()
    if not isinstance(keymap, dict):
        raise InvalidClientsKeymapStructure()
    return keymap


@cached(LRUCache(DEFAULT_CLIENT_ID_CACHE_SIZE_LIMIT), key=_default_client_id_cache_key)
def get_default_auth_client_id(context: Context) -> str:
    config: Config = context.get('config')
    return get_client_id_from_name(get_clients_path(config.config_path), config.default_auth_client)


@cached(LRUCache(CLIENT_FILE_CACHE_SIZE_LIMIT), key=_client_file_cache_key, lock=Lock())
def query_client(path: pathlib.Path, client: str, is_name: bool = False) -> Client:
    if is_name:
        client_id = get_client_id_from_name(path, client)
        if client_id is None:
            raise (InvalidClientError(description='Invalid client name'))

        client = client_id
    client_file = get_client_file_path(path, client)

    id_path = get_id_path(path)
    validate_id_path(id_path)

    try:
        data = yaml.load(client_file)
    except FileNotFoundError:
        raise (InvalidClientError(description='Invalid client id or secret'))
    if not isinstance(data, dict):
        raise InvalidClientFileFormat(client_file=client_file.name, client_file_type=type(data))
    if not isinstance(data['scopes'], list):
        raise Exception(f'Client {client_file} scopes must be list of scopes.')
    client_id = data["client_id"]
    client_name = data["client_name"] if ("client_name" in data.keys() and data["client_name"]) else None
    client = Client(id_=client_id, name_=client_name, secret_hash=data['client_secret_hash'],
                    scopes=data['scopes'])
    return client

