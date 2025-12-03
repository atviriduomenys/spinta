from __future__ import annotations

import base64
import dataclasses
import datetime
import enum
import json
import logging
import os
import pathlib
import time
import uuid
from collections import defaultdict
from functools import cached_property
from threading import Lock
from typing import Set, Any, TypedDict, Literal
from typing import Type
from typing import Union, List, Tuple

import requests
import ruamel.yaml
from authlib.jose import JsonWebKey, RSAKey, JWTClaims
from authlib.jose import jwt
from authlib.jose.errors import JoseError, DecodeError, InvalidTokenError, BadSignatureError
from authlib.oauth2 import OAuth2Error
from authlib.oauth2 import OAuth2Request
from authlib.oauth2 import rfc6749
from authlib.oauth2 import rfc6750
from authlib.oauth2.rfc6749 import grants, OAuth2Payload, scope_to_list, list_to_scope
from authlib.oauth2.rfc6749.errors import InvalidClientError
from authlib.oauth2.rfc6750.errors import InsufficientScopeError
from authlib.oauth2.rfc6749.util import scope_to_list
from cachetools import cached, LRUCache
from cachetools.keys import hashkey
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from multipledispatch import dispatch
from requests import RequestException
from starlette.datastructures import FormData, QueryParams, Headers
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse

from spinta.components import Config
from spinta.components import Context, Namespace, Model, Property
from spinta.components import ScopeFormatterFunc
from spinta.core.enums import Access, Action
from spinta.exceptions import AuthorizedClientsOnly
from spinta.exceptions import BasicAuthRequired
from spinta.exceptions import (
    InvalidToken,
    NoTokenValidationKey,
    ClientWithNameAlreadyExists,
    ClientAlreadyExists,
    ClientsKeymapNotFound,
    ClientsIdFolderNotFound,
    InvalidClientsKeymapStructure,
    InvalidScopes,
    InvalidClientFileFormat,
)
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

# File permission constants for sensitive authentication files
OWNER_READABLE_FILE = 0o600  # rw------- (owner read/write only)
OWNER_READABLE_DIR = 0o700  # rwx------ (owner read/write/execute only)
WORLD_READABLE_FILE = 0o644  # rw-r--r-- (owner read/write, others read)

# Cache limits
CLIENT_FILE_CACHE_SIZE_LIMIT = 1000
KEYMAP_CACHE_SIZE_LIMIT = 1
DEFAULT_CLIENT_ID_CACHE_SIZE_LIMIT = 1
DEFAULT_CREDENTIALS_SECTION = "default"
DEPRECATED_SCOPE_PREFIX = "spinta_"

# Scope types taken from authlib.oauth2.rfc6749.util.scope_to_list
SCOPE_TYPE = Union[tuple, list, set, str, None]
Kid = str  # key id


class JWK(TypedDict):
    kid: str
    kty: str
    alg: str
    use: str
    n: str
    e: str


class JWKS(TypedDict):
    keys: List[JWK]


class KeyType(enum.Enum):
    public = "public"
    private = "private"


class Scopes(enum.Enum):
    """
    These are special scopes, that are not created from `Action` enum using authorize function.
    """

    # Grants access to manipulate client files through API
    AUTH_CLIENTS = "auth_clients"

    # Grants access to change its own client file backends
    CLIENT_BACKENDS_UPDATE_SELF = "client_backends_update_self"

    # Grants access to generate inspect files through API
    INSPECT = "inspect"

    # Grants access to manipulate backend schema through API
    SCHEMA_WRITE = "schema_write"

    # Grants access to change meta fields (like _id) through request
    SET_META_FIELDS = "set_meta_fields"

    def __str__(self) -> str:
        return self.value


class AuthorizationServer(rfc6749.AuthorizationServer):
    def __init__(self, context):
        super().__init__()
        self.register_grant(grants.ClientCredentialsGrant)
        self.register_token_generator(
            "default",
            rfc6750.BearerToken(
                access_token_generator=self._generate_token,
                expires_generator=self._get_expires_in,
            ),
        )
        self._context = context
        self._private_key = load_key(context, KeyType.private, required=False)

    def enabled(self) -> bool:
        return self._private_key is not None

    def create_oauth2_request(self, request: StarletteOAuth2Data) -> OAuth2Request:
        return get_auth_request(request)

    def handle_response(self, status_code: int, payload: Any, headers: Any) -> JSONResponse:
        return JSONResponse(payload, status_code=status_code, headers=dict(headers))

    def handle_error_response(
        self,
        request: OAuth2Request,
        error: OAuth2Error,
    ) -> JSONResponse:
        log.exception("Authorization server error: %s", error)
        return super().handle_error_response(request, error)

    def send_signal(self, *args, **kwargs):
        pass

    def query_client(self, client_name: str) -> Client:
        path = get_clients_path(self._context.get("config"))
        return query_client(path, client_name, is_name=True)

    def save_token(self, token, request):
        pass

    def _get_expires_in(self, client: Client, grant_type: str) -> int:
        return int(datetime.timedelta(days=10).total_seconds())

    def _generate_token(self, grant_type: str, client: Client, user: str, scope: str, **kwargs) -> str:
        expires_in = self._get_expires_in(client, grant_type)
        scopes = set(scope.split()) if scope else set()
        return create_access_token(self._context, self._private_key, client.id, expires_in, scopes)


class ResourceProtector(rfc6749.ResourceProtector):
    def __init__(
        self,
        context: Context,
        Validator: Type[rfc6750.BearerTokenValidator],
    ):
        super().__init__()
        self.register_token_validator(Validator(context))


def load_all_public_keys(context: Context) -> list[RSAKey]:
    config = context.get("config")
    token_validation_key = config.token_validation_key
    token_validation_keys_download_url = config.token_validation_keys_download_url

    if isinstance(token_validation_key, dict) and token_validation_key:
        local_public_keys: list[RSAKey] = []
        if "keys" in token_validation_key:
            for key in token_validation_key["keys"]:
                local_public_keys.append(JsonWebKey.import_key(key))
        else:
            local_public_keys = [JsonWebKey.import_key(token_validation_key)]
        return local_public_keys
    elif token_validation_keys_download_url:
        return load_downloaded_public_keys(context)
    else:
        return [load_key(context, KeyType.public)]


def load_downloaded_public_keys(context: Context) -> list[RSAKey]:
    config = context.get("config")
    if not config.downloaded_public_keys_file:
        log.error("config.downloaded_public_keys_file is not set")
        return []
    if not config.downloaded_public_keys_file.exists():
        log.error(f"File {config.downloaded_public_keys_file} does not exist")
        return []

    with config.downloaded_public_keys_file.open() as f:
        return [JsonWebKey.import_key(key) for key in json.load(f)["keys"]]


def download_and_store_public_keys(context: Context) -> JWKS | None:
    config = context.get("config")
    if not config.token_validation_keys_download_url:
        return None
    log.info("Downloading public keys from %s", config.token_validation_keys_download_url)
    try:
        response = requests.get(config.token_validation_keys_download_url)
    except RequestException as e:
        log.exception(
            f"Failed to download public keys from {config.token_validation_keys_download_url}. Exception: {e}"
        )
        return None
    if not response.ok or "keys" not in response.json():
        log.error(
            f"Failed to download public keys from {config.token_validation_keys_download_url}. Response: {response.text}"
        )
        return None

    jwks: JWKS = response.json()

    if not os.path.exists(config.downloaded_public_keys_file):
        log.warning(f"Warning: {config.downloaded_public_keys_file=} does not exist. Creating it now.")
        os.makedirs(os.path.dirname(config.downloaded_public_keys_file), exist_ok=True)
        with open(config.downloaded_public_keys_file, "x") as f:
            json.dump({}, f)

    with open(config.downloaded_public_keys_file, "w") as f:
        json.dump(jwks, f, indent=4)
        log.info(f"Successfully downloaded public keys ({jwks}) from {config.downloaded_public_keys_file=}")

    return jwks


class BearerTokenValidator(rfc6750.BearerTokenValidator):
    def __init__(self, context: Context):
        super().__init__()
        self._context = context
        self._default_public_key: RSAKey = load_key(context, KeyType.public)
        self._all_public_keys: list[RSAKey] = load_all_public_keys(context)

    def decode_token(self, token_string: str) -> JWTClaims:
        if not token_string:
            raise InvalidToken("Token string is required")

        try:
            token_header = decode_unverified_header(token_string)
            if kid := token_header.get("key"):
                for key in self._all_public_keys:
                    if key.kid and str(key.kid) == str(kid):
                        return jwt.decode(token_string, key)

            token_kty = decode_kty_from_alg(token_header["alg"])
            for key in self._all_public_keys:
                is_not_encryption_key = key.tokens.get("use") != "enc"
                key_algorithm = key.tokens.get("alg")
                is_same_algorithm = key_algorithm and token_header["alg"] == key_algorithm
                is_same_algorithm_type = key.kty and key.kty == token_kty
                if is_not_encryption_key and (is_same_algorithm or is_same_algorithm_type):
                    try:
                        return jwt.decode(token_string, key)
                    except BadSignatureError:
                        continue
        except (JoseError, DecodeError, InvalidTokenError) as e:
            raise InvalidToken(error=str(e))
        raise InvalidToken(f"No public key found for token {token_header=}")

    def authenticate_token(self, token_string: str) -> Token:
        return Token(token_string, self)


class Client(rfc6749.ClientMixin):
    id: str
    name: str
    secret_hash: str
    scopes: Set[str]
    backends: dict[str, dict[str, Any]]

    def __init__(
        self,
        *,
        id_: str,
        name_: str,
        secret_hash: str,
        scopes: list[str],
        backends: dict[str, dict[str, Any]],
    ) -> None:
        self.id = id_
        self.name = name_
        self.secret_hash = secret_hash
        self.scopes = set(scopes)
        self.backends = backends

        # Auth method used for token endpoint.
        # More info: token_endpoint_auth_method https://datatracker.ietf.org/doc/html/rfc7591#autoid-5
        self.token_endpoint_auth_method = "client_secret_basic"

        # Allowed grant types.
        # More info: grant_types https://datatracker.ietf.org/doc/html/rfc7591#autoid-5
        self.grant_types = ["client_credentials"]

    def __repr__(self) -> str:
        cls = type(self)
        return f"{cls.__module__}.{cls.__name__}(id={self.id!r})"

    def get_client_id(self) -> str:
        return self.id

    def get_allowed_scope(self, scope: str) -> str:
        scopes = set(scope_to_list(scope))
        unknown_scopes = scopes - self.scopes
        if unknown_scopes:
            log.warning("requested unknown scopes: %s", ", ".join(sorted(unknown_scopes)))
            unknown_scopes = ", ".join(sorted(unknown_scopes))
            raise InvalidScopes(scopes=unknown_scopes)
        else:
            result = list_to_scope(scopes)
            return result

    def check_client_secret(self, client_secret: Any) -> bool:
        log.debug(f"Incorrect client {self.id!r} secret hash.")
        return passwords.verify(client_secret, self.secret_hash)

    def check_endpoint_auth_method(self, method: str, endpoint: str) -> bool:
        if endpoint == "token":
            return method == self.token_endpoint_auth_method
        return False

    def check_grant_type(self, grant_type: str) -> bool:
        return grant_type in self.grant_types

    def check_requested_scopes(self, scopes: set) -> bool:
        unknown_scopes = scopes - self.scopes
        if unknown_scopes:
            log.warning("requested unknown scopes: %s", ", ".join(sorted(unknown_scopes)))
            unknown_scopes = ", ".join(sorted(unknown_scopes))
            raise InvalidScopes(scopes=unknown_scopes)
        else:
            return True


def decode_unverified_header(token: str) -> dict[str, Any]:
    try:
        if isinstance(token, bytes):
            token = token.decode("utf-8")

        header_b64 = token.split(".")[0]

        # Add padding if missing
        header_b64 += "=" * (-len(header_b64) % 4)
        header_bytes = base64.urlsafe_b64decode(header_b64)

        return json.loads(header_bytes)
    except (UnicodeDecodeError, DecodeError, json.JSONDecodeError, ValueError, IndexError) as e:
        raise InvalidToken(token=token) from e


def decode_kty_from_alg(alg: str) -> Literal["RSA", "EC", None]:
    """Get Key Type from Token Algorithm."""
    if alg.startswith("RS"):
        return "RSA"
    if alg.startswith("ES"):
        return "EC"
    return None


class Token(rfc6749.TokenMixin):
    def __init__(self, token_string, validator: BearerTokenValidator):
        self._token = validator.decode_token(token_string)

        self.expires_in = self._token["exp"] - self._token["iat"]
        self.client_id = self.get_aud()

        self._validator = validator

    def valid_scope(self, scope: SCOPE_TYPE) -> bool:
        required_scopes = scope_to_list(scope)
        return not self._validator.scope_insufficient(self.get_scope(), required_scopes)

    def check_scope(self, scope: SCOPE_TYPE):
        token_scopes = set(scope_to_list(self._token.get("scope", "")))
        if any(token_scope for token_scope in token_scopes if token_scope.startswith(DEPRECATED_SCOPE_PREFIX)):
            log.warning(
                "Deprecation warning: using 'spinta_*' scopes is deprecated and will be removed in a future version."
            )

        if not self.valid_scope(scope):
            client_id = self._token["aud"]

            operator = "OR"
            if isinstance(scope, str):
                operator = "AND"
                scope = [scope]
            missing_scopes = ", ".join(
                sorted([single_scope for single_scope in scope if not single_scope.startswith(DEPRECATED_SCOPE_PREFIX)])
            )

            # FIXME: this should be wrapped into UserError.
            if operator == "AND":
                log.error(f"client {client_id!r} is missing required scopes: %s", missing_scopes)
                raise InsufficientScopeError(description=f"Missing scopes: {missing_scopes}")
            elif operator == "OR":
                log.error(f"client {client_id!r} is missing one of required scopes: %s", missing_scopes)
                raise InsufficientScopeError(description=f"Missing one of scopes: {missing_scopes}")
            else:
                raise Exception(f"Unknown operator {operator}.")

    # No longer mandatory, but will keep it, since it is used in other places.
    def get_client_id(self) -> str:
        return self.get_aud()

    def get_sub(self) -> str:  # User.
        return self._token.get("sub", "")

    def get_aud(self) -> str:  # Client.
        return self._token.get("aud", "")

    def get_jti(self) -> str:
        return self._token.get("jti", "")

    # Currently required implementations for authlib >= 1.0
    # https://gist.github.com/lepture/506bfc29b827fae87981fc58eff2393e#token-model

    def get_scope(self) -> str:
        return self._token.get("scope", "")

    def check_client(self, client) -> bool:
        return self.get_aud() == client.id

    def get_expires_in(self) -> int:
        return self.expires_in

    def is_revoked(self) -> bool:
        return False

    def is_expired(self) -> bool:
        return time.time() > self._token["exp"]


class AdminToken(rfc6749.TokenMixin):
    def valid_scope(self, scope: SCOPE_TYPE, **kwargs) -> bool:
        return True

    def check_scope(self, scope: SCOPE_TYPE, **kwargs):
        pass

    def get_sub(self) -> str:  # User.
        return "admin"

    def get_aud(self) -> str:  # Client.
        return "admin"

    def get_jti(self) -> str:
        return "admin"

    def get_client_id(self) -> str:
        return self.get_aud()


@dataclasses.dataclass
class StarletteOAuth2Data:
    method: str
    uri: str
    headers: Headers
    query: QueryParams
    form: FormData


class StarletteOAuth2Payload(OAuth2Payload):
    # Implementation was taken from DjangoOAuth2Payload

    def __init__(self, data: StarletteOAuth2Data):
        self.query = data.query
        self.form = data.form

    @property
    def data(self) -> dict:
        data = {}
        data.update(self.query)
        data.update(self.form)
        return data

    @cached_property
    def datalist(self) -> dict:
        values = defaultdict(list)
        for k in self.query:
            values[k].extend(self.query.getlist(k))
        for k in self.form:
            values[k].extend(self.form.getlist(k))
        return values


class StarletteOAuth2Request(OAuth2Request):
    def __init__(self, data: StarletteOAuth2Data):
        super().__init__(method=data.method, uri=data.uri, headers=data.headers)
        self.payload = StarletteOAuth2Payload(data)
        self._data = data

    @property
    def args(self) -> QueryParams:
        return self._data.query

    @property
    def form(self) -> FormData:
        return self._data.form


def authenticate_token(protector: ResourceProtector, token: str, type_: str) -> Token:
    type_ = type_.lower()
    validator = protector.get_token_validator(type_)
    return validator.authenticate_token(token)


def get_auth_token(context: Context) -> Token:
    scope = None  # Scopes will be validated later using Token.check_scope
    request: OAuth2Request = context.get("auth.request")

    config = context.get("config")
    if config.default_auth_client and "authorization" not in request.headers:
        default_id = get_default_auth_client_id(context)
        if default_id:
            token = create_client_access_token(context, default_id)
            request.headers = request.headers.mutablecopy()
            request.headers["authorization"] = f"Bearer {token}"

    elif config.http_basic_auth:
        token = get_token_from_http_basic_auth(context, request)
        if token is not None:
            request.headers = request.headers.mutablecopy()
            request.headers["authorization"] = f"Bearer {token}"

    resource_protector = context.get("auth.resource_protector")
    try:
        token = resource_protector.validate_request(scope, request)
    except JoseError as e:
        raise HTTPException(status_code=400, detail=e.error)
    return token


def get_token_from_http_basic_auth(context: Context, request: OAuth2Request):
    if "authorization" not in request.headers:
        raise BasicAuthRequired()

    auth = request.headers["authorization"]
    if " " not in auth:
        raise BasicAuthRequired()

    method, value = request.headers["authorization"].split(None, 1)
    method = method.lower()
    if method != "basic":
        # Pass authentication to authlib.
        return

    value = base64.b64decode(value).decode()
    client, secret = value.split(":", 1)

    # Get client.
    try:
        config = context.get("config")
        client = query_client(get_clients_path(config), client, is_name=True)
    except InvalidClientError:
        raise BasicAuthRequired()

    # Check secret.
    if not client.check_client_secret(secret):
        raise BasicAuthRequired()

    return create_client_access_token(context, client)


def get_auth_request(data: StarletteOAuth2Data) -> StarletteOAuth2Request:
    return StarletteOAuth2Request(data)


def create_key_pair():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())


def load_key_from_file(config: Config, key_type: KeyType) -> dict | None:
    keypath = config.config_path / "keys" / f"{key_type.value}.json"
    if keypath.exists():
        with keypath.open() as f:
            return json.load(f)
    return None


def load_key(context: Context, key_type: KeyType, *, required: bool = True) -> RSAKey | None:
    key = None
    config = context.get("config")
    default_key = load_key_from_file(config, key_type)

    # Public key can be set via configuration.
    if key_type == KeyType.public:
        key = config.token_validation_key

    # Load key from a file.
    if key is None:
        key = default_key

    if isinstance(key, dict) and "keys" in key:
        # Left for backwards compatibility in case private/public file has multiple keys.
        keys = [k for k in key["keys"] if k.get("alg") == "RS512"]
        if keys:
            key = keys[0]
        elif key != default_key:
            key = default_key
        else:
            key = None

    if key is None:
        if required:
            raise NoTokenValidationKey(key_type=key_type.value)
        else:
            return None

    return JsonWebKey.import_key(key)


def create_client_access_token(context: Context, client: Union[str, Client]):
    private_key = load_key(context, KeyType.private)
    if isinstance(client, str):
        client = query_client(get_clients_path(context.get("config")), client)
    expires_in = int(datetime.timedelta(days=10).total_seconds())
    return create_access_token(context, private_key, client.id, expires_in, client.scopes)


def create_access_token(
    context: Context,
    private_key,
    client: str,
    expires_in: int = None,
    scopes: Set[str] = None,
):
    config = context.get("config")

    if expires_in is None:
        expires_in = int(datetime.timedelta(minutes=10).total_seconds())

    header = {
        "typ": "JWT",
        "alg": "RS512",
    }

    iat = int(time.time())
    exp = iat + expires_in
    scopes = " ".join(sorted(scopes)) if scopes else ""
    jti = str(uuid.uuid4())
    payload = {
        "iss": config.server_url,
        "sub": client,
        "aud": client,
        "iat": iat,
        "exp": exp,
        "scope": scopes,
        "jti": jti,
    }
    return jwt.encode(header, payload, private_key).decode("ascii")


def get_client_file_path(path: pathlib.Path, client: str) -> pathlib.Path:
    is_uuid = is_str_uuid(client)
    client_file = path / "unknown" / f"{client}.yml"
    if is_uuid:
        client_file = get_id_path(path) / client[:2] / client[2:4] / f"{client[4:]}.yml"
    return client_file


def check_scope(context: Context, scope: Union[Scopes, str]):
    config = context.get("config")
    token = context.get("auth.token")

    if isinstance(scope, Scopes):
        scope = scope.value

    token.check_scope([f"{config.scope_prefix}{scope}", f"{config.scope_prefix_udts}:{scope}"])


def has_scope(context: Context, scope: Scopes | str, raise_error: bool = True) -> bool:
    valid_scope = False
    try:
        check_scope(context, scope)
        valid_scope = True
    except InsufficientScopeError as error:
        if raise_error:
            raise error

    return valid_scope


def get_scope_name(
    context: Context,
    node: Union[Namespace, Model, Property],
    action: Action,
    is_udts: bool = False,
) -> str:
    config = context.get("config")

    if isinstance(node, Namespace):
        name = node.name
    elif isinstance(node, Model):
        name = node.model_type()
    elif isinstance(node, Property):
        name = node.model.model_type() + "/@" + node.place if is_udts else node.model.model_type() + "_" + node.place
    else:
        raise Exception(f"Unknown node type {node}.")

    if is_udts:
        template = "{prefix}{name}/:{action}" if name else "{prefix}:{action}"
    else:
        template = "{prefix}{name}_{action}" if name else "{prefix}{action}"

    return name_to_scope(
        template,
        name,
        maxlen=config.scope_max_length,
        params={
            "prefix": config.scope_prefix_udts if is_udts else config.scope_prefix,
            "action": "create" if action.value == "insert" and is_udts else action.value,
        },
        is_udts=is_udts,
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
                if uuid_item.endswith(".yml") and len(uuid_item) == 36:
                    ids.append(f"{id0}{id1}{uuid_item[:-4]}")
    return ids


def authorized(
    context: Context,
    node: Union[Namespace, Model, Property],
    action: Action,
    *,
    throw: bool = False,
    scope_formatter: ScopeFormatterFunc = None,
):
    config: Config = context.get("config")
    token = context.get("auth.token")
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
        scope_formatter(context, scope, act, is_udts) for act in action for scope in scopes for is_udts in [False, True]
    ]
    # Check if client has at least one of required scopes.
    if throw:
        token.check_scope(scopes)
    else:
        return token.valid_scope(scopes)


def auth_server_keys_exists(path: pathlib.Path):
    return (path / "keys/private.json").exists() and (path / "keys/public.json").exists()


def gen_auth_server_keys(
    path: pathlib.Path,
    *,
    overwrite: bool = False,
    exist_ok: bool = False,
) -> Tuple[pathlib.Path, pathlib.Path]:
    path = path / "keys"
    path.mkdir(exist_ok=True)
    os.chmod(path, OWNER_READABLE_DIR)

    files = (
        path / "private.json",
        path / "public.json",
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
        private_key = create_key_pair()
        public_key = private_key.public_key()

        with files[0].open("w") as f:
            result = JsonWebKey.import_key(private_key, {"kty": "RSA"})
            json.dump(result.as_dict(is_private=True), f, indent=4, ensure_ascii=False)
        os.chmod(files[0], OWNER_READABLE_FILE)

        with files[1].open("w") as f:
            result = JsonWebKey.import_key(public_key, {"kty": "RSA"})
            json.dump(result.as_dict(), f, indent=4, ensure_ascii=False)
        os.chmod(files[1], WORLD_READABLE_FILE)

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
    secret: str | None = None,
    scopes: List[str] | None = None,
    backends: dict[str, dict[str, str]] | None = None,
    *,
    add_secret: bool = False,
) -> tuple[pathlib.Path, dict]:
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
    os.chmod(id_path / client_id[:2], OWNER_READABLE_DIR)
    os.chmod(id_path / client_id[:2] / client_id[2:4], OWNER_READABLE_DIR)

    secret = secret or passwords.gensecret(32)
    secret_hash = passwords.crypt(secret)

    data = write = {
        "client_id": client_id,
        "client_name": name,
        "client_secret": secret,
        "client_secret_hash": secret_hash,
        "scopes": scopes or [],
        "backends": backends or {},
    }
    keymap[name] = client_id

    if not add_secret:
        write = data.copy()
        del write["client_secret"]
    yml.dump(write, client_file)
    os.chmod(client_file, OWNER_READABLE_FILE)
    yml.dump(keymap, keymap_path)
    os.chmod(keymap_path, OWNER_READABLE_FILE)

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
            """
                Remove only empty folders
            """
    else:
        raise InvalidClientError(description="Invalid client id or secret")


def update_client_file(
    context: Context,
    path: pathlib.Path,
    client_id: str,
    name: str | None,
    secret: str | None,
    scopes: list | None,
    backends: dict[str, dict[str, str]] | None,
) -> dict:
    if client_exists(path, client_id):
        config = context.get("config")
        client = query_client(get_clients_path(config), client_id)
        keymap_path = get_keymap_path(path)
        validate_keymap_path(keymap_path)

        new_name = name if name else client.name
        new_secret_hash = passwords.crypt(secret) if secret else client.secret_hash
        new_scopes = scopes if scopes is not None else client.scopes
        new_backends = backends if backends is not None else client.backends

        client_path = get_client_file_path(path, client_id)
        keymap = _load_keymap_data(keymap_path)
        if new_name != client.name:
            if client_name_exists(keymap, new_name):
                raise ClientWithNameAlreadyExists(client_name=new_name)

        new_data = {
            "client_id": client.id,
            "client_name": new_name,
            "client_secret_hash": new_secret_hash,
            "scopes": list(new_scopes),
            "backends": new_backends,
        }

        yml.dump(new_data, client_path)
        os.chmod(client_path, OWNER_READABLE_FILE)
        if keymap:
            changed = False
            # Check if client changed name
            if client.name != new_name and client.name in keymap.keys():
                del keymap[client.name]
                keymap[new_name] = client.id
                changed = True

            if changed:
                yml.dump(keymap, keymap_path)
                os.chmod(keymap_path, OWNER_READABLE_FILE)
        return new_data
    else:
        raise InvalidClientError(description="Invalid client id or secret")


def get_client_id_from_name(path: pathlib.Path, client_name: str):
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
    os.chmod(clients_path, OWNER_READABLE_DIR)

    # Ensure clients/helpers directory
    helpers_path = get_helpers_path(clients_path)
    helpers_path.mkdir(parents=True, exist_ok=True)
    os.chmod(helpers_path, OWNER_READABLE_DIR)

    # Ensure clients/helpers/keymap.yml exists
    keymap_path = get_keymap_path(clients_path)
    keymap_path.touch(exist_ok=True)
    os.chmod(keymap_path, OWNER_READABLE_FILE)

    # Ensure clients/id directory
    id_path = get_id_path(clients_path)
    id_path.mkdir(parents=True, exist_ok=True)
    os.chmod(id_path, OWNER_READABLE_DIR)


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
    config: Config = context.get("config")
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
            raise InvalidClientError(description="Invalid client name")

        client = client_id

    client_file = get_client_file_path(path, client)
    if not client_file.exists():
        raise InvalidClientError(description="Invalid client id or secret")

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
    config: Config = context.get("config")
    return get_client_id_from_name(get_clients_path(config.config_path), config.default_auth_client)


@cached(LRUCache(CLIENT_FILE_CACHE_SIZE_LIMIT), key=_client_file_cache_key, lock=Lock())
def query_client(path: pathlib.Path, client: str, is_name: bool = False) -> Client:
    if is_name:
        client_id = get_client_id_from_name(path, client)
        if client_id is None:
            raise InvalidClientError(description="Invalid client name")

        client = client_id
    client_file = get_client_file_path(path, client)

    id_path = get_id_path(path)
    validate_id_path(id_path)

    try:
        data = yaml.load(client_file)
    except FileNotFoundError:
        raise InvalidClientError(description="Client file not found. Invalid client id or secret")
    if not isinstance(data, dict):
        raise InvalidClientFileFormat(client_file=client_file.name, client_file_type=type(data))
    if not isinstance(data["scopes"], list):
        raise Exception(f"Client {client_file} scopes must be list of scopes.")
    client_id = data["client_id"]
    client_name = data["client_name"] if ("client_name" in data.keys() and data["client_name"]) else None
    client = Client(
        id_=client_id,
        name_=client_name,
        secret_hash=data["client_secret_hash"],
        scopes=data["scopes"],
        backends=data["backends"] if data.get("backends") else {},
    )
    return client
