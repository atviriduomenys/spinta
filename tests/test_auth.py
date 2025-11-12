import datetime
import json
import pathlib
import shutil
import uuid
from http import HTTPStatus

import pytest
import ruamel.yaml
from authlib.jose import JsonWebKey
from authlib.jose import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from spinta import auth, commands
from spinta.auth import (
    get_client_file_path,
    query_client,
    get_clients_path,
    ensure_client_folders_exist,
    KeyType,
    load_key_from_file,
)
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.enums import Action
from spinta.exceptions import InvalidClientFileFormat
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_test_client, get_yaml_data
from spinta.testing.context import create_test_context
from spinta.testing.utils import get_error_codes
from spinta.utils.config import get_keymap_path


def generate_rsa_keypair(kid: str):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    public_numbers = public_key.public_numbers()

    # Build JWK dict
    jwk = {
        "kty": "RSA",
        "kid": kid,
        "use": "sig",
        "alg": "RS512",
        "n": int_to_base64(public_numbers.n),
        "e": int_to_base64(public_numbers.e),
    }

    return private_key, jwk


def int_to_base64(val):
    import base64

    b = val.to_bytes((val.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("utf-8")


def generate_jwt(private_key, kid, scopes="spinta_getall"):
    now = datetime.datetime.now()
    payload = {
        "sub": "user1",
        "exp": now + datetime.timedelta(minutes=5),
        "scope": scopes,
        "iat": int(now.timestamp()),
    }
    token = jwt.encode(
        {"kid": kid, "alg": "RS512"},
        payload,
        private_key,
    )
    return token


def test_app(context, app):
    client_id = "baa448a8-205c-4faa-a048-a10e4b32a136"
    client_secret = "joWgziYLap3eKDL6Gk2SnkJoyz0F8ukB"

    resp = app.post(
        "/auth/token",
        auth=(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "profile",
        },
    )
    assert resp.status_code == 200, resp.text

    data = resp.json()
    assert data == {
        "token_type": "Bearer",
        "expires_in": 864000,
        "scope": "profile",
        "access_token": data["access_token"],
    }

    config = context.get("config")
    key = JsonWebKey.import_key(json.loads((config.config_path / "keys/public.json").read_text()))
    token = jwt.decode(data["access_token"], key)
    assert token == {
        "iss": config.server_url,
        "sub": client_id,
        "aud": client_id,
        "iat": int(token["iat"]),
        "jti": token["jti"],
        "exp": int(token["exp"]),
        "scope": "profile",
    }


def test_genkeys(rc, cli: SpintaCliRunner, tmp_path):
    result = cli.invoke(rc, ["key", "generate", "-p", tmp_path])

    private_path = tmp_path / "keys" / "private.json"
    public_path = tmp_path / "keys" / "public.json"

    assert result.output == f"Private key saved to {private_path}.\nPublic key saved to {public_path}.\n"
    JsonWebKey.import_key(json.loads(private_path.read_text()))
    JsonWebKey.import_key(json.loads(public_path.read_text()))


def test_cant_download_keys(rc, cli: SpintaCliRunner, tmp_path, context, requests_mock):
    result = cli.invoke(rc, ["key", "download"])
    assert result.output == "Error, config.token_validation_keys_download_url is not set.\n"


def test_download_keys(rc: RawConfig, cli: SpintaCliRunner, tmp_path, context, requests_mock):
    mock_url = "https://www.example.com/.well-known/jwks.json"
    rc = rc.fork({"token_validation_keys_download_url": mock_url})
    config = context.get("config")
    well_known = {
        "keys": [
            {
                "kid": "rotation-1",
                "kty": "RSA",
                "alg": "RS512",
                "use": "sig",
                "n": "oAXjeXtZxiEUI7EcG6uITGCuUHmMQxMdTuSkQMaijmX0R1xSN--xBwVRpCJaM_ZYLdmtiBvX7qoNhEXC5H_uzHNxdw",
                "e": "AQAB",
            },
            {
                "kty": "RSA",
                "n": "ngg7HGoRkBDkhLFZpFIF5qOSnWPt7FoThHpP5-HOeVZzrM2NlVKhcJ4sRwn9FFQu1_hHwRt-Lx5UyQ",
                "e": "AQAB",
            },
        ]
    }
    download_mock = requests_mock.get(
        mock_url,
        status_code=HTTPStatus.OK,
        json=well_known,
        headers={"Content-Type": "application/json"},
    )
    result = cli.invoke(rc, ["key", "download"])
    assert download_mock.called
    assert json.loads(config.downloaded_public_keys_file.read_text()) == well_known
    assert result.output == f"Successfully downloaded and stored public keys: {well_known}.\n"


def test_client_add_old(rc, cli: SpintaCliRunner, tmp_path):
    result = cli.invoke(rc, ["client", "add", "-p", tmp_path, "-n", "test"])

    for child in tmp_path.glob("**/*"):
        if not str(child).endswith("keymap.yml"):
            client_file = child
    assert f"client created and saved to:\n\n    {client_file}" in result.output

    yaml = ruamel.yaml.YAML(typ="safe")
    client = yaml.load(client_file)
    assert client == {
        "client_id": client["client_id"],
        "client_name": "test",
        "client_secret_hash": client["client_secret_hash"],
        "scopes": [],
        "backends": {},
    }


def test_client_add(rc, cli: SpintaCliRunner, tmp_path):
    result = cli.invoke(rc, ["client", "add", "-p", tmp_path])

    for child in tmp_path.glob("**/*"):
        if not str(child).endswith("keymap.yml"):
            client_file = child
    assert f"client created and saved to:\n\n    {client_file}" in result.output

    yaml = ruamel.yaml.YAML(typ="safe")
    client = yaml.load(client_file)
    assert client == {
        "client_id": client["client_id"],
        "client_name": client["client_id"],
        "client_secret_hash": client["client_secret_hash"],
        "scopes": [],
        "backends": {},
    }


def test_client_add_default_path(rc, cli: SpintaCliRunner, tmp_path):
    config_path = tmp_path / "config"
    config_path.mkdir(exist_ok=True)
    rc = rc.fork({"config_path": config_path})
    result = cli.invoke(rc, ["client", "add", "-n", "test"])
    clients_path = get_clients_path(config_path)
    keymap = get_keymap_path(clients_path)

    keymap_data = get_yaml_data(keymap)
    client_path = get_client_file_path(clients_path, keymap_data["test"])

    assert f"client created and saved to:\n\n    {client_path}" in result.output

    client = get_yaml_data(client_path)
    assert client == {
        "client_id": client["client_id"],
        "client_name": "test",
        "client_secret_hash": client["client_secret_hash"],
        "scopes": [],
        "backends": {},
    }


@pytest.mark.parametrize("scopes", [{"spinta_getall", "spinta_getone"}, {"uapi:/:getall", "uapi:/:getone"}])
def test_client_add_with_scope(
    rc,
    context: Context,
    cli: SpintaCliRunner,
    tmp_path,
    scopes: set,
):
    scopes_in_string_format = " ".join(scopes)
    cli.invoke(
        rc,
        [
            "client",
            "add",
            "--path",
            tmp_path,
            "--name",
            "test",
            "--scope",
            scopes_in_string_format,
        ],
    )

    client = query_client(get_clients_path(tmp_path), "test", is_name=True)
    assert client.name == "test"
    assert client.scopes == scopes


@pytest.mark.parametrize("scopes", [{"spinta_getall", "spinta_getone"}, {"uapi:/:getall", "uapi:/:getone"}])
def test_client_add_with_scope_via_stdin(
    rc,
    cli: SpintaCliRunner,
    tmp_path,
    scopes: set,
):
    stdin = "\n".join(sorted(scopes)) + "\n"
    cli.invoke(
        rc,
        [
            "client",
            "add",
            "--path",
            tmp_path,
            "--name",
            "test",
            "--scope",
            "-",
        ],
        input=stdin,
    )

    client = query_client(get_clients_path(tmp_path), "test", is_name=True)
    assert client.name == "test"
    assert client.scopes == scopes


def test_empty_scope(context, app):
    client_id = "3388ea36-4a4f-4821-900a-b574c8829d52"
    client_secret = "b5DVbOaEY1BGnfbfv82oA0-4XEBgLQuJ"

    resp = app.post(
        "/auth/token",
        auth=(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "",
        },
    )
    assert resp.status_code == 200, resp.text

    data = resp.json()
    assert "scope" not in data

    config = context.get("config")
    public_path = config.config_path / "keys" / "public.json"

    key = JsonWebKey.import_key(json.loads(public_path.read_text()))
    token = jwt.decode(data["access_token"], key)
    assert token["scope"] == ""


def test_invalid_client(app):
    client_id = "invalid_client"
    client_secret = "b5DVbOaEY1BGnfbfv82oA0-4XEBgLQuJ"

    resp = app.post(
        "/auth/token",
        auth=(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scopes": "",
        },
    )
    assert resp.status_code == 400, resp.text

    assert resp.json() == {"error": "invalid_client", "error_description": "Invalid client name"}


@pytest.mark.parametrize(
    "client, scopes, node, action, authorized",
    [
        ("default-client", "spinta_getone", "backends/mongo/Subitem", "getone", False),
        ("test-client", "spinta_getone", "backends/mongo/Subitem", "getone", True),
        ("test-client", "spinta_getone", "backends/mongo/Subitem", "insert", False),
        ("test-client", "spinta_getone", "backends/mongo/Subitem", "update", False),
        ("test-client", "spinta_backends_getone", "backends/mongo/Subitem", "getone", True),
        ("test-client", "spinta_backends_mongo_subitem_getone", "backends/mongo/Subitem", "getone", True),
        ("default-client", "spinta_backends_mongo_subitem_getone", "backends/mongo/Subitem", "getone", False),
        ("test-client", "spinta_backends_mongo_subitem_getone", "backends/mongo/Subitem", "insert", False),
        ("test-client", "spinta_getone", "backends/mongo/Subitem.subobj", "getone", True),
        ("test-client", "spinta_backends_mongo_getone", "backends/mongo/Subitem.subobj", "getone", True),
        ("test-client", "spinta_backends_mongo_subitem_getone", "backends/mongo/Subitem.subobj", "getone", True),
        ("test-client", "spinta_backends_mongo_subitem_subobj_getone", "backends/mongo/Subitem.subobj", "getone", True),
        (
            "test-client",
            "spinta_backends_mongo_subitem_subobj_getone",
            "backends/mongo/Subitem.subobj",
            "insert",
            False,
        ),
        (
            "default-client",
            "spinta_backends_mongo_subitem_subobj_getone",
            "backends/mongo/Subitem.subobj",
            "getone",
            False,
        ),
        ("test-client", "spinta_getone", "backends/mongo/Subitem.hidden_subobj", "getone", False),
        ("test-client", "spinta_backends_mongo_getone", "backends/mongo/Subitem.hidden_subobj", "getone", False),
        (
            "test-client",
            "spinta_backends_mongo_subitem_getone",
            "backends/mongo/Subitem.hidden_subobj",
            "getone",
            False,
        ),
        (
            "test-client",
            "spinta_backends_mongo_subitem_hidden_subobj_getone",
            "backends/mongo/Subitem.hidden_subobj",
            "getone",
            True,
        ),
        (
            "test-client",
            "spinta_backends_mongo_subitem_hidden_subobj_getone",
            "backends/mongo/Subitem.hidden_subobj",
            "update",
            False,
        ),
        (
            "default-client",
            "spinta_backends_mongo_subitem_hidden_subobj_getone",
            "backends/mongo/Subitem.hidden_subobj",
            "getone",
            False,
        ),
        ("default-client", "uapi:/:getone", "backends/mongo/Subitem", "getone", False),
        ("test-client", "uapi:/:getone", "backends/mongo/Subitem", "getone", True),
        ("test-client", "uapi:/:getone", "backends/mongo/Subitem", "insert", False),
        ("test-client", "uapi:/:getone", "backends/mongo/Subitem", "update", False),
        ("test-client", "uapi:/backends/:getone", "backends/mongo/Subitem", "getone", True),
        ("test-client", "uapi:/backends/mongo/Subitem/:getone", "backends/mongo/Subitem", "getone", True),
        ("default-client", "uapi:/backends/mongo/Subitem/:getone", "backends/mongo/Subitem", "getone", False),
        ("test-client", "uapi:/backends/mongo/Subitem/:getone", "backends/mongo/Subitem", "insert", False),
        ("test-client", "uapi:/:getone", "backends/mongo/Subitem.subobj", "getone", True),
        ("test-client", "uapi:/backends/mongo/:getone", "backends/mongo/Subitem.subobj", "getone", True),
        ("test-client", "uapi:/backends/mongo/Subitem/:getone", "backends/mongo/Subitem.subobj", "getone", True),
        (
            "test-client",
            "uapi:/backends/mongo/Subitem/@subobj/:getone",
            "backends/mongo/Subitem.subobj",
            "getone",
            True,
        ),
        (
            "test-client",
            "uapi:/backends/mongo/Subitem/@subobj/:getone",
            "backends/mongo/Subitem.subobj",
            "insert",
            False,
        ),
        (
            "default-client",
            "uapi:/backends/mongo/Subitem/@subobj/:getone",
            "backends/mongo/Subitem.subobj",
            "getone",
            False,
        ),
        ("test-client", "uapi:/:getone", "backends/mongo/Subitem.hidden_subobj", "getone", False),
        ("test-client", "uapi:/backends/mango/:getone", "backends/mongo/Subitem.hidden_subobj", "getone", False),
        (
            "test-client",
            "uapi:/backends/mongo/Subitem/@hidden_subobj/:getone",
            "backends/mongo/Subitem.hidden_subobj",
            "getone",
            True,
        ),
        (
            "test-client",
            "uapi:/backends/mongo/Subitem/@hidden_subobj/:getone",
            "backends/mongo/Subitem.hidden_subobj",
            "update",
            False,
        ),
        (
            "test-client",
            "uapi:/backends/mongo/Subitem/:getone",
            "backends/mongo/Subitem.hidden_subobj",
            "getone",
            False,
        ),
        (
            "default-client",
            "uapi:/backends/mongo/Subitem/@hidden_subobj/:getone",
            "backends/mongo/Subitem.hidden_subobj",
            "getone",
            False,
        ),
        ("test-client", "uapi:/backends/mongo/Subitem/:create", "backends/mongo/Subitem", "insert", True),
        ("test-client", "uapi:/:create", "backends/mongo/Subitem", "insert", True),
    ],
)
def test_authorized(context, client, scopes, node, action, authorized):
    if client == "default-client":
        client = context.get("config").default_auth_client
    scopes = [scopes]
    pkey = auth.load_key(context, auth.KeyType.private)
    token = auth.create_access_token(context, pkey, client, scopes=scopes)
    token = auth.Token(token, auth.BearerTokenValidator(context))
    context.set("auth.token", token)
    store = context.get("store")
    if "." in node:
        model, prop = node.split(".", 1)
        node = commands.get_model(context, store.manifest, model).flatprops[prop]
    elif commands.has_model(context, store.manifest, node):
        node = commands.get_model(context, store.manifest, node)
    else:
        node = commands.get_namespace(context, store.manifest, node)
    action = getattr(Action, action.upper())
    assert auth.authorized(context, node, action) is authorized


def test_invalid_access_token(app):
    app.headers.update({"Authorization": "Bearer FAKE_TOKEN"})
    resp = app.get("/Report")
    assert resp.status_code == 401
    assert "WWW-Authenticate" in resp.headers
    assert resp.headers["WWW-Authenticate"] == 'Bearer error="invalid_token"'
    assert get_error_codes(resp.json()) == ["InvalidToken"]


@pytest.mark.parametrize("scopes", [["spinta_report_getall"], ["uapi:/Report/:getall"]])
def test_token_validation_key_config(backends, rc, tmp_path, request, scopes: list):
    confdir = pathlib.Path(__file__).parent
    prvkey = json.loads((confdir / "config/keys/private.json").read_text())
    pubkey = json.loads((confdir / "config/keys/public.json").read_text())

    rc = rc.fork(
        {
            "config_path": str(tmp_path),
            "default_auth_client": None,
            "token_validation_key": json.dumps(pubkey),
        }
    )

    context = create_test_context(rc).load()
    request.addfinalizer(context.wipe_all)

    prvkey = JsonWebKey.import_key(prvkey)
    client = "RANDOMID"
    scopes = scopes
    token = auth.create_access_token(context, prvkey, client, scopes=scopes)

    client = create_test_client(context)
    resp = client.get("/Report", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


@pytest.fixture(params=[["spinta_getall"], ["uapi:/:getall"]])
def basic_auth(backends, rc, tmp_path, request):
    scopes = request.param

    confdir = pathlib.Path(__file__).parent / "config"
    shutil.copytree(str(confdir / "keys"), str(tmp_path / "keys"))

    path = get_clients_path(tmp_path)
    ensure_client_folders_exist(path)
    new_id = uuid.uuid4()
    auth.create_client_file(
        path,
        name="default",
        client_id=str(new_id),
        secret="secret",
        scopes=scopes,
        add_secret=True,
    )

    rc = rc.fork(
        {
            "config_path": str(tmp_path),
            "default_auth_client": None,
            "http_basic_auth": True,
        }
    )

    context = create_test_context(rc).load()
    request.addfinalizer(context.wipe_all)

    client = create_test_client(context)

    return client


def test_http_basic_auth_unauthorized(basic_auth):
    client = basic_auth
    resp = client.get("/Report")
    assert resp.status_code == 401, resp.json()
    assert resp.headers["www-authenticate"] == 'Basic realm="Authentication required."'
    assert resp.json() == {
        "errors": [
            {
                "code": "BasicAuthRequired",
                "context": {},
                "message": "Unauthorized",
                "template": "Unauthorized",
                "type": "system",
            },
        ],
    }


def test_http_basic_auth_invalid_secret(basic_auth):
    client = basic_auth
    resp = client.get("/Report", auth=("default", "invalid"))
    assert resp.status_code == 401, resp.json()
    assert resp.headers["www-authenticate"] == 'Basic realm="Authentication required."'


def test_http_basic_auth_invalid_client(basic_auth):
    client = basic_auth
    resp = client.get("/Report", auth=("invalid", "secret"))
    assert resp.status_code == 401, resp.json()
    assert resp.headers["www-authenticate"] == 'Basic realm="Authentication required."'


def test_http_basic_auth(basic_auth):
    client = basic_auth
    resp = client.get("/Report", auth=("default", "secret"))
    assert resp.status_code == 200, resp.json()


def test_get_client_file_path_uuid(tmp_path):
    file_name = "a6c06c3a-3aa4-4704-b144-4fc23e2152f7"
    assert str(get_client_file_path(get_clients_path(tmp_path), file_name)) == str(
        tmp_path / "clients" / "id" / "a6" / "c0" / "6c3a-3aa4-4704-b144-4fc23e2152f7.yml"
    )


def test_invalid_scope(context, app):
    client_id = "3388ea36-4a4f-4821-900a-b574c8829d52"
    client_secret = "b5DVbOaEY1BGnfbfv82oA0-4XEBgLQuJ"
    unknown_scope = "unknown_scope1"

    resp = app.post(
        "/auth/token",
        auth=(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": unknown_scope,
        },
    )
    assert resp.status_code == 400, resp.text
    assert get_error_codes(resp.json()) == ["InvalidScopes"]


@pytest.mark.parametrize(
    "scopes",
    [
        [
            "spinta_getone",
            "spinta_getall",
            "spinta_search",
        ],
        [
            "uapi:/:getone",
            "uapi:/:getall",
            "uapi:/:search",
        ],
    ],
)
def test_invalid_client_file_data_type_list(
    tmp_path,
    context,
    cli,
    rc,
    scopes: list,
):
    cli.invoke(rc, ["client", "add", "-p", tmp_path, "-n", "test"])

    for child in tmp_path.glob("**/*"):
        if not str(child).endswith("keymap.yml"):
            client_file = child
    yaml = ruamel.yaml.YAML(typ="safe")
    scopes = scopes
    yaml.dump(scopes, client_file)
    with pytest.raises(InvalidClientFileFormat, match="File .* data must be a dictionary, not a <class 'list'>."):
        query_client(get_clients_path(tmp_path), "test", is_name=True)


@pytest.mark.parametrize(
    "scopes",
    [
        [
            "spinta_getone",
            "spinta_getall",
            "spinta_search",
        ],
        [
            "uapi:/:getone",
            "uapi:/:getall",
            "uapi:/:search",
        ],
    ],
)
def test_invalid_client_file_data_type_str(
    tmp_path,
    context,
    cli,
    rc,
    scopes: list,
):
    cli.invoke(rc, ["client", "add", "-p", tmp_path, "-n", "test"])

    for child in tmp_path.glob("**/*"):
        if not str(child).endswith("keymap.yml"):
            client_file = child
    yaml = ruamel.yaml.YAML(typ="safe")
    scopes = scopes
    yaml.dump(str(scopes), client_file)
    with pytest.raises(InvalidClientFileFormat, match="File .* data must be a dictionary, not a <class 'str'>."):
        query_client(get_clients_path(tmp_path), "test", is_name=True)


def test_get_public_jwk_verification_keys_from_config(app, context):
    config = context.get("config")
    jwk_keys = [
        {"alg": "RS512", "e": "AQAB", "kid": "rotation-1", "kty": "RSA", "n": "jwkrimvoifsdvicmdf", "use": "sig"},
        {"alg": "RS512", "e": "AQAB", "kid": "rotation-2", "kty": "RSA", "n": "asdsad-asd", "use": "sig"},
    ]
    config.token_validation_key = {"keys": jwk_keys}
    resp = app.get("/.well-known/jwks.json")
    assert resp.status_code == 200, resp.text
    assert resp.json()
    received_keys = resp.json()["keys"]
    assert received_keys
    for key in jwk_keys:
        assert key in received_keys
    assert load_key_from_file(config, KeyType.public) not in received_keys
    config.token_validation_key = None


def test_get_public_jwk_verification_keys_from_file(app, context):
    config = context.get("config")
    config.token_validation_key = None
    resp = app.get("/.well-known/jwks.json")
    assert resp.status_code == 200, resp.text
    assert resp.json()
    received_keys = resp.json()["keys"]
    assert received_keys
    assert [load_key_from_file(config, KeyType.public)] == received_keys


def test_pick_correct_key(app, context):
    config = context.get("config")

    private_1, jwk1 = generate_rsa_keypair("rotation-1")
    private_2, jwk2 = generate_rsa_keypair("rotation-2")

    config.token_validation_key = {"keys": [jwk1, jwk2]}

    token = generate_jwt(private_2, "rotation-2")

    resp = app.get("/datasets/backends/postgres/dataset/:all", headers={"Authorization": f"Bearer {token.decode()}"})
    assert resp.status_code == 200, resp.text
    config.token_validation_key = None
