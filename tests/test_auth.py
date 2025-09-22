import io
import json
import pathlib
import shutil
import uuid

import pytest
import ruamel.yaml
from authlib.jose import JsonWebKey
from authlib.jose import jwt

from spinta import auth, commands
from spinta.auth import get_client_file_path, query_client, get_clients_path, ensure_client_folders_exist
from spinta.components import Context
from spinta.core.enums import Action
from spinta.exceptions import InvalidClientFileFormat
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_test_client, get_yaml_data
from spinta.testing.context import create_test_context
from spinta.testing.utils import get_error_codes
from spinta.utils.config import get_keymap_path


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
    result = cli.invoke(rc, ["genkeys", "-p", tmp_path])

    private_path = tmp_path / "keys" / "private.json"
    public_path = tmp_path / "keys" / "public.json"

    assert result.output == f"Private key saved to {private_path}.\nPublic key saved to {public_path}.\n"
    JsonWebKey.import_key(json.loads(private_path.read_text()))
    JsonWebKey.import_key(json.loads(public_path.read_text()))


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


@pytest.mark.parametrize("scope", [{"spinta_getall", "spinta_getone"}, {"uapi:/:getall", "uapi:/:getone"}])
def test_client_add_with_scope(
    rc,
    context: Context,
    cli: SpintaCliRunner,
    tmp_path,
    scope: set,
):
    scopes_in_string_format = " ".join(scope)
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
    assert client.scopes == scope


@pytest.mark.parametrize("scope", [{"spinta_getall", "spinta_getone"}, {"uapi:/:getall", "uapi:/:getone"}])
def test_client_add_with_scope_via_stdin(
    rc,
    cli: SpintaCliRunner,
    tmp_path,
    scope: set,
):
    stdin = "\n".join(sorted(scope)) + "\n"
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
    assert client.scopes == scope


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
            "scope": "",
        },
    )
    assert resp.status_code == 400, resp.text

    assert resp.json() == {"error": "invalid_client", "error_description": "Invalid client name"}


@pytest.mark.parametrize(
    "client, scope, node, action, authorized",
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
def test_authorized(context, client, scope, node, action, authorized):
    if client == "default-client":
        client = context.get("config").default_auth_client
    scopes = [scope]
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


@pytest.mark.parametrize("scope", [["spinta_report_getall"], ["uapi:/Report/:getall"]])
def test_token_validation_key_config(backends, rc, tmp_path, request, scope: list):
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
    scopes = scope
    token = auth.create_access_token(context, prvkey, client, scopes=scopes)

    client = create_test_client(context)
    resp = client.get("/Report", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


@pytest.fixture(params=[["spinta_getall"], ["uapi:/:getall"]])
def basic_auth(backends, rc, tmp_path, request):
    scope = request.param

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
        scopes=scope,
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
    "scope",
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
    scope: list,
):
    cli.invoke(rc, ["client", "add", "-p", tmp_path, "-n", "test"])

    for child in tmp_path.glob("**/*"):
        if not str(child).endswith("keymap.yml"):
            client_file = child
    yaml = ruamel.yaml.YAML(typ="safe")
    scopes = scope
    yaml.dump(scopes, client_file)
    with pytest.raises(InvalidClientFileFormat, match="File .* data must be a dictionary, not a <class 'list'>."):
        query_client(get_clients_path(tmp_path), "test", is_name=True)


@pytest.mark.parametrize(
    "scope",
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
    scope: list,
):
    cli.invoke(rc, ["client", "add", "-p", tmp_path, "-n", "test"])

    for child in tmp_path.glob("**/*"):
        if not str(child).endswith("keymap.yml"):
            client_file = child
    yaml = ruamel.yaml.YAML(typ="safe")
    scopes = scope
    yaml.dump(str(scopes), client_file)
    with pytest.raises(InvalidClientFileFormat, match="File .* data must be a dictionary, not a <class 'str'>."):
        query_client(get_clients_path(tmp_path), "test", is_name=True)
