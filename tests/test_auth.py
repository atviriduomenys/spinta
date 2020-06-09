import json
import pathlib
import shutil

import pytest
import ruamel.yaml

from authlib.jose import jwk
from authlib.jose import jwt

from spinta import auth
from spinta.cli import genkeys
from spinta.cli import client_add
from spinta.components import Action
from spinta.testing.utils import get_error_codes
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context


def test_app(context, app):
    client_id = 'baa448a8-205c-4faa-a048-a10e4b32a136'
    client_secret = 'joWgziYLap3eKDL6Gk2SnkJoyz0F8ukB'

    resp = app.post('/auth/token', auth=(client_id, client_secret), data={
        'grant_type': 'client_credentials',
        'scope': 'profile',
    })
    assert resp.status_code == 200, resp.text

    data = resp.json()
    assert data == {
        'token_type': 'Bearer',
        'expires_in': 864000,
        'scope': 'profile',
        'access_token': data['access_token'],
    }

    config = context.get('config')
    key = jwk.loads(json.loads((config.config_path / 'keys/public.json').read_text()))
    token = jwt.decode(data['access_token'], key)
    assert token == {
        'iss': config.server_url,
        'sub': client_id,
        'aud': client_id,
        'iat': int(token['iat']),
        'exp': int(token['exp']),
        'scope': 'profile',
    }


def test_genkeys(rc, cli, tmpdir):
    result = cli.invoke(rc, genkeys, ['-p', str(tmpdir)])
    assert result.output == (
        f'Private key saved to {tmpdir}/keys/private.json.\n'
        f'Public key saved to {tmpdir}/keys/public.json.\n'
    )
    jwk.loads(json.loads(tmpdir.join('keys/private.json').read()))
    jwk.loads(json.loads(tmpdir.join('keys/public.json').read()))


def test_client_add(rc, cli, tmpdir):
    result = cli.invoke(rc, client_add, ['-p', str(tmpdir)])

    client_file = pathlib.Path(str(tmpdir.listdir()[0]))
    assert f'client created and saved to:\n\n    {client_file}' in result.output

    yaml = ruamel.yaml.YAML(typ='safe')
    client = yaml.load(client_file)
    assert client == {
        'client_id': client['client_id'],
        'client_secret_hash': client['client_secret_hash'],
        'scopes': [],
    }


def test_empty_scope(context, app):
    client_id = '3388ea36-4a4f-4821-900a-b574c8829d52'
    client_secret = 'b5DVbOaEY1BGnfbfv82oA0-4XEBgLQuJ'

    resp = app.post('/auth/token', auth=(client_id, client_secret), data={
        'grant_type': 'client_credentials',
        'scope': '',
    })
    assert resp.status_code == 200, resp.text

    data = resp.json()
    assert 'scope' not in data

    config = context.get('config')
    key = jwk.loads(json.loads((config.config_path / 'keys/public.json').read_text()))
    token = jwt.decode(data['access_token'], key)
    assert token['scope'] == ''


def test_invalid_client(app):
    client_id = 'invalid_client'
    client_secret = 'b5DVbOaEY1BGnfbfv82oA0-4XEBgLQuJ'

    resp = app.post('/auth/token', auth=(client_id, client_secret), data={
        'grant_type': 'client_credentials',
        'scope': '',
    })
    assert resp.status_code == 400, resp.text

    assert resp.json() == {
        "error": "invalid_client",
        "error_description": "Invalid client id or secret"
    }


@pytest.mark.parametrize('client, scope, node, action, authorized', [
    ('default-client', 'spinta_getone', 'backends/mongo/subitem', 'getone', False),
    ('test-client', 'spinta_getone', 'backends/mongo/subitem', 'getone', True),
    ('test-client', 'spinta_getone', 'backends/mongo/subitem', 'insert', False),
    ('test-client', 'spinta_getone', 'backends/mongo/subitem', 'update', False),
    ('test-client', 'spinta_backends_getone', 'backends/mongo/subitem', 'getone', True),
    ('test-client', 'spinta_backends_mongo_subitem_getone', 'backends/mongo/subitem', 'getone', True),
    ('default-client', 'spinta_backends_mongo_subitem_getone', 'backends/mongo/subitem', 'getone', False),
    ('test-client', 'spinta_backends_mongo_subitem_getone', 'backends/mongo/subitem', 'insert', False),
    ('test-client', 'spinta_getone', 'backends/mongo/subitem.subobj', 'getone', True),
    ('test-client', 'spinta_backends_mongo_getone', 'backends/mongo/subitem.subobj', 'getone', True),
    ('test-client', 'spinta_backends_mongo_subitem_getone', 'backends/mongo/subitem.subobj', 'getone', True),
    ('test-client', 'spinta_backends_mongo_subitem_subobj_getone', 'backends/mongo/subitem.subobj', 'getone', True),
    ('test-client', 'spinta_backends_mongo_subitem_subobj_getone', 'backends/mongo/subitem.subobj', 'insert', False),
    ('default-client', 'spinta_backends_mongo_subitem_subobj_getone', 'backends/mongo/subitem.subobj', 'getone', False),
    ('test-client', 'spinta_getone', 'backends/mongo/subitem.hidden_subobj', 'getone', False),
    ('test-client', 'spinta_backends_mongo_getone', 'backends/mongo/subitem.hidden_subobj', 'getone', False),
    ('test-client', 'spinta_backends_mongo_subitem_getone', 'backends/mongo/subitem.hidden_subobj', 'getone', False),
    ('test-client', 'spinta_backends_mongo_subitem_hidden_subobj_getone', 'backends/mongo/subitem.hidden_subobj', 'getone', True),
    ('test-client', 'spinta_backends_mongo_subitem_hidden_subobj_getone', 'backends/mongo/subitem.hidden_subobj', 'update', False),
    ('default-client', 'spinta_backends_mongo_subitem_hidden_subobj_getone', 'backends/mongo/subitem.hidden_subobj', 'getone', False),
])
def test_authorized(context, client, scope, node, action, authorized):
    if client == 'default-client':
        client = context.get('config').default_auth_client
    scopes = [scope]
    pkey = auth.load_key(context, auth.KeyType.private)
    token = auth.create_access_token(context, pkey, client, scopes=scopes)
    token = auth.Token(token, auth.BearerTokenValidator(context))
    context.set('auth.token', token)
    store = context.get('store')
    if '.' in node:
        model, prop = node.split('.', 1)
        node = store.manifest.models[model].flatprops[prop]
    elif node in store.manifest.models:
        node = store.manifest.models[node]
    else:
        node = store.manifest.objects['ns'][node]
    action = getattr(Action, action.upper())
    assert auth.authorized(context, node, action) is authorized


def test_invalid_access_token(app):
    app.headers.update({"Authorization": "Bearer FAKE_TOKEN"})
    resp = app.get('/reports')
    assert resp.status_code == 401
    assert 'WWW-Authenticate' in resp.headers
    assert resp.headers['WWW-Authenticate'] == 'Bearer error="invalid_token"'
    assert get_error_codes(resp.json()) == ["InvalidToken"]


def test_token_validation_key_config(backends, rc, tmpdir, request):
    confdir = pathlib.Path(__file__).parent
    prvkey = json.loads((confdir / 'config/keys/private.json').read_text())
    pubkey = json.loads((confdir / 'config/keys/public.json').read_text())

    rc = rc.fork({
        'config_path': str(tmpdir),
        'default_auth_client': None,
        'token_validation_key': json.dumps(pubkey),
    })

    context = create_test_context(rc).load()
    request.addfinalizer(context.wipe_all)

    prvkey = jwk.loads(prvkey)
    client = 'RANDOMID'
    scopes = ['spinta_report_getall']
    token = auth.create_access_token(context, prvkey, client, scopes=scopes)

    client = create_test_client(context)
    resp = client.get('/reports', headers={'Authorization': f'Bearer {token}'})
    assert resp.status_code == 200


@pytest.fixture()
def basicauth(backends, rc, tmpdir, request):
    confdir = pathlib.Path(__file__).parent / 'config'
    shutil.copytree(str(confdir / 'keys'), str(tmpdir / 'keys'))

    (tmpdir / 'clients').mkdir()
    auth.create_client_file(
        tmpdir / 'clients',
        client='default',
        secret='secret',
        scopes=['spinta_getall'],
        add_secret=True,
    )

    rc = rc.fork({
        'config_path': str(tmpdir),
        'default_auth_client': None,
        'http_basic_auth': True,
    })

    context = create_test_context(rc).load()
    request.addfinalizer(context.wipe_all)

    client = create_test_client(context)

    return client


def test_http_basic_auth_unauthorized(basicauth):
    client = basicauth
    resp = client.get('/reports')
    assert resp.status_code == 401, resp.json()
    assert resp.headers['www-authenticate'] == 'Basic realm="Authentication required."'
    assert resp.json() == {
        'errors': [
            {
                'code': 'BasicAuthRequired',
                'context': {},
                'message': 'Unauthorized',
                'template': 'Unauthorized',
                'type': 'system',
            },
        ],
    }


def test_http_basic_auth_invalid_secret(basicauth):
    client = basicauth
    resp = client.get('/reports', auth=('default', 'invalid'))
    assert resp.status_code == 401, resp.json()
    assert resp.headers['www-authenticate'] == 'Basic realm="Authentication required."'


def test_http_basic_auth_invalid_client(basicauth):
    client = basicauth
    resp = client.get('/reports', auth=('invalid', 'secret'))
    assert resp.status_code == 401, resp.json()
    assert resp.headers['www-authenticate'] == 'Basic realm="Authentication required."'


def test_http_basic_auth(basicauth):
    client = basicauth
    resp = client.get('/reports', auth=('default', 'secret'))
    assert resp.status_code == 200, resp.json()
