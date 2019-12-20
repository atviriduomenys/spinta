import datetime
import json
import pathlib

import pytest
import ruamel.yaml

from authlib.jose import jwk
from authlib.jose import jwt
from authlib.oauth2.rfc6750.errors import InsufficientScopeError

from spinta import auth
from spinta.cli import genkeys
from spinta.cli import client_add
from spinta.components import Action


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


def test_genkeys(cli, tmpdir):
    result = cli.invoke(genkeys, ['-p', str(tmpdir)], catch_exceptions=False)
    assert result.output == (
        f'Private key saved to {tmpdir}/private.json.\n'
        f'Public key saved to {tmpdir}/public.json.\n'
    )
    jwk.loads(json.loads(tmpdir.join('private.json').read()))
    jwk.loads(json.loads(tmpdir.join('public.json').read()))


def test_client_add(cli, tmpdir):
    result = cli.invoke(client_add, ['-p', str(tmpdir)], catch_exceptions=False)

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


def args_for_token(context):
    private_key = auth.load_key(context, 'private.json')
    client_id = 'baa448a8-205c-4faa-a048-a10e4b32a136'
    client = auth.query_client(context, client_id)
    grant_type = 'client_credentials'
    expires_in = int(datetime.timedelta(days=10).total_seconds())
    return context, private_key, client, grant_type, expires_in


def test_check_generated_scopes_global(context, app):
    # tests global scope - any scopes action is allowed
    scopes = ['spinta_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        auth.check_generated_scopes(ctx,
                                    'backends/mongo/subitem',
                                    Action.GETONE.value)


def test_check_generated_scopes_global_wrong_action(context, app):
    # tests global scope - any scopes action is allowed
    scopes = ['spinta_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        with pytest.raises(InsufficientScopeError):
            auth.check_generated_scopes(ctx,
                                        'backends/mongo/subitem',
                                        Action.INSERT.value)


def test_check_generated_scopes_model(context, app):
    scopes = ['spinta_backends_mongo_subitem_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        auth.check_generated_scopes(ctx,
                                    'backends/mongo/subitem',
                                    Action.GETONE.value)


def test_check_generated_scopes_model_wrong_action(context, app):
    scopes = ['spinta_backends_mongo_subitem_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        with pytest.raises(InsufficientScopeError):
            auth.check_generated_scopes(ctx,
                                        'backends/mongo/subitem',
                                        Action.INSERT.value)


def test_check_generated_scopes_prop(context, app):
    scopes = ['spinta_backends_mongo_subitem_subobj_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        auth.check_generated_scopes(ctx,
                                    'backends/mongo/subitem',
                                    Action.GETONE.value,
                                    'backends/mongo/subitem_subobj')


def test_check_generated_scopes_prop_wrong_action(context, app):
    scopes = ['spinta_backends_mongo_subitem_subobj_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        with pytest.raises(InsufficientScopeError):
            auth.check_generated_scopes(ctx,
                                        'backends/mongo/subitem',
                                        Action.INSERT.value,
                                        'backends/mongo/subitem_subobj')


def test_check_generated_scopes_prop_w_model_scope(context, app):
    scopes = ['spinta_backends_mongo_subitem_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        auth.check_generated_scopes(ctx,
                                    'backends/mongo/subitem',
                                    Action.GETONE.value,
                                    'backends/mongo/subitem_subobj')


def test_check_generated_scopes_prop_hidden(context, app):
    scopes = ['spinta_backends_mongo_subitem_hidden_subobj_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        auth.check_generated_scopes(ctx,
                                    'backends/mongo/subitem',
                                    Action.GETONE.value,
                                    'backends/mongo/subitem_hidden_subobj',
                                    True)


def test_check_generated_scopes_prop_hidden_w_model_scope(context, app):
    scopes = ['spinta_backends_mongo_subitem_getone']
    token = auth.create_access_token(*args_for_token(context), scopes=scopes)
    token_instance = auth.Token(token, auth.BearerTokenValidator(context))
    with context.fork('test_scope') as ctx:
        ctx.set('auth.token', token_instance)
        with pytest.raises(InsufficientScopeError):
            auth.check_generated_scopes(ctx,
                                        'backends/mongo/subitem',
                                        Action.GETONE.value,
                                        'backends/mongo/subitem_hidden_subobj',
                                        True)
