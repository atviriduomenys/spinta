import json
import pathlib

import ruamel.yaml

from authlib.jose import jwk
from authlib.jose import jwt

from spinta.cli import genkeys
from spinta.cli import client_add


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
