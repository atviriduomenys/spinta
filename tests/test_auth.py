import json
import pathlib

import ruamel.yaml

from authlib.jose import jwk
from authlib.jose import jwt

from spinta.cli import genkeys
from spinta.cli import client_add


def test_app(context, app):
    client_id = '63957342-bbf6-4efb-9e34-c17e42bbd59c'
    client_secret = 'RG2xtKk05HWdWHL7VIEdjKurDz2Ns3l3'

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
    assert f'New client created at {client_file}' in result.output

    yaml = ruamel.yaml.YAML(typ='safe')
    client = yaml.load(client_file)
    assert client == {
        'client_id': client['client_id'],
        'client_secret_hash': client['client_secret_hash'],
        'scopes': [],
    }
