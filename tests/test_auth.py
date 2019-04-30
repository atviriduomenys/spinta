import json

from authlib.jose import jwk
from authlib.jose import jwt

from spinta.cli import genkeys


def test_app(context, app):
    resp = app.post('/auth/token', auth=('user', 'pass'), data={
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
    key = jwk.loads(json.loads((config.keys_dir / 'public.json').read_text()))
    token = jwt.decode(data['access_token'], key)
    assert token == {
        'iss': 'Authlib',
        'sub': '123',
    }


def test_genkeys(cli, tmpdir):
    result = cli.invoke(genkeys, [str(tmpdir)], catch_exceptions=False)
    assert result.output == (
        f'Private key saved to {tmpdir}/private.json.\n'
        f'Public key saved to {tmpdir}/public.json.\n'
    )
