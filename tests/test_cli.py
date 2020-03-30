import pytest

import json
import pathlib

from responses import GET, POST

from spinta.cli import pull, push
from spinta.testing.utils import create_manifest_files
from spinta.testing.utils import update_manifest_files


@pytest.mark.skip('datasets')
def test_pull(responses, rc, cli, app, tmpdir):
    responses.add(
        GET, 'http://example.com/countries.csv',
        status=200, content_type='text/plain; charset=utf-8',
        body=(
            'kodas,šalis\n'
            'lt,Lietuva\n'
            'lv,Latvija\n'
            'ee,Estija'
        ),
    )

    create_manifest_files(tmpdir, {
        'datasets/csv.yml': None,
        'datasets/csv/country.yml': None,
    })
    update_manifest_files(tmpdir, {
        'datasets/csv.yml': [
            {'op': 'add', 'path': '/resources/countries/backend', 'value': 'csv'},
        ],
    })
    rc = rc.fork({
        'manifests.default': {
            'type': 'yaml',
            'path': str(tmpdir),
        },
        'backends.csv': {
            'type': 'csv',
        },
    })

    result = cli.invoke(rc, pull, ['datasets/csv'])
    assert result.output == (
        '\n'
        '\n'
        'Table: country/:dataset/csv/:resource/countries\n'
        '                  _id                      code    title     _op                         _where                    \n'
        '===================================================================================================================\n'
        '552c4c243ec8c98c313255ea9bf16ee286591f8c   lt     Lietuva   upsert   _id="552c4c243ec8c98c313255ea9bf16ee286591f8c"\n'
        'b5dcb86880816fb966cdfbbacd1f3406739464f4   lv     Latvija   upsert   _id="b5dcb86880816fb966cdfbbacd1f3406739464f4"\n'
        '68de1c04d49aeefabb7081a5baf81c055f235be3   ee     Estija    upsert   _id="68de1c04d49aeefabb7081a5baf81c055f235be3"'
    )

    app.authmodel('country/:dataset/csv/:resource/countries', ['getall'])

    assert app.get('/country/:dataset/csv/:resource/countries').json() == {
        '_data': [],
    }

    result = cli.invoke(rc, pull, ['csv', '--push'])
    assert 'csv:' in result.output

    rows = sorted(
        (row['code'], row['title'], row['_type'])
        for row in app.get('/country/:dataset/csv/:resource/countries').json()['_data']
    )
    assert rows == [
        ('ee', 'Estija', 'country/:dataset/csv/:resource/countries'),
        ('lt', 'Lietuva', 'country/:dataset/csv/:resource/countries'),
        ('lv', 'Latvija', 'country/:dataset/csv/:resource/countries'),
    ]


def test_push(app, rc, cli, responses, tmpdir):
    continent = 'datasets/backends/postgres/dataset/continent'
    app.authorize([
        'spinta_set_meta_fields',
        'spinta_getall',
    ])
    app.authmodel(continent, ['insert', 'upsert'])

    tmpdir = pathlib.Path(str(tmpdir))
    credsfile = tmpdir / 'credentials.cfg'
    credsfile.write_text(
        '[client@example.com]\n'
        'client_id = client\n'
        'client_secret =\n'
        'scopes =\n'
        '  spinta_set_meta_fields\n'
        '  spinta_upsert\n'
    )

    data = [
        {
            '_op': 'insert',
            '_type': continent,
            'title': 'Europe',
        },
        {
            '_op': 'insert',
            '_type': continent,
            'title': 'Africa',
        },
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(d) + '\n' for d in data)
    resp = app.post('/', headers=headers, data=payload)
    data = resp.json()
    assert resp.status_code == 200, data
    data = sorted(data['_data'], key=lambda x: x['_id'])

    def remote(request):
        stream = request.body
        resp = app.post('/', headers=request.headers, data=stream)
        return resp.status_code, resp.headers, resp.content

    def auth_token(request):
        _, token = app.headers['Authorization'].split(None, 1)
        return 200, {}, json.dumps({
            'access_token': token,
        })

    target = 'https://example.com/'
    responses.add_callback(
        POST, target,
        callback=remote,
        content_type='application/json',
    )
    responses.add_callback(
        POST, target + 'auth/token',
        callback=auth_token,
        content_type='application/json',
    )
    result = cli.invoke(rc, push, [target, '-r', str(credsfile), '-c', 'client', '-d', 'datasets/backends/postgres/dataset'])
    assert sorted(result.output.splitlines()) == [
        f"datasets/backends/postgres/dataset/continent  {data[0]['_id']}",
        f"datasets/backends/postgres/dataset/continent  {data[1]['_id']}",
    ]
