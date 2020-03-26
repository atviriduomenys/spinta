import json
import pathlib

from responses import GET, POST

from spinta.cli import push


def test_pull(responses, rc, cli, app):
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

    rc = rc.fork({
        'manifests.default': {
            'type': 'inline',
            'manifest': {
                'datasets/example': {
                    'type': 'dataset',
                    'resources': {
                        'countries': {
                            'type': 'csv',
                            'source': 'http://example.com/',
                        },
                    },
                },
                'datasets/example/country': {
                    'type': 'model',
                    'external': {
                        'dataset': 'datasets/example',
                        'resource': 'countries',
                        'source': 'countries.csv',
                        'pkey': 'code',
                    },
                    'properties': {
                        'code': {
                            'type': 'string',
                            'level': 4,
                            'access': 'open',
                            'external': 'kodas',
                        },
                        'title': {
                            'type': 'string',
                            'level': 4,
                            'access': 'open',
                            'external': 'šalis',
                        },
                    },
                },
            },
        },
        'backends.http': {
            'type': 'http',
        },
        'backends.example': {
            'type': 'csv',
            'io': 'http',
        },
    })

    app.authmodel('datasets/example/csv/country', ['getall'])

    assert app.get('/datasets/example/csv/country?select(code,title)&sort(code)').json()['_data'] == [
        ('ee', 'Estija', 'country/:dataset/csv/:resource/countries'),
        ('lt', 'Lietuva', 'country/:dataset/csv/:resource/countries'),
        ('lv', 'Latvija', 'country/:dataset/csv/:resource/countries'),
    ]


def test_push(app, rc, cli, responses, tmpdir):
    continent = 'backends/postgres/continent/:dataset/test'
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
            '_id': 'e6a2ca1e2b50f1a6cd5203f3153162357add5614',
            'title': 'Europe',
        },
        {
            '_op': 'insert',
            '_type': continent,
            '_id': '64f17dc0c56b943ed2835cb35705463c86b39c3d',
            'title': 'Africa',
        },
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(d) + '\n' for d in data)
    resp = app.post('/', headers=headers, data=payload)
    assert resp.status_code == 200

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
    result = cli.invoke(rc, push, [target, '-r', str(credsfile), '-c', 'client', '-d', 'test'])
    assert sorted(result.output.splitlines()) == [
        'backends/postgres/continent/:dataset/test   64f17dc0c56b943ed2835cb35705463c86b39c3d',
        'backends/postgres/continent/:dataset/test   e6a2ca1e2b50f1a6cd5203f3153162357add5614',
    ]
