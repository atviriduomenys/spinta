import json
import pathlib

from responses import GET, POST

from spinta.cli import main


def test_pull(responses, cli, app, mocker, event_loop):
    mocker.patch('asyncio.run', event_loop.run_until_complete)

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

    result = cli.invoke(main, ['pull', 'csv'], catch_exceptions=False)
    assert result.output == (
        '\n'
        '\n'
        'Table: country/:dataset/csv/:resource/countries\n'
        '                  _id                      code    title     _op                        _where                   \n'
        '=================================================================================================================\n'
        '552c4c243ec8c98c313255ea9bf16ee286591f8c   lt     Lietuva   upsert   _id=552c4c243ec8c98c313255ea9bf16ee286591f8c\n'
        'b5dcb86880816fb966cdfbbacd1f3406739464f4   lv     Latvija   upsert   _id=b5dcb86880816fb966cdfbbacd1f3406739464f4\n'
        '68de1c04d49aeefabb7081a5baf81c055f235be3   ee     Estija    upsert   _id=68de1c04d49aeefabb7081a5baf81c055f235be3'
    )

    app.authmodel('country/:dataset/csv/:resource/countries', ['getall'])

    assert app.get('/country/:dataset/csv/:resource/countries').json() == {
        '_data': [],
    }

    result = cli.invoke(main, ['pull', 'csv', '--push'], catch_exceptions=False)
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


def test_push(app, cli, responses, tmpdir):
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
            '_id': 'a697ab05-3184-40a7-a7ba-e1da614ace5e',
            'title': 'Europe',
        },
        {
            '_op': 'insert',
            '_type': continent,
            '_id': 'e51aa67b-2297-4fe3-b54f-0d9240de5886',
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
    result = cli.invoke(main, ['push', target, '-r', str(credsfile), '-c', 'client', '-d', 'test'], catch_exceptions=False)
    assert sorted(result.output.splitlines()) == [
        'backends/postgres/continent/:dataset/test   a697ab05-3184-40a7-a7ba-e1da614ace5e',
        'backends/postgres/continent/:dataset/test   e51aa67b-2297-4fe3-b54f-0d9240de5886',
    ]
