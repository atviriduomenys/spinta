from operator import itemgetter

from responses import GET

from spinta.utils.itertools import consume


def test_csv(app, context, responses):
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

    assert len(context.pull('csv')) == 3
    assert len(context.pull('csv')) == 0

    app.authmodel('country', ['getall'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['getall'])

    assert sorted([(x['code'], x['title']) for x in app.get('/country').json()['_data']]) == []
    assert sorted([(x['code'], x['title']) for x in app.get('/country/:dataset/csv/:resource/countries').json()['_data']]) == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_denorm(app, context, responses):
    responses.add(
        GET, 'http://example.com/orgs.csv',
        status=200, content_type='text/plain; charset=utf-8',
        body=(
            'govid,org,kodas,šalis\n'
            '1,Org1,lt,Lietuva\n'
            '2,Org2,lt,Lietuva\n'
            '3,Org3,lv,Latvija'
        ),
    )

    assert consume(context.pull('denorm')) == 5
    assert consume(context.pull('denorm')) == 0

    lt = '552c4c243ec8c98c313255ea9bf16ee286591f8c'
    lv = 'b5dcb86880816fb966cdfbbacd1f3406739464f4'

    app.authmodel('org', ['getall'])
    app.authmodel('org/:dataset/denorm/:resource/orgs', ['getall'])
    app.authmodel('country', ['getall'])
    app.authmodel('country/:dataset/denorm/:resource/orgs', ['getall'])

    assert app.get('country').json()['_data'] == []
    assert sorted([(x['_id'], x['title']) for x in app.get('country/:dataset/denorm/:resource/orgs').json()['_data']]) == [
        (lt, 'Lietuva'),
        (lv, 'Latvija'),
    ]

    assert app.get('/org').json()['_data'] == []
    assert sorted([(x['_id'], x['title'], x['country']) for x in app.get('/org/:dataset/denorm/:resource/orgs').json()['_data']], key=itemgetter(1)) == [
        ('23fcdb953846e7c709d2967fb549de67d975c010', 'Org1', lt),
        ('6f9f652eb6dae29e4406f1737dd6043af6142090', 'Org2', lt),
        ('11a0764da48b674ce0c09982e7c43002b510d5b5', 'Org3', lv),
    ]
