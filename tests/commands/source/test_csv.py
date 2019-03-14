from operator import itemgetter

from responses import GET

from spinta.utils.itertools import consume


def test_csv(store, responses):
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

    assert consume(store.pull('csv')) == 3
    assert consume(store.pull('csv')) == 0

    assert sorted([(x['code'], x['title']) for x in store.getall('country')]) == []
    assert sorted([(x['code'], x['title']) for x in store.getall('country', {'source': 'csv'})]) == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]


def test_denorm(store, responses):
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

    assert consume(store.pull('denorm')) == 5
    assert consume(store.pull('denorm')) == 0

    lt = '552c4c243ec8c98c313255ea9bf16ee286591f8c'
    lv = 'b5dcb86880816fb966cdfbbacd1f3406739464f4'

    assert list(store.getall('country')) == []
    assert sorted([(x['id'], x['title']) for x in store.getall('country', {'source': 'denorm'})]) == [
        (lt, 'Lietuva'),
        (lv, 'Latvija'),
    ]

    assert list(store.getall('org')) == []
    assert sorted([(x['id'], x['title'], x['country']) for x in store.getall('org', {'source': 'denorm'})], key=itemgetter(1)) == [
        ('23fcdb953846e7c709d2967fb549de67d975c010', 'Org1', lt),
        ('6f9f652eb6dae29e4406f1737dd6043af6142090', 'Org2', lt),
        ('11a0764da48b674ce0c09982e7c43002b510d5b5', 'Org3', lv),
    ]
