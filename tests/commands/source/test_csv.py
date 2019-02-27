from responses import GET


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

    assert len(store.pull('csv')) == 3
    assert len(store.pull('csv')) == 3

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

    assert len(store.pull('denorm')) == 6
    assert len(store.pull('denorm')) == 6

    assert list(store.getall('country')) == []
    assert sorted([(x['id'], x['title']) for x in store.getall('country', {'source': 'denorm'})]) == [
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]

    assert list(store.getall('org')) == []
    assert sorted([(x['id'], x['title'], x['country']) for x in store.getall('org', {'source': 'denorm'})]) == [
        ('1', 'Org1', 'lt'),
        ('2', 'Org2', 'lt'),
        ('3', 'Org3', 'lv'),
    ]
