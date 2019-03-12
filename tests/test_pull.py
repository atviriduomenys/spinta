from responses import GET


def test_csv(store, responses):
    responses.add(
        GET, 'http://example.com/continents.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='id,continent\n1,Europe\n',
    )

    responses.add(
        GET, 'http://example.com/continents/1/countries.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='id,country\n1,Lithuania\n',
    )

    responses.add(
        GET, 'http://example.com/continents/1/countries/1/captials.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='id,capital\n1,Vilnius\n',
    )

    assert len(store.pull('dependencies', {'models': ['continent']})) == 1
    assert len(store.pull('dependencies', {'models': ['country']})) == 1
    assert len(store.pull('dependencies', {'models': ['capital']})) == 1

    assert sorted(store.getall('continent', {'source': 'dependencies'})) == [
        {
            'type': 'continent/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Europe',
            'continent_id': '1',
        },
    ]
    assert sorted(store.getall('country', {'source': 'dependencies'})) == [
        {
            'type': 'country/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Lithuania',
            'continent': '23fcdb953846e7c709d2967fb549de67d975c010',
            'country_id': '1',
        },
    ]
    assert sorted(store.getall('capital', {'source': 'dependencies'})) == [
        {
            'type': 'capital/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Vilnius',
            'country': '23fcdb953846e7c709d2967fb549de67d975c010',
        },
    ]
