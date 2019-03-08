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

    assert len(store.pull('dependencies')) == 3

    assert sorted(store.getall('continent', {'source': 'dependencies'})) == [
        ('1', 'Europe'),
    ]
    assert sorted(store.getall('country', {'source': 'dependencies'})) == [
        ('1', 'Lithuania'),
    ]
    assert sorted(store.getall('capital', {'source': 'dependencies'})) == [
        ('1', 'Vilnius'),
    ]
