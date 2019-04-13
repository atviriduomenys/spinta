import re

from responses import GET

from spinta.utils.itertools import consume


def test_csv(context, responses):
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

    assert consume(context.pull('dependencies', models=['continent'])) == 1
    assert consume(context.pull('dependencies', models=['country'])) == 1
    assert consume(context.pull('dependencies', models=['capital'])) == 1

    assert context.getall('continent', dataset='dependencies') == [
        {
            'type': 'continent/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Europe',
            'continent_id': '1',
        },
    ]
    assert context.getall('country', dataset='dependencies') == [
        {
            'type': 'country/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Lithuania',
            'continent': '23fcdb953846e7c709d2967fb549de67d975c010',
            'country_id': '1',
        },
    ]
    assert context.getall('capital', dataset='dependencies') == [
        {
            'type': 'capital/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Vilnius',
            'country': '23fcdb953846e7c709d2967fb549de67d975c010',
        },
    ]


def test_no_push(context, responses):
    responses.add(
        GET, 'http://example.com/continents.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='id,continent\n1,Europe\n',
    )

    assert list(context.pull('dependencies', push=False)) == [
        {
            'type': 'continent/:source/dependencies',
            'id': '1',
            'title': 'Europe',
            'continent_id': '1',
        },
    ]
    assert context.getall('continent', dataset='dependencies') == []


def test_generator(context, responses):

    def func(request):
        status = 200
        headers = {}
        year = request.url.split('/')[3]
        body = f'id,continent\n{year},Europe\n'
        return status, headers, body

    responses.add_callback(
        GET,
        re.compile(r'http://example.com/\d+/continents.csv'),
        callback=func, content_type='text/plain; charset=utf-8',
    )

    assert list(context.pull('generator', push=False)) == [
        {
            'type': 'continent/:source/generator',
            'id': '2000',
            'title': 'Europe',
        },
        {
            'type': 'continent/:source/generator',
            'id': '2001',
            'title': 'Europe',
        },
    ]
