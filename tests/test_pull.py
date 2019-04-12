import re

from responses import GET

from spinta.utils.itertools import consume
from spinta.commands import pull, push, getall


def _pull(context, dataset, models=None, push_=True):
    store = context.get('store')
    dataset = store.manifests['default'].objects['dataset'][dataset]
    models = models or []
    data = pull(context, dataset, models=models)
    if push_:
        return push(context, store, data)
    else:
        return data


def _getall(context, dataset, model):
    store = context.get('store')
    backend = store.backends['default']
    model = store.manifests['default'].objects['dataset'][dataset].objects[model]
    return sorted(getall(context, model, backend))


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

    assert consume(_pull(context, 'dependencies', models=['continent'])) == 1
    assert consume(_pull(context, 'dependencies', models=['country'])) == 1
    assert consume(_pull(context, 'dependencies', models=['capital'])) == 1

    assert _getall(context, 'dependencies', 'continent') == [
        {
            'type': 'continent/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Europe',
            'continent_id': '1',
        },
    ]
    assert _getall(context, 'dependencies', 'country') == [
        {
            'type': 'country/:source/dependencies',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'title': 'Lithuania',
            'continent': '23fcdb953846e7c709d2967fb549de67d975c010',
            'country_id': '1',
        },
    ]
    assert _getall(context, 'dependencies', 'capital') == [
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

    assert list(_pull(context, 'dependencies', push_=False)) == [
        {
            'type': 'continent/:source/dependencies',
            'id': '1',
            'title': 'Europe',
            'continent_id': '1',
        },
    ]
    assert _getall(context, 'dependencies', 'continent') == []


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

    assert list(_pull(context, 'generator', push_=False)) == [
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
