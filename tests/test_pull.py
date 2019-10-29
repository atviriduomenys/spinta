import re

from responses import GET

from spinta.utils.refs import get_ref_id


def test_csv(app, context, responses):
    responses.add(
        GET, 'http://example.com/continents.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='id,continent,population\n1,Europe,42\n',
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

    assert len(context.pull('dependencies', models=['continent'])) == 1
    assert len(context.pull('dependencies', models=['country'])) == 1
    assert len(context.pull('dependencies', models=['capital'])) == 1

    model = 'continent/:dataset/dependencies/:resource/continents'
    app.authmodel(model, ['getall'])
    data = app.get(f'/{model}').json()
    assert data == {'_data': [
        {
            '_type': model,
            '_id': '23fcdb953846e7c709d2967fb549de67d975c010',
            '_revision': data['_data'][0]['_revision'],
            'title': 'Europe',
            'continent_id': '1',
            'population': 42,
        },
    ]}
    model = 'country/:dataset/dependencies/:resource/continents'
    app.authmodel(model, ['getall'])
    data = app.get(f'/{model}').json()
    assert data == {'_data': [
        {
            '_type': model,
            '_id': '23fcdb953846e7c709d2967fb549de67d975c010',
            '_revision': data['_data'][0]['_revision'],
            'title': 'Lithuania',
            'continent': '23fcdb953846e7c709d2967fb549de67d975c010',
            'country_id': '1',
        },
    ]}
    model = 'capital/:dataset/dependencies/:resource/continents'
    app.authmodel(model, ['getall'])
    data = app.get(f'/{model}').json()
    assert data == {'_data': [
        {
            '_type': model,
            '_id': '23fcdb953846e7c709d2967fb549de67d975c010',
            '_revision': data['_data'][0]['_revision'],
            'title': 'Vilnius',
            'country': '23fcdb953846e7c709d2967fb549de67d975c010',
        },
    ]}


def test_no_push(app, context, responses):
    responses.add(
        GET, 'http://example.com/continents.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='id,continent,population\n1,Europe,42\n',
    )

    id_ = get_ref_id('1')
    assert list(context.pull('dependencies', push=False)) == [
        {
            '_op': 'upsert',
            '_type': 'continent/:dataset/dependencies/:resource/continents',
            '_id': id_,
            '_where': f'_id={id_}',
            'title': 'Europe',
            'continent_id': '1',
            'population': 42,
        },
    ]
    model = 'continent/:dataset/dependencies/:resource/continents'
    app.authmodel(model, ['getall'])
    data = app.get(f'/{model}').json()
    assert data == {'_data': []}


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

    assert context.pull('generator', push=False) == [
        {
            '_op': 'upsert',
            '_type': 'continent/:dataset/generator/:resource/continents',
            '_id': '9675e909a3e67c169ed9406e6c21358d46d6d1b1',
            '_where': '_id=9675e909a3e67c169ed9406e6c21358d46d6d1b1',
            'title': 'Europe',
        },
        {
            '_op': 'upsert',
            '_type': 'continent/:dataset/generator/:resource/continents',
            '_id': '43f647075f78797b76e258206538b40b7b3f52dd',
            '_where': '_id=43f647075f78797b76e258206538b40b7b3f52dd',
            'title': 'Europe',
        },
    ]


def test_convert_integers(app, context, responses):
    responses.add(
        GET, 'http://example.com/continents.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='id,continent,population\n1,Europe,9\n2,Africa,10\n',
    )

    assert len(context.pull('dependencies', models=['continent'])) == 2

    model = 'continent/:dataset/dependencies/:resource/continents'
    app.authmodel(model, ['search'])
    data = app.get(f'/{model}?sort(+_id)').json()['_data']
    assert data == [
        {
            '_id': '23fcdb953846e7c709d2967fb549de67d975c010',
            '_revision': data[0]['_revision'],
            '_type': 'continent/:dataset/dependencies/:resource/continents',
            'continent_id': '1',
            'population': 9,
            'title': 'Europe',
        },
        {
            '_id': '6f9f652eb6dae29e4406f1737dd6043af6142090',
            '_revision': data[1]['_revision'],
            '_type': 'continent/:dataset/dependencies/:resource/continents',
            'continent_id': '2',
            'population': 10,
            'title': 'Africa',
        },
    ]
