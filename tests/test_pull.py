import re

from responses import GET

from spinta.utils.refs import get_ref_id


def test_csv(context, responses, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REVISION')

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

    assert len(context.pull('dependencies', models=['continent'])) == 1
    assert len(context.pull('dependencies', models=['country'])) == 1
    assert len(context.pull('dependencies', models=['capital'])) == 1

    assert context.getall('continent', dataset='dependencies', resource='continents') == [
        {
            'type': 'continent/:ds/dependencies/:rs/continents',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'revision': 'REVISION',
            'title': 'Europe',
            'continent_id': '1',
        },
    ]
    assert context.getall('country', dataset='dependencies', resource='continents') == [
        {
            'type': 'country/:ds/dependencies/:rs/continents',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'revision': 'REVISION',
            'title': 'Lithuania',
            'continent': '23fcdb953846e7c709d2967fb549de67d975c010',
            'country_id': '1',
        },
    ]
    assert context.getall('capital', dataset='dependencies', resource='continents') == [
        {
            'type': 'capital/:ds/dependencies/:rs/continents',
            'id': '23fcdb953846e7c709d2967fb549de67d975c010',
            'revision': 'REVISION',
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
            'type': 'continent/:ds/dependencies/:rs/continents',
            'id': get_ref_id('1'),
            'title': 'Europe',
            'continent_id': '1',
        },
    ]
    assert context.getall('continent', dataset='dependencies', resource='continents') == []


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
            'type': 'continent/:ds/generator/:rs/continents',
            'id': '9675e909a3e67c169ed9406e6c21358d46d6d1b1',
            'title': 'Europe',
        },
        {
            'type': 'continent/:ds/generator/:rs/continents',
            'id': '43f647075f78797b76e258206538b40b7b3f52dd',
            'title': 'Europe',
        },
    ]
