import operator
import pathlib

from responses import GET

from spinta.utils.itertools import consume


def test_json(context, responses, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REV')

    responses.add(
        GET, 'http://example.com/data.json',
        status=200, content_type='application/json; charset=utf-8',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.json').read_bytes(),
    )

    assert consume(context.pull('json')) == 10
    assert sorted(context.getall('rinkimai', dataset='json', resource='data'), key=operator.itemgetter('id'))[:2] == [
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': '1b704eeff5f38eeeb84ebcecdddb989e53a4a709',
            'revision': 'REV',
            'pavadinimas': '2017 m. balandžio 23 d. nauji savivaldybių tarybų narių – merų rinkimai '
                           'Jonavos ir Šakių rajonų savivaldybėse',
        },
        {
            'type': 'rinkimai/:ds/json/:rs/data',
            'id': '2a9d25cbf1e8ee6402433d6fc5535f79b5701481',
            'revision': 'REV',
            'pavadinimas': '2017 m. balandžio 23 d. nauji rinkimai į Lietuvos Respublikos Seimą vienmandatėje '
                           'Anykščių-Panevėžio rinkimų apygardoje Nr. 49',
        },
    ]
