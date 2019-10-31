import operator
import pathlib

from responses import GET

from spinta.utils.itertools import consume


def test_json(app, context, responses):
    responses.add(
        GET, 'http://example.com/data.json',
        status=200, content_type='application/json; charset=utf-8',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.json').read_bytes(),
    )

    app.authmodel('rinkimai/:dataset/json/:resource/data', ['getall'])

    data = context.pull('json')
    assert len(data) == 10
    data = {d['_id']: d for d in data}
    assert sorted(app.get('/rinkimai/:dataset/json/:resource/data').json()['_data'], key=operator.itemgetter('_id'))[:2] == [
        {
            '_type': 'rinkimai/:dataset/json/:resource/data',
            '_id': '1b704eeff5f38eeeb84ebcecdddb989e53a4a709',
            '_revision': data['1b704eeff5f38eeeb84ebcecdddb989e53a4a709']['_revision'],
            'pavadinimas': '2017 m. balandžio 23 d. nauji savivaldybių tarybų narių – merų rinkimai '
                           'Jonavos ir Šakių rajonų savivaldybėse',
        },
        {
            '_type': 'rinkimai/:dataset/json/:resource/data',
            '_id': '2a9d25cbf1e8ee6402433d6fc5535f79b5701481',
            '_revision': data['2a9d25cbf1e8ee6402433d6fc5535f79b5701481']['_revision'],
            'pavadinimas': '2017 m. balandžio 23 d. nauji rinkimai į Lietuvos Respublikos Seimą vienmandatėje '
                           'Anykščių-Panevėžio rinkimų apygardoje Nr. 49',
        },
    ]
