import operator
import pathlib

from responses import GET


def test_json(store, responses):
    responses.add(
        GET, 'http://example.com/data.json',
        status=200, content_type='application/json; charset=utf-8',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.json').read_bytes(),
    )

    assert len(store.pull('json')) == 10
    assert sorted(store.getall('rinkimai/:source/json'), key=operator.itemgetter('id'))[:2] == [
        {
            'id': '101',
            'pavadinimas': '2015 m. birželio 21 d. pakartotiniai Šilutės rajono savivaldybės tarybos rinkimai\n',
        },
        {
            'id': '102',
            'pavadinimas': '2015 m. birželio 7 d. nauji rinkimai į Lietuvos Respublikos Seimą vienmandatėje Varėnos–Eišiškių rinkimų apygardoje Nr. 70\n',
        },
    ]
