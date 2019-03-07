import operator
import pathlib

from responses import GET


def test_xml(store, responses):
    responses.add(
        GET, 'http://example.com/data.xml',
        status=200, content_type='application/xml; charset=utf-8',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.xml').read_bytes(),
        stream=True,
    )

    assert len(store.pull('xml')) == 8
    assert sorted(store.getall('tenure', {'source': 'xml'}), key=operator.itemgetter('id'))[:2] == [
        {
            'type': 'tenure/:source/xml',
            'id': '11a0764da48b674ce0c09982e7c43002b510d5b5',
            'title': '1996–2000 metų kadencija',
            'since': '1996-11-25',
            'until': '2000-10-18',
        },
        {
            'type': 'tenure/:source/xml',
            'id': '1cc7ac9d26603972f6c471a284ff37b9868854d9',
            'title': '2016–2020 metų kadencija',
            'since': '2016-11-14',
            'until': '',
        },
    ]
