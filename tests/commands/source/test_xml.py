import operator
import pathlib

from responses import GET


def test_xml(app, context, responses):
    responses.add(
        GET, 'http://example.com/data.xml',
        status=200, content_type='application/xml; charset=utf-8',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.xml').read_bytes(),
        stream=True,
    )

    app.authmodel('tenure/:dataset/xml/:resource/data', ['getall'])

    data = context.pull('xml')
    assert len(data) == 8
    data = {d['_id']: d for d in data}
    assert sorted(app.getdata('/tenure/:dataset/xml/:resource/data'), key=operator.itemgetter('_id'))[:2] == [
        {
            '_type': 'tenure/:dataset/xml/:resource/data',
            '_id': '11a0764da48b674ce0c09982e7c43002b510d5b5',
            '_revision': data['11a0764da48b674ce0c09982e7c43002b510d5b5']['_revision'],
            'title': '1996–2000 metų kadencija',
            'since': '1996-11-25',
            'until': '2000-10-18',
        },
        {
            '_type': 'tenure/:dataset/xml/:resource/data',
            '_id': '1cc7ac9d26603972f6c471a284ff37b9868854d9',
            '_revision': data['1cc7ac9d26603972f6c471a284ff37b9868854d9']['_revision'],
            'title': '2016–2020 metų kadencija',
            'since': '2016-11-14',
            'until': '',
        },
    ]
