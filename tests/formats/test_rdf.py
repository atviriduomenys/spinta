import base64

from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import pushdata, encode_page_values_manually
from spinta.testing.manifest import bootstrap_manifest


def test_rdf_get_all_without_uri(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   | Country      |        | name    |         | 
      |   |   |   | name     | string |         | open    | 
      |   |   | City         |        | name    |         | 
      |   |   |   | name     | string |         | open    |
      |   |   |   | country  | ref    | Country | open    | 
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/rdf', ['insert', 'getall'])

    country = pushdata(app, '/example/rdf/Country', {'name': 'Lithuania'})
    city1 = pushdata(app, '/example/rdf/City', {
        'name': 'Vilnius',
        'country': {'_id': country['_id']},
    })
    city2 = pushdata(app, '/example/rdf/City', {
        'name': 'Kaunas',
        'country': {'_id': country['_id']},
    })

    res = app.get("/example/rdf/City/:format/rdf").text
    assert res == f'<?xml version="1.0" encoding="UTF-8"?>\n'\
                  f'<rdf:Rdf\n' \
                  f' xmlns:rdf="http://www.rdf.com"\n' \
                  f' xmlns:pav="http://purl.org/pav/"\n' \
                  f' xml:base="https://testserver/">\n'\
                  f'<rdf:Description rdf:about="/example/rdf/City/{city1["_id"]}" rdf:type="example/rdf/City" ' \
                  f'pav:version="{city1["_revision"]}">\n' \
                  f'  <_page>{encode_page_values_manually({ "name": city1["name"], "_id": city1["_id"]})}</_page>\n' \
                  f'  <name>{city1["name"]}</name>\n' \
                  f'  <country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n' \
                  f'</rdf:Description>\n'\
                  f'<rdf:Description rdf:about="/example/rdf/City/{city2["_id"]}" rdf:type="example/rdf/City" ' \
                  f'pav:version="{city2["_revision"]}">\n' \
                  f'  <_page>{encode_page_values_manually({ "name": city2["name"], "_id": city2["_id"]})}</_page>\n' \
                  f'  <name>{city2["name"]}</name>\n' \
                  f'  <country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n' \
                  f'</rdf:Description>\n'\
                  f'</rdf:Rdf>\n'


def test_rdf_get_all_with_uri(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | dct:country
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/rdf', ['insert', 'getall'])

    country = pushdata(app, '/example/rdf/Country', {'name': 'Lithuania'})
    city1 = pushdata(app, '/example/rdf/City', {
        'name': 'Vilnius',
        'country': {'_id': country['_id']},
    })
    city2 = pushdata(app, '/example/rdf/City', {
        'name': 'Kaunas',
        'country': {'_id': country['_id']},
    })

    res = app.get("/example/rdf/City/:format/rdf").text
    assert res == f'<?xml version="1.0" encoding="UTF-8"?>\n'\
                  f'<rdf:Rdf\n' \
                  f' xmlns:rdf="http://www.rdf.com"\n' \
                  f' xmlns:pav="http://purl.org/pav/"\n' \
                  f' xmlns:dcat="http://www.dcat.com"\n' \
                  f' xmlns:dct="http://dct.com"\n' \
                  f' xml:base="https://testserver/">\n' \
                  f'<dcat:city rdf:about="/example/rdf/City/{city1["_id"]}" rdf:type="example/rdf/City" ' \
                  f'pav:version="{city1["_revision"]}">\n' \
                  f'  <dct:_page>{encode_page_values_manually({"name": city1["name"], "_id": city1["_id"]})}</dct:_page>\n' \
                  f'  <dct:name>{city1["name"]}</dct:name>\n'\
                  f'  <dct:country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n' \
                  f'</dcat:city>\n'\
                  f'<dcat:city rdf:about="/example/rdf/City/{city2["_id"]}" rdf:type="example/rdf/City" ' \
                  f'pav:version="{city2["_revision"]}">\n' \
                  f'  <dct:_page>{encode_page_values_manually({"name": city2["name"], "_id": city2["_id"]})}</dct:_page>\n' \
                  f'  <dct:name>{city2["name"]}</dct:name>\n' \
                  f'  <dct:country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n' \
                  f'</dcat:city>\n'\
                  f'</rdf:Rdf>\n'


def test_rdf_get_one(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | dct:country
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/rdf', ['insert', 'getone'])

    country = pushdata(app, '/example/rdf/Country', {'name': 'Lithuania'})
    city = pushdata(app, '/example/rdf/City', {
        'name': 'Vilnius',
        'country': {'_id': country['_id']},
    })

    res = app.get(f"/example/rdf/City/{city['_id']}/:format/rdf").text
    assert res == f'<?xml version="1.0" encoding="UTF-8"?>\n'\
                  f'<rdf:Rdf\n' \
                  f' xmlns:rdf="http://www.rdf.com"\n' \
                  f' xmlns:pav="http://purl.org/pav/"\n' \
                  f' xmlns:dcat="http://www.dcat.com"\n' \
                  f' xmlns:dct="http://dct.com"\n' \
                  f' xml:base="https://testserver/">\n' \
                  f'<dcat:city rdf:about="/example/rdf/City/{city["_id"]}" rdf:type="example/rdf/City" ' \
                  f'pav:version="{city["_revision"]}">\n' \
                  f'  <dct:_page>{encode_page_values_manually({"name": city["name"], "_id": city["_id"]})}</dct:_page>\n' \
                  f'  <dct:name>{city["name"]}</dct:name>\n'\
                  f'  <dct:country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n' \
                  f'</dcat:city>\n'\
                  f'</rdf:Rdf>\n'


def test_rdf_with_file(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf/file         |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | flag     | file   |         | open    | dct:flag
    ''', backend=postgresql, request=request)
    app = create_test_client(context)
    app.authmodel('example/rdf/file', ['insert', 'getone'])

    country = pushdata(app, '/example/rdf/file/Country', {
        'name': 'Lithuania',
        'flag': {
            '_id': 'file.txt',
            '_content_type': 'text/plain',
            '_content': base64.b64encode(b'DATA').decode(),
        },
    })

    res = app.get(f"/example/rdf/file/Country/{country['_id']}/:format/rdf").text
    assert res == f'<?xml version="1.0" encoding="UTF-8"?>\n'\
                  f'<rdf:Rdf\n' \
                  f' xmlns:rdf="http://www.rdf.com"\n' \
                  f' xmlns:pav="http://purl.org/pav/"\n' \
                  f' xmlns:dcat="http://www.dcat.com"\n' \
                  f' xmlns:dct="http://dct.com"\n' \
                  f' xml:base="https://testserver/">\n' \
                  f'<dcat:country rdf:about="/example/rdf/file/Country/{country["_id"]}" ' \
                  f'rdf:type="example/rdf/file/Country" ' \
                  f'pav:version="{country["_revision"]}">\n' \
                  f'  <dct:_page>{encode_page_values_manually({"name": country["name"], "_id": country["_id"]})}</dct:_page>\n' \
                  f'  <dct:name>{country["name"]}</dct:name>\n'\
                  f'  <dct:flag rdf:about="/example/rdf/file/Country/{country["_id"]}/flag"/>\n' \
                  f'</dcat:country>\n'\
                  f'</rdf:Rdf>\n'
