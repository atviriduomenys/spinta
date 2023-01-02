from _pytest.fixtures import FixtureRequest

from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import pushdata
from spinta.testing.manifest import bootstrap_manifest


def test_rdf_get_all_without_namespaces(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
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
    assert res == f'<Rdf>\n'\
                  f'  <City>\n'\
                  f'    <_type>{city1["_type"]}</_type>\n'\
                  f'    <_id about="https://testserver/example/rdf/City/{city1["_id"]}">{city1["_id"]}</_id>\n'\
                  f'    <_revision>{city1["_revision"]}</_revision>\n'\
                  f'    <name>{city1["name"]}</name>\n'\
                  f'    <country>\n'\
                  f'      <_id about="https://testserver/example/rdf/Country/{country["_id"]}">{country["_id"]}</_id>\n'\
                  f'    </country>\n'\
                  f'  </City>\n'\
                  f'  <City>\n'\
                  f'    <_type>{city2["_type"]}</_type>\n'\
                  f'    <_id about="https://testserver/example/rdf/City/{city2["_id"]}">{city2["_id"]}</_id>\n'\
                  f'    <_revision>{city2["_revision"]}</_revision>\n'\
                  f'    <name>{city2["name"]}</name>\n' \
                  f'    <country>\n' \
                  f'      <_id about="https://testserver/example/rdf/Country/{country["_id"]}">{country["_id"]}</_id>\n' \
                  f'    </country>\n' \
                  f'  </City>\n'\
                  f'</Rdf>\n'


def test_rdf_get_all_with_namespaces(
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(rc, '''
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
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
    assert res == f'<rdf:Rdf xmlns:rdf="http://www.rdf.com" xmlns:dcat="http://www.dcat.com" xmlns:dct="http://dct.com">\n'\
                  f'  <dcat:City>\n'\
                  f'    <_type>{city1["_type"]}</_type>\n'\
                  f'    <_id rdf:about="https://testserver/example/rdf/City/{city1["_id"]}">{city1["_id"]}</_id>\n'\
                  f'    <_revision>{city1["_revision"]}</_revision>\n'\
                  f'    <dct:name>{city1["name"]}</dct:name>\n'\
                  f'    <dct:country>\n'\
                  f'      <_id rdf:about="https://testserver/example/rdf/Country/{country["_id"]}">{country["_id"]}</_id>\n'\
                  f'    </dct:country>\n'\
                  f'  </dcat:City>\n'\
                  f'  <dcat:City>\n'\
                  f'    <_type>{city2["_type"]}</_type>\n'\
                  f'    <_id rdf:about="https://testserver/example/rdf/City/{city2["_id"]}">{city2["_id"]}</_id>\n'\
                  f'    <_revision>{city2["_revision"]}</_revision>\n'\
                  f'    <dct:name>{city2["name"]}</dct:name>\n' \
                  f'    <dct:country>\n' \
                  f'      <_id rdf:about="https://testserver/example/rdf/Country/{country["_id"]}">{country["_id"]}</_id>\n' \
                  f'    </dct:country>\n' \
                  f'  </dcat:City>\n'\
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
    assert res == f'<rdf:Rdf xmlns:rdf="http://www.rdf.com" xmlns:dcat="http://www.dcat.com" xmlns:dct="http://dct.com">\n'\
                  f'  <dcat:City>\n'\
                  f'    <_type>{city["_type"]}</_type>\n'\
                  f'    <_id rdf:about="https://testserver/example/rdf/City/{city["_id"]}">{city["_id"]}</_id>\n'\
                  f'    <_revision>{city["_revision"]}</_revision>\n'\
                  f'    <dct:name>{city["name"]}</dct:name>\n'\
                  f'    <dct:country>\n'\
                  f'      <_id rdf:about="https://testserver/example/rdf/Country/{country["_id"]}">{country["_id"]}</_id>\n'\
                  f'    </dct:country>\n'\
                  f'  </dcat:City>\n'\
                  f'</rdf:Rdf>\n'
