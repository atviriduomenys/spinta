import json
from pathlib import Path

from responses import RequestsMock, POST

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest
from spinta.testing.utils import get_error_codes, get_error_context


def test_xml_read(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city>
                <code>lt</code>
                <name>Vilnius</name>
            </city>
            <city>
                <code>lv</code>
                <name>Ryga</name>
            </city>
            <city>
                <code>ee</code>
                <name>Talin</name>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source       | access
    example/xml              |          |      |              |
      | xml                  | dask/xml |      | {path}       |
      |   |   | City         |          | name | /cities/city |
      |   |   |   | name     | string   |      | name         | open
      |   |   |   | country  | string   |      | code         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_with_attributes(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city code="lt" name="Vilnius"/>
            <city code="lv" name="Ryga"/>
            <city code="ee" name="Talin"/>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source       | access
    example/xml              |          |      |              |
      | xml                  | dask/xml |      | {path}       |
      |   |   | City         |          | name | /cities/city |
      |   |   |   | name     | string   |      | @name        | open
      |   |   |   | country  | string   |      | @code        | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_refs_level_3(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source                         | access | level
       example/xml              |          |         |                                |        |
         | xml                  | dask/xml |         | {path}                         |        |
         |   |   | City         |          | name    | /countries/country/cities/city |        |
         |   |   |   | name     | string   |         | @name                          | open   |
         |   |   |   | code     | ref      | Country | ../../code                     | open   | 3
                     |  
         |   |   | Country      |          | country | /countries/country             |        |
         |   |   |   | name     | string   |         | name                           | open   |
         |   |   |   | country  | string   |         | code                           | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_refs_level_4(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source                         | access | level
       example/xml              |          |         |                                |        |
         | xml                  | dask/xml |         | {path}                         |        |
         |   |   | City         |          | name    | /countries/country/cities/city |        |
         |   |   |   | name     | string   |         | @name                          | open   |
         |   |   |   | code     | ref      | Country | ../..                          | open   | 4
                     |  
         |   |   | Country      |          | country | /countries/country             |        |
         |   |   |   | name     | string   |         | name                           | open   |
         |   |   |   | country  | string   |         | code                           | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/Country")
    countries = {c["country"]: c["_id"] for c in listdata(resp, "_id", "country", full=True)}
    assert sorted(countries) == ["ee", "lt", "lv"]

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        (countries["lt"], "Vilnius"),
        (countries["lv"], "Ryga"),
        (countries["ee"], "Talin"),
    ]


def test_xml_read_multiple_sources(rc: RawConfig, tmp_path: Path):
    xml0 = """
        <cities>
            <city name="Vilnius" code="lt"/>
            <city name="Ryga" code="lv"/>
            <city name="Talin" code="ee"/>
        </cities>
    """
    path0 = tmp_path / "cities.xml"
    path0.write_text(xml0)

    xml1 = """
        <countries>
            <country code="lt" name="Lietuva"/>
            <country code="lv" name="Latvija"/>
            <country code="ee" name="Estija"/>
        </countries>
    """
    path1 = tmp_path / "countries.xml"
    path1.write_text(xml1)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source             | access | level
       example/xml              |          |         |                    |        |
         | xml_city             | dask/xml |         | {path0}            |        |
         |   |   | City         |          | name    | /cities/city       |        |
         |   |   |   | name     | string   |         | @name              | open   |
         |   |   |   | code     | string   |         | @code              | open   | 3
                     |  
         | xml_country          | dask/xml |         | {path1}            |        |
         |   |   | Country      |          | code    | /countries/country |        |
         |   |   |   | name     | string   |         | @name              | open   |
         |   |   |   | code     | string   |         | @code              | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
        ("ee", "Estija"),
    ]


def test_xml_read_with_empty(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country code="lt"/>
            <country code="lv" name="Latvija"/>
            <country name="Estija"/>
        </countries>"""
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type     | ref  | source               
           example/xml                |          |      |                    
             | resource               | dask/xml |      | {path}             
                                      |          |      |                    
             |   | Country |          |          | code | /countries/country 
             |   |         | name     | string   |      | @name              
             |   |         | code     | string   |      | @code              
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Country", ["getall"])

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None),
        ("lv", "Latvija"),
        (None, "Estija"),
    ]


def test_xml_read_with_empty_nested(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country code="lt" name="Lietuva"/>
            <country name="Latvija">
                <location>
                    <lon>3</lon>
                </location>
            </country>
            <country code="ee">
                <location>
                    <lon>5</lon>
                    <lat>4</lat>
                </location>
            </country>
        </countries>"""
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property  | type     | ref  | source                  
           example/xml                 |          |      |                   
             | resource                | dask/xml |      | {path}            
                                       |          |      |                   
             |   | Country |           |          | code | /countries/country
             |   |         | name      | string   |      | @name             
             |   |         | code      | string   |      | @code             
             |   |         | latitude  | integer  |      | location/lat      
             |   |         | longitude | integer  |      | location/lon      
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Country", ["getall"])

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None, None, "Lietuva"),
        (None, None, 3, "Latvija"),
        ("ee", 4, 5, None),
    ]


def test_xml_read_parametrize_simple_iterate_pages(rc: RawConfig, tmp_path: Path):
    page_count = 10
    for i in range(1, page_count):
        current_page_file = tmp_path / f"page{i - 1}.xml"
        xml_manifest = f"""
        <pages name="Page {i}">
            <next>{str(tmp_path / f"page{i}.xml") if i != page_count - 1 else ""}</next>
        </pages>
        """
        current_page_file.write_text(xml_manifest)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source                   | prepare     | access
    example/xml              |          |      |                          |             |
      | resource             | dask/xml |      | {{path}}                 |             |
      |   |   |              | param    | path | {tmp_path / "page0.xml"} |             |
      |   |   |              |          |      | Page                     | read().next |
      |   |   | Page         |          | name | ../pages                 |             |
      |   |   |   | name     | string   |      | @name                    |             | open
      |   |   |   | next     | uri      |      | next/text()              |             | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Page", ["getall"])

    resp = app.get("/example/xml/Page")
    assert listdata(resp, "name", sort=False) == [
        "Page 1",
        "Page 2",
        "Page 3",
        "Page 4",
        "Page 5",
        "Page 6",
        "Page 7",
        "Page 8",
        "Page 9",
    ]


def test_xml_read_from_different_resource_property(rc: RawConfig, tmp_path: Path, responses: RequestsMock):
    endpoint_url = "http://example.com/city"
    soap_response = """
        <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="city_app">
            <ns0:Body>
                <ns1:CityOutputResponse>
                    <ns1:CityOutput>
                        <ns1:id>1</ns1:id>
                        <ns1:name><![CDATA[<names><nameData><name>Vilnius</name><founded>1387</founded></nameData></names>]]></ns1:name>
                    </ns1:CityOutput>
                    <ns1:CityOutput>
                        <ns1:id>2</ns1:id>
                        <ns1:name><![CDATA[<names><nameData><name>Kaunas</name><founded>1408</founded></nameData></names>]]></ns1:name>
                    </ns1:CityOutput>
                </ns1:CityOutputResponse>
            </ns0:Body>
        </ns0:Envelope>
    """
    responses.add(POST, endpoint_url, status=200, content_type="text/plain; charset=utf-8", body=soap_response)

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type     | ref        | source                                          | access | prepare
        example                  | dataset  |            |                                                 |        |
          | wsdl_resource        | wsdl     |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap     |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param    | parameter1 | request_model/param1                            |        | input('default_val')
          |   |   |   |          | param    | parameter2 | request_model/param2                            |        | input('default_val')
          |   |   | City         |          |            | /                                               | open   |
          |   |   |   | name     | string   |            | name                                            |        |
          |   |   |   | p1       | integer  |            |                                                 |        | param(parameter1)
          |   |   |   | p2       | integer  |            |                                                 |        | param(parameter2)
          | xml_resource         | dask/xml |            |                                                 |        | eval(param(nested_xml))
          |   |   |   |          | param    | nested_xml | City                                            |        | read().name
          |   |   | Name         |          |            | names/nameData                                  | open   |
          |   |   |   | name     | string   |            | name                                            |        |
          |   |   |   | since    | integer  |            | founded                                         |        |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Name", ["getall"])
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/Name")
    assert listdata(resp, sort=False) == [
        ("Vilnius", 1387),
        ("Kaunas", 1408),
    ]


def test_xml_read_from_different_dataset_resource(rc: RawConfig, tmp_path: Path, responses: RequestsMock):
    endpoint_url = "http://example.com/city"
    soap_response = """
        <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="city_app">
            <ns0:Body>
                <ns1:CityOutputResponse>
                    <ns1:CityOutput>
                        <ns1:id>1</ns1:id>
                        <ns1:name><![CDATA[<names><nameData><name>Vilnius</name><founded>1387</founded></nameData></names>]]></ns1:name>
                    </ns1:CityOutput>
                    <ns1:CityOutput>
                        <ns1:id>2</ns1:id>
                        <ns1:name><![CDATA[<names><nameData><name>Kaunas</name><founded>1408</founded></nameData></names>]]></ns1:name>
                    </ns1:CityOutput>
                </ns1:CityOutputResponse>
            </ns0:Body>
        </ns0:Envelope>
    """
    responses.add(POST, endpoint_url, status=200, content_type="text/plain; charset=utf-8", body=soap_response)

    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type     | ref        | source                                          | access | prepare
        example                  | dataset  |            |                                                 |        |
          | wsdl_resource        | wsdl     |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap     |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param    | parameter1 | request_model/param1                            |        | input('default_val')
          |   |   |   |          | param    | parameter2 | request_model/param2                            |        | input('default_val')
          |   |   | City         |          |            | /                                               | open   |
          |   |   |   | name     | string   |            | name                                            |        |
          |   |   |   | p1       | integer  |            |                                                 |        | param(parameter1)
          |   |   |   | p2       | integer  |            |                                                 |        | param(parameter2)
        example2                 | dataset  |            |                                                 |        |
          | xml_resource         | dask/xml |            |                                                 |        | eval(param(nested_xml))
          |   |   |   |          | param    | nested_xml | example/City                                    |        | read().name
          |   |   | Name         |          |            | names/nameData                                  | open   |
          |   |   |   | name     | string   |            | name                                            |        |
          |   |   |   | since    | integer  |            | founded                                         |        |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example2/Name", ["getall"])
    app.authmodel("example/City", ["getall"])

    resp = app.get("example2/Name")
    assert listdata(resp, sort=False) == [
        ("Vilnius", 1387),
        ("Kaunas", 1408),
    ]


def test_xml_read_from_different_resource_property_can_use_same_url_parameters_on_both_reads(
    rc: RawConfig, tmp_path: Path, responses: RequestsMock
):
    endpoint_url = "http://example.com/city"
    soap_response = """
        <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="city_app">
            <ns0:Body>
                <ns1:CityOutputResponse>
                    <ns1:CityOutput>
                        <ns1:id>1</ns1:id>
                        <ns1:name><![CDATA[<names><nameData><title>Vilnius</title><founded>1387</founded></nameData></names>]]></ns1:name>
                    </ns1:CityOutput>
                    <ns1:CityOutput>
                        <ns1:id>2</ns1:id>
                        <ns1:name><![CDATA[<names><nameData><title>Kaunas</title><founded>1408</founded></nameData></names>]]></ns1:name>
                    </ns1:CityOutput>
                </ns1:CityOutputResponse>
            </ns0:Body>
        </ns0:Envelope>
    """
    responses.add(POST, endpoint_url, status=200, content_type="text/plain; charset=utf-8", body=soap_response)

    wsdl_file = "tests/datasets/backends/wsdl/data/required_param_wsdl.xml"
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | ref        | source                                          | access | prepare
        example                  | dataset  |            |                                                 |        |
          | wsdl_resource        | wsdl     |            | {wsdl_file}                                     |        |
          | soap_resource        | soap     |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param    | parameter1 | request_model/param1                            |        | input()
          |   |   |   |          | param    | parameter2 | request_model/param2                            |        | input()
          |   |   | City         |          |            | /                                               | open   |
          |   |   |   | name     | string   |            | name                                            |        |
          |   |   |   | p1       | integer  |            |                                                 |        | param(parameter1)
          |   |   |   | p2       | integer  |            |                                                 |        | param(parameter2)
          | xml_resource         | dask/xml |            |                                                 |        | eval(param(nested_xml))
          |   |   |   |          | param    | nested_xml | City                                            |        | read().name
          |   |   | Name         |          |            | names/nameData                                  | open   |
          |   |   |   | title    | string   |            | title                                           |        |
          |   |   |   | since    | integer  |            | founded                                         |        |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Name", ["getall", "search"])
    app.authmodel("example/City", ["getall", "search"])

    resp = app.get("/example/Name?p1='foo'&p2='bar'&title='Vilnius'")
    assert listdata(resp, sort=False) == [(1387, "Vilnius")]


def test_xml_read_raise_error_if_neither_resource_source_nor_prepare_given(rc: RawConfig, tmp_path: Path):
    context, manifest = prepare_manifest(
        rc,
        """
        d | r | b | m | property | type     | ref        | source                                          | access | prepare
        example                  | dataset  |            |                                                 |        |
          | wsdl_resource        | wsdl     |            | tests/datasets/backends/wsdl/data/wsdl.xml      |        |
          | soap_resource        | soap     |            | CityService.CityPort.CityPortType.CityOperation |        | wsdl(wsdl_resource)
          |   |   |   |          | param    | parameter1 | request_model/param1                            |        | input('default_val')
          |   |   |   |          | param    | parameter2 | request_model/param2                            |        | input('default_val')
          |   |   | City         |          |            | /                                               | open   |
          |   |   |   | name     | string   |            | name                                            |        |
          |   |   |   | p1       | integer  |            |                                                 |        | param(parameter1)
          |   |   |   | p2       | integer  |            |                                                 |        | param(parameter2)
          | xml_resource         | dask/xml |            |                                                 |        | 
          |   |   |   |          | param    | nested_xml | City                                            |        | read().name
          |   |   | Name         |          |            | names/nameData                                  | open   |
          |   |   |   | name     | string   |            | name                                            |        |
          |   |   |   | since    | integer  |            | founded                                         |        |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Name", ["getall"])
    app.authmodel("example/City", ["getall"])

    response = app.get("/example/Name")
    assert response.status_code == 500
    assert get_error_codes(response.json()) == ["CannotReadResource"]
    assert get_error_context(response.json(), "CannotReadResource", ["resource"]) == {"resource": "xml_resource"}


def test_xml_read_from_different_resource_property_with_iterate_pages(rc: RawConfig, tmp_path: Path):
    page1_file = tmp_path / "page1.json"
    page1_file.write_text(
        json.dumps(
            {
                "page": {
                    "next": str(tmp_path / "page2.json"),
                    "name": (
                        "<names><nameData><name>Vilnius</name><founded>1387</founded></nameData>"
                        "<nameData><name>Kaunas</name><founded>1408</founded></nameData></names>"
                    ),
                }
            }
        )
    )
    page2_file = tmp_path / "page2.json"
    page2_file.write_text(
        json.dumps(
            {
                "page": {
                    "next": None,
                    "name": (
                        "<names><nameData><name>Vilnius</name><founded>1387</founded></nameData>"
                        "<nameData><name>Kaunas</name><founded>1408</founded></nameData></names>"
                    ),
                }
            }
        )
    )

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type      | ref        | source                    | prepare                 | access
        example                  |           |            |                           |                         |
          | json_resource        | dask/json |            | {{path}}                  |                         |
          |   |   |              | param     | path       | {tmp_path / "page1.json"} |                         |
          |   |   |              |           |            | Page                      | read().next             |
          |   |   | Page         |           | name       | page                      |                         |
          |   |   |   | name     | string    |            | name                      |                         | open
          |   |   |   | next     | uri       |            | next                      |                         | open
          | xml_resource         | dask/xml  |            |                           | eval(param(nested_xml)) | 
          |   |   |   |          | param     | nested_xml | Page                      | read().name             | 
          |   |   | Name         |           |            | names/nameData            |                         | open
          |   |   |   | name     | string    |            | name                      |                         |
          |   |   |   | since    | integer   |            | founded                   |                         |          
        """,
        mode=Mode.external,
    )

    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Page", ["getall"])
    app.authmodel("example/Name", ["getall"])

    resp = app.get("/example/Name")
    assert listdata(resp, sort=False) == [
        ("Vilnius", 1387),
        ("Kaunas", 1408),
        ("Vilnius", 1387),
        ("Kaunas", 1408),
    ]


def test_xml_read_filters_results_if_url_query_parameter_is_property_without_prepare(rc: RawConfig, tmp_path: Path):
    xml = """
        <names>
            <nameData>
                <name>Vilnius</name>
                <founded>1387</founded>
            </nameData>
            <nameData>
                <name>Kaunas</name>
                <founded>1408</founded>
            </nameData>
        </names>
    """
    path = tmp_path / "names.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | m | property | type     | source
        example/xml          |          |
          | resource         | dask/xml | {path}
          |   |              |          |
          |   | Names        |          | /names/nameData
          |   |   | name     | string   | name
          |   |   | founded  | integer  | founded
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Names", ["getall", "search"])

    resp = app.get("/example/xml/Names?name='Vilnius'")
    assert listdata(resp, sort=False) == [(1387, "Vilnius")]


def test_xml_read_error_if_backend_cannot_parse_data(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "names.xml"
    path.write_text('[{"name": "Vilnius", "founded": 1387},{"name": "Kaunas", "founded": 1408}]')

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | m | property | type     | source
        example/xml          |          |
          | resource         | dask/xml | {path}
          |   |              |          |
          |   | Names        |          | /names/nameData
          |   |   | name     | string   | name
          |   |   | founded  | integer  | founded
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Names", ["getall"])

    response = app.get("/example/xml/Names")
    assert response.status_code == 500
    assert get_error_codes(response.json()) == ["UnexpectedErrorReadingData"]


# XML with planets, countries, cities and streets for testing multiple refs
xml_cities = """
    <planets>
        <planet>
            <id>1</id>
            <code>er</code>
            <name>Earth</name>
            <countries>
                <country>
                    <id>2</id>
                    <code>lt</code>
                    <name>Lietuva</name>
                    <cities>
                        <city name="Vilnius">
                            <id>4</id>
                            <streets>
                                <street name="Gedimino pr." />
                            </streets>
                        </city>
                    </cities>
                </country>
                <country>
                    <id>3</id>
                    <code>lv</code>
                    <name>Latvija</name>
                    <cities>
                        <city name="Ryga">
                            <id>5</id>
                            <streets>
                                <street name="Elizabetes" />
                            </streets>
                        </city>
                    </cities>
                </country>
                <country>
                    <id>6</id>
                    <code>ee</code>
                    <name>Estija</name>
                    <cities>
                        <city name="Talin">
                            <id>7</id>
                            <streets>
                                <street name="Narva" />
                            </streets>
                        </city>
                    </cities>
                </country>
            </countries>
        </planet>
    </planets>
"""


def test_xml_read_many_refs_0(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "cities.xml"
    path.write_text(xml_cities)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref     | source                                                           | prepare          | access
    example/xml              |          |         |                                                                  |                  |
      | data                 | dask/xml |         | {path}                                                           |                  |
      |   |                  |          |         |                                                                  |                  |
      |   |   | Planet       |          | id      | /planets/planet                                                  |                  |
      |   |   |   | id       | string   |         | id                                                               |                  | open
      |   |   |   | code     | string   |         | code                                                             |                  | open
      |   |   |   | name     | string   |         | name                                                             |                  | open
      |   |                  |          |         |                                                                  |                  |
      |   |   | Country      |          | id      | /planets/planet/countries/country                                |                  |
      |   |   |   | id       | string   |         | id                                                               |                  | open      
      |   |   |   | code     | string   |         |                                                                  |                  | open
      |   |   |   | name     | string   |         | name                                                             |                  | open
      |   |   |   | planet   | ref      | Planet  | ../../id                                                         |                  | open
      |   |                  |          |         |                                                                  |                  |
      |   |   | City         |          | id      | /planets/planet/countries/country/cities/city                    |                  |
      |   |   |   | id       | string   |         | id                                                               |                  | open
      |   |   |   | name     | string   |         | @name                                                            |                  | open
      |   |   |   | country  | ref      | Country | ../../id                                                         |                  | open
      |   |   |   | planet   | ref      | Planet  | ../../../../id                                                   |                  | open
      |   |                  |          |         |                                                                  |                  |
      |   |   | Street       |          | name    | /planets/planet/countries/country/cities/city/streets/street     |                  |
      |   |   |   | name     | string   |         | @name                                                            |                  | open
      |   |   |   | city     | ref      | City    | ../../id                                                         |                  | open
      |   |   |   | country  | ref      | Country | ../../../../id                                                   |                  | open
      |   |   |   | planet   | ref      | Planet  | /planets/planet/id                                               |                  | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    config = context.get("config")
    assert config.check_ref_filters is True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/Street")
    assert resp.status_code == 200


def test_xml_read_text_lang_get_all(rc: RawConfig, tmp_path: Path):
    xml = """
    <miestai>
        <miestas>
            <pavadinimas>Kaunas</pavadinimas>
            <kodas>KNS</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Vilnius</pavadinimas>
            <kodas>VNO</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Klaipėda</pavadinimas>
            <kodas>KLJ</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Šiauliai</pavadinimas>
            <kodas>SQQ</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Panevėžys</pavadinimas>
            <kodas>PNV</kodas>
        </miestas>
    </miestai>
    """
    path = tmp_path / "miestai.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property   | type     | ref  | source            | access
    example/xml                |          |      |                   |
      | xml                    | dask/xml |      | {path}            |
      |   |   | City           |          | code | /miestai/miestas  |
      |   |   |   | name@lt    | string   |      | pavadinimas/text()| open
      |   |   |   | code       | string   |      | kodas/text()      | open
      |   |   |   | population | integer  |      | popul/text()      | open      
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, "code", "name", sort=False) == [
        ("KNS", "Kaunas"),
        ("VNO", "Vilnius"),
        ("KLJ", "Klaipėda"),
        ("SQQ", "Šiauliai"),
        ("PNV", "Panevėžys"),
    ]


def test_xml_read_text_lang_search_select(rc: RawConfig, tmp_path: Path):
    xml = """
    <miestai>
        <miestas>
            <pavadinimas>Kaunas</pavadinimas>
            <kodas>KNS</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Vilnius</pavadinimas>
            <kodas>VNO</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Klaipėda</pavadinimas>
            <kodas>KLJ</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Šiauliai</pavadinimas>
            <kodas>SQQ</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Panevėžys</pavadinimas>
            <kodas>PNV</kodas>
        </miestas>
    </miestai>
    """
    path = tmp_path / "miestai.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source            | access
    example/xml              |          |      |                   |
      | xml                  | dask/xml |      | {path}            |
      |   |   | City         |          | code | /miestai/miestas  |
      |   |   |   | name@lt  | string   |      | pavadinimas/text()| open
      |   |   |   | code     | string   |      | kodas/text()      | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall", "search"])

    resp = app.get("/example/xml/City?select(code,name@lt)")
    assert listdata(resp, "code", "name@lt", sort=False) == [
        ("KNS", "Kaunas"),
        ("VNO", "Vilnius"),
        ("KLJ", "Klaipėda"),
        ("SQQ", "Šiauliai"),
        ("PNV", "Panevėžys"),
    ]


def test_xml_read_text_lang_select_single_search_at(rc: RawConfig, tmp_path: Path):
    xml = """
    <miestai>
        <miestas>
            <pavadinimas>Kaunas</pavadinimas>
            <kodas>KNS</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Vilnius</pavadinimas>
            <kodas>VNO</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Klaipėda</pavadinimas>
            <kodas>KLJ</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Šiauliai</pavadinimas>
            <kodas>SQQ</kodas>
        </miestas>
        <miestas>
            <pavadinimas>Panevėžys</pavadinimas>
            <kodas>PNV</kodas>
        </miestas>
    </miestai>
    """
    path = tmp_path / "miestai.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source            | access
    example/xml              |          |      |                   |
      | xml                  | dask/xml |      | {path}            |
      |   |   | City         |          | code | /miestai/miestas  |
      |   |   |   | name@lt  | string   |      | pavadinimas/text()| open
      |   |   |   | code     | string   |      | kodas/text()      | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall", "search"])

    resp = app.get("/example/xml/City?select(code,name@lt)")
    assert listdata(resp, "code", "name@lt", sort=False) == [
        ("KNS", "Kaunas"),
        ("VNO", "Vilnius"),
        ("KLJ", "Klaipėda"),
        ("SQQ", "Šiauliai"),
        ("PNV", "Panevėžys"),
    ]


def test_xml_read_text_lang_multiple_variants_get_all(rc: RawConfig, tmp_path: Path):
    xml = """
        <miestai>
            <miestas>
                <pavadinimas_lt>Kaunas</pavadinimas_lt>
                <pavadinimas_en>Kaunas_en</pavadinimas_en>
                <kodas>KNS</kodas>
            </miestas>
            <miestas>
                <pavadinimas_lt>Vilnius</pavadinimas_lt>
                <pavadinimas_en>Vilnius_en</pavadinimas_en>
                <kodas>VNO</kodas>
            </miestas>
        </miestai>
    """
    path = tmp_path / "miestai.xml"
    path.write_text(xml)
    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source               | access
    example/xml              |          |      |                      |
      | xml                  | dask/xml |      | {path}               |
      |   |   | City         |          | code | /miestai/miestas      |
      |   |   |   | name@lt  | string   |      | pavadinimas_lt/text() | open
      |   |   |   | name@en  | string   |      | pavadinimas_en/text() | open
      |   |   |   | code     | string   |      | kodas/text()          | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")

    data = resp.json()["_data"]

    for item in data:
        item.pop("_id")

    assert data == [
        {
            "_type": "example/xml/City",
            "_revision": None,
            "name": {"lt": "Kaunas", "en": "Kaunas_en"},
            "code": "KNS",
        },
        {
            "_type": "example/xml/City",
            "_revision": None,
            "name": {"lt": "Vilnius", "en": "Vilnius_en"},
            "code": "VNO",
        },
    ]


def test_xml_read_text_lang_select_multiple_variants_search(rc: RawConfig, tmp_path: Path):
    xml = """
        <miestai>
            <miestas>
                <pavadinimas_lt>Kaunas</pavadinimas_lt>
                <pavadinimas_en>Kaunas</pavadinimas_en>
                <kodas>KNS</kodas>
            </miestas>
            <miestas>
                <pavadinimas_lt>Vilnius</pavadinimas_lt>
                <pavadinimas_en>Vilnius</pavadinimas_en>
                <kodas>VNO</kodas>
            </miestas>
        </miestai>
    """
    path = tmp_path / "miestai.xml"
    path.write_text(xml)
    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source               | access
    example/xml              |          |      |                      |
      | xml                  | dask/xml |      | {path}               |
      |   |   | City         |          | code | /miestai/miestas      |
      |   |   |   | name@lt  | string   |      | pavadinimas_lt/text() | open
      |   |   |   | name@en  | string   |      | pavadinimas_en/text() | open
      |   |   |   | code     | string   |      | kodas/text()          | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall", "search"])

    resp = app.get("/example/xml/City?select(code,name@lt,name@en)")
    assert listdata(resp, "code", "name@lt", "name@en", sort=False) == [
        ("KNS", "Kaunas", "Kaunas"),
        ("VNO", "Vilnius", "Vilnius"),
    ]
