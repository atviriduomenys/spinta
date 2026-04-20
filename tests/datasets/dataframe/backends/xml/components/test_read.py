import json
from pathlib import Path
import pytest
from unittest.mock import ANY
from responses import RequestsMock, POST

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import SourceOrPrepareNotAllowed, PartialIncorrectProperty
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest
from spinta.testing.utils import get_error_codes, get_error_context
from spinta.utils.schema import NA


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


@pytest.mark.parametrize(
    "first_val, second_val", [("false", "true"), ("False", "True"), ("0", "1"), ("0.0", "1.0"), (0, 1), (0.0, 1.0)]
)
def test_xml_read_bool_enum(rc: RawConfig, tmp_path: Path, first_val: str, second_val: str):
    xml = f"""
        <cities>
            <city>
                <is_capital>{first_val}</is_capital>
                <name>Kaunas</name>
            </city>
            <city>
                <is_capital>{second_val}</is_capital>
                <name>Vilnius</name>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property   | type     | ref  | source       | prepare | access
    example/xml                |          |      |              |         |
      | xml                    | dask/xml |      | {path}       |         |
      |   |   | City           |          | name | /cities/city |         |
      |   |   |   | name       | string   |      | name         |         | open
      |   |   |   | is_capital | boolean  |      | is_capital   |         | open
      |   |   |   |            | enum     |      | {first_val}  | false   | open
      |   |   |   |            | enum     |      | {second_val} | true    | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        (False, "Kaunas"),
        (True, "Vilnius"),
    ]


@pytest.mark.parametrize(
    "first_val, second_val", [("false", "true"), ("False", "True"), ("0", "1"), (0, 1), ("off", "on")]
)
def test_xml_read_bool(rc: RawConfig, tmp_path: Path, first_val: str, second_val: str):
    xml = f"""
        <cities>
            <city>
                <is_capital>{first_val}</is_capital>
                <name>Kaunas</name>
            </city>
            <city>
                <is_capital>{second_val}</is_capital>
                <name>Vilnius</name>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property   | type     | ref  | source       | access
    example/xml                |          |      |              |
      | xml                    | dask/xml |      | {path}       |
      |   |   | City           |          | name | /cities/city |
      |   |   |   | name       | string   |      | name         | open
      |   |   |   | is_capital | boolean  |      | is_capital   | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        (False, "Kaunas"),
        (True, "Vilnius"),
    ]


def test_xml_with_ref_loads_data_enum(rc: RawConfig, tmp_path: Path):
    xml = """
        <r>
            <Cities>
                <CityID>401</CityID>
                <Code>6666000000</Code>
            </Cities>
            <Cities>
                <CityID>402</CityID>
                <Code>7777000000</Code>
            </Cities>
        </r>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property           | type             | ref          | source                  | access | prepare
    example/xml                        |                  |              |                         |        |
      | xml                            | dask/xml         |              | {path}                  |        |
      |   |   | City                   |                  | id           |                         |        |
      |   |   |   | id                 | integer required |              | CityID/text()           | open   |
      |   |   |   |                    | enum             |              | 35                      | open   | 35
      |   |   |   |                    |                  |              | 40                      | open   | 40
      |   |   |   | code               | integer required |              | Code/text()           | open   |
      |   |   | Details                |                  |              | /r/Cities               |        |
      |   |   |   | contract_type      | ref              | City         | CityID/text()           | open   |
      |   |   |   | contract_type.code | integer required |              | Code/text()           | open   |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Details", ["getall"])
    resp = app.get("/example/xml/Details")
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/xml/Details",
            "_id": ANY,
            "_revision": None,
            "contract_type": {
                "_id": ANY,
                "code": 6666000000,
            },
        },
        {
            "_type": "example/xml/Details",
            "_id": ANY,
            "_revision": None,
            "contract_type": {
                "_id": ANY,
                "code": 7777000000,
            },
        },
    ]


def test_xml_with_ref_loads_data(rc: RawConfig, tmp_path: Path):
    xml = """
        <r>
            <Cities>
                <CityID>301</CityID>
                <Code>6666000000</Code>
            </Cities>
            <Cities>
                <CityID>302</CityID>
                <Code>7777000000</Code>
            </Cities>
        </r>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property           | type             | ref          | source                  | access | prepare
    example/xml                        |                  |              |                         |        |
      | xml                            | dask/xml         |              | {path}                  |        |
      |   |   | City                   |                  | id           |                         |        |
      |   |   |   | id                 | integer required |              | CityID/text()           | open   |

      |   |   | Details                |                  | code         | /r/Cities               |        |
      |   |   |   | contract_type      | ref              | City         | CityID/text()           | open   |
      |   |   |   | contract_type.code | string required  |              | CityID/text()           | open   |
      |   |   |   | code               | string           |              | Code/text()             | open   |
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Details", ["getall"])
    resp = app.get("/example/xml/Details")
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/xml/Details",
            "_id": ANY,
            "_revision": None,
            "contract_type": {
                "_id": ANY,
                "code": "301",
            },
            "code": "6666000000",
        },
        {
            "_type": "example/xml/Details",
            "_id": ANY,
            "_revision": None,
            "contract_type": {
                "_id": ANY,
                "code": "302",
            },
            "code": "7777000000",
        },
    ]


def test_xml_read_text_lang_getall(rc: RawConfig, tmp_path: Path):
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

    assert data == [
        {
            "_id": ANY,
            "_type": "example/xml/City",
            "_revision": None,
            "name": {"lt": "Kaunas", "en": "Kaunas_en"},
            "code": "KNS",
        },
        {
            "_id": ANY,
            "_type": "example/xml/City",
            "_revision": None,
            "name": {"lt": "Vilnius", "en": "Vilnius_en"},
            "code": "VNO",
        },
    ]


def test_xml_fails_on_composite_prepare(rc: RawConfig, tmp_path: Path):
    xml = """
	<israsas>
        <akciju_klases_tipas>
            <kodas>110</kodas>
            <pavadinimas>Vardinių paprastųjų akcijų skaičius</pavadinimas>
        </akciju_klases_tipas>
	</israsas>
    """

    path = tmp_path / "example.xml"
    path.write_text(xml)
    with pytest.raises(SourceOrPrepareNotAllowed) as error:
        context, manifest = prepare_manifest(
            rc,
            f"""
            d | r | b | m | property                | type            | ref        | source              | prepare                 | access
            example/xml                             |                 |            |                     |                         |
              | resource                            | dask/xml        |            | {path}              |                         |
              |   |   | Event                       |                 |            | israsas             |                         |
              |   |   |   | type                    | ref required    | AssetType  | akciju_klases_tipas | type_attribute.title_lt | open
              |   |   |   | type_attribute          | ref required    | AssetType  | akciju_klases_tipas |                         | open
              |   |   |   | type_attribute.code     | string required |            | kodas               |                         | open
              |   |   |   | type_attribute.title_lt | string required |            | pavadinimas         |                         | open
              |   |   | EntityAttribute             |                 |            | israsas             |                         |
              |   |   |   | code                    | string          |            | kodas               |                         | open
              |   |   |   | title_lt                | string          |            | pavadinimas         |                         | open
              |   |   | AssetType                   |                 |            | israsas             |                         | open
              |   |   |   | code                    | string          |            | kodas               |                         | open
              |   |   |   | title_lt                | string          |            | pavadinimas         |                         | open
            """,
            mode=Mode.external,
        )
        assert (
            error.value.args[0]
            == "The source akciju_klases_tipas was not expected. Delete it from the manifest or update the prepare function to allow it."
        )


def test_xml_passes_on_composite_prepare_if_no_source(rc: RawConfig, tmp_path: Path):
    xml = """
	<israsas>
        <akciju_klases_tipas>
            <kodas>110</kodas>
            <pavadinimas>Vardinių paprastųjų akcijų skaičius</pavadinimas>
        </akciju_klases_tipas>
	</israsas>
    """

    path = tmp_path / "example.xml"
    path.write_text(xml)
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property                | type            | ref                    | source              | prepare                 | access
        example/xml                             |                 |                        |                     |                         |
          | resource                            | dask/xml        |                        | {path}              |                         |
          |   |   | Event                       |                 |                        | israsas             |                         |
          |   |   |   | type                    | ref required    | example2/xml/AssetType |                     | type_attribute.title_lt | open
          |   |   |   | type_attribute          | ref required    | EntityAttribute        | akciju_klases_tipas |                         | open
          |   |   |   | type_attribute.code     | string required |                        | kodas               |                         | open
          |   |   |   | type_attribute.title_lt | string required |                        | pavadinimas         |                         | open
          |   |   | EntityAttribute             |                 |                        | israsas             |                         |
          |   |   |   | code                    | string          |                        | kodas               |                         | open
          |   |   |   | title_lt                | string          |                        | pavadinimas         |                         | open
        example2/xml                            |                 |                        |                     |                         |
          |   |   | AssetType                   |                 |                        | israsas             |                         | open
          |   |   |   | code                    | string          |                        | kodas               |                         | open
          |   |   |   | title_lt                | string          |                        | pavadinimas         |                         | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Event", ["getall"])
    resp = app.get("/example/xml/Event")
    assert resp.json()["errors"][0]["code"] == "GivenValueCountMissmatch"


@pytest.mark.parametrize(
    "select,expected_name",
    [
        (
            "code,name@lt,name@en",
            [
                {"lt": "Kaunas", "en": "Kaunas_en"},
                {"lt": "Vilnius", "en": "Vilnius_en"},
            ],
        ),
        (
            "code,name@lt",
            [
                {"lt": "Kaunas"},
                {"lt": "Vilnius"},
            ],
        ),
        (
            "code,name",
            [
                {"lt": "Kaunas", "en": "Kaunas_en"},
                {"lt": "Vilnius", "en": "Vilnius_en"},
            ],
        ),
    ],
)
def test_xml_read_text_lang_select(rc: RawConfig, tmp_path: Path, select: str, expected_name: list[dict[str, str]]):
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
    app.authmodel("example/xml/City", ["getall", "search"])

    resp = app.get(f"/example/xml/City?select({select})")
    assert listdata(resp, "code", "name", "name@lt", "name@en", sort=False) == [
        ("KNS", expected_name[0], NA, NA),
        ("VNO", expected_name[1], NA, NA),
    ]


def test_composite_prepare_links_tables_error_if_count_mismatch(rc: RawConfig, tmp_path: Path):
    xml = """
	<israsas>
        <akciju_klases_tipas>
            <kodas>110</kodas>
            <pavadinimas>Vardinių paprastųjų akcijų skaičius</pavadinimas>
        </akciju_klases_tipas>
	</israsas>
    """

    path = tmp_path / "example.xml"
    path.write_text(xml)
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property                | type            | ref                    | source              | prepare                 | access
        example/xml                             |                 |                        |                     |                         |
          | resource                            | dask/xml        |                        | {path}              |                         |
          |   |   | Event                       |                 |                        | israsas             |                         |
          |   |   |   | type                    | ref required    | AssetType              |                     | type_attribute.title_lt | open
          |   |   |   | type_attribute          | ref required    | EntityAttribute[code]  |                     |                         | open
          |   |   |   | type_attribute.code     | string required |                        | kodas               |                         | open
          |   |   |   | type_attribute.title_lt | string required |                        | pavadinimas         |                         | open
          |   |   | EntityAttribute             |                 |                        | israsas             |                         |
          |   |   |   | code                    | string          |                        | kodas               |                         | open
          |   |   |   | title_lt                | string          |                        | pavadinimas         |                         | open
          |   |   | AssetType                   |                 |                        | israsas             |                         | open
          |   |   |   | code                    | string          |                        | kodas               |                         | open
          |   |   |   | title_lt                | string          |                        | pavadinimas         |                         | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Event", ["getall"])
    resp = app.get("/example/xml/Event")
    assert resp.json()["errors"][0]["code"] == "GivenValueCountMissmatch"


def test_composite_prepare_links_tables(rc: RawConfig, tmp_path: Path):
    xml = """
    <israsas>
        <kodas>110</kodas>
        <pavadinimas>pavadinimas</pavadinimas>
    </israsas>
    """

    path = tmp_path / "example.xml"
    path.write_text(xml)
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property                       | type            | ref                        | source             | prepare
        example                                        |                 |                            |                    |
          | service                                    | dask/xml        |                            | {path}             |
          |   |   | ParticipantEvent                   |                 |                            | /israsas           |
          |   |   |   | asset_type                     | ref required    | AssetType                  |                    | asset_type_attribute.title_lt
          |   |   |   | asset_type_attribute           | ref required    | LegalEntityAttribute[code] | kodas/text()       |
          |   |   |   | asset_type_attribute.code      | string required |                            | kodas/text()       |
          |   |   |   | asset_type_attribute.title_lt  | string          |                            | pavadinimas/text() |
          |   |   | LegalEntityAttribute               |                 |                            | /israsas           |
          |   |   |   | code                           | string          |                            | kodas/text()       |
          |   |   |   | title_lt                       | string          |                            | pavadinimas/text() |
          |   |   | AssetType                          |                 | name                       | /israsas           |
          |   |   |   | code                           | string          |                            | kodas/text()       |
          |   |   |   | name                           | string          |                            | pavadinimas/text() |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    models = ["example/ParticipantEvent", "example/AssetType", "example/LegalEntityAttribute"]
    for model in models:
        app.authmodel(model, ["getall"])

    participant = app.get("/example/ParticipantEvent")
    legal = app.get("/example/LegalEntityAttribute")
    asset = app.get("/example/AssetType")

    participant_data = participant.json()["_data"]
    legal_data = legal.json()["_data"]
    asset_data = asset.json()["_data"]

    assert participant_data == [
        {
            "_id": ANY,
            "_revision": None,
            "_type": "example/ParticipantEvent",
            "asset_type": {"_id": asset_data[0]["_id"]},
            "asset_type_attribute": {
                "_id": legal_data[0]["_id"],
                "code": "110",
                "title_lt": "pavadinimas",
            },
        }
    ]


def test_read_with_prepare_cast(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city>
                <string_field>abc</string_field>
                <integer_field>1</integer_field>
                <number_field>1.2</number_field>
                <boolean_field>true</boolean_field>
                <date_field>2025-05-05</date_field>
                <time_field>12:05</time_field>
                <datetime_field>2025-05-05T12:05</datetime_field>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property       | type     | ref          | source         | prepare
        example/xml                    |          |              |                |
          | xml                        | dask/xml |              | {path}         |
          |   |   | City               |          | string_field | /cities/city   |
          |   |   |   | string_field   | string   |              | string_field   | cast()
          |   |   |   | integer_field  | integer  |              | integer_field  | cast()
          |   |   |   | number_field   | number   |              | number_field   | cast()
          |   |   |   | boolean_field  | boolean  |              | boolean_field  | cast()
          |   |   |   | date_field     | date     |              | date_field     | cast()
          |   |   |   | time_field     | time     |              | time_field     | cast()
          |   |   |   | datetime_field | datetime |              | datetime_field | cast()
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    response = app.get("/example/xml/City")
    assert response.json()["_data"] == [
        {
            "_type": "example/xml/City",
            "_id": ANY,
            "_revision": None,
            "string_field": "abc",
            "integer_field": 1,
            "number_field": 1.2,
            "boolean_field": True,
            "date_field": "2025-05-05",
            "time_field": "12:05:00",
            "datetime_field": "2025-05-05T12:05:00",
        },
    ]


def test_read_with_invalid_prepare_cast(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city>
                <geometry_field>SRID=4326;POINT(15 15)</geometry_field>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property       | type     | source         | prepare
        example/xml                    |          |                |
          | xml                        | dask/xml | {path}         |
          |   |   | City               |          | /cities/city   |
          |   |   |   | geometry_field | geometry | geometry_field | cast()
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    response = app.get("/example/xml/City")
    assert response.status_code == 500

    response_error = response.json()["errors"][0]
    assert response_error["code"] == "NotImplementedFeature"
    assert response_error["message"] == 'Prepare method "cast()" for data type geometry is not implemented yet.'


def test_read_prepare_cast_with_argument(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city>
                <string_field>1234</string_field>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property     | type     | source       | prepare
        example/xml                  |          |              |
          | xml                      | dask/xml | {path}       |
          |   |   | City             |          | /cities/city |
          |   |   |   | string_field | string   | string_field | cast("integer")
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    response = app.get("/example/xml/City")
    assert response.status_code == 500

    response_error = response.json()["errors"][0]
    assert response_error["code"] == "InvalidArgumentInExpression"
    assert response_error["message"] == "Invalid ['integer'] arguments given to cast expression."


def test_composite_ref_two_levels_returns_data(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <participant>
            <code>P001</code>
            <asset_code>AT001</asset_code>
            <asset_name>Equipment</asset_name>
        </participant>
        <participant>
            <code>P002</code>
            <asset_code>AT002</asset_code>
            <asset_name>Building</asset_name>
        </participant>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property             | type       | ref       | source              | access | level
    example                              |            |           |                     |        |
      | data                             | dask/xml   |           | {path}              |        |
      |   |   | AssetType                |            | code      | /root/participant   |        | 5
      |   |   |   | code                 | string     |           | asset_code          | open   | 5
      |   |   |   | name                 | string     |           | asset_name          | open   | 5
      |   |   | Participant              |            |           | /root/participant   |        | 5
      |   |   |   | code                 | string     |           | code                | open   | 5
      |   |   |   | asset_type           | ref required | AssetType | asset_code         | open   | 5
      |   |   |   | asset_type.code      | string     |           | asset_code          | open   | 5
      |   |   |   | asset_type.name      | string     |           | asset_name          | open   | 5
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Participant", ["getall"])
    app.authmodel("example/AssetType", ["getall"])

    asset_type_resp = app.get("/example/AssetType")
    asset_type_ids = [asset_type_object["_id"] for asset_type_object in asset_type_resp.json()["_data"]]

    resp = app.get("/example/Participant")
    assert resp.status_code == 200

    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Participant",
            "_id": ANY,
            "_revision": None,
            "code": "P001",
            "asset_type": {"_id": asset_type_ids[0], "name": "Equipment", "code": "AT001"},
        },
        {
            "_type": "example/Participant",
            "_id": ANY,
            "_revision": None,
            "code": "P002",
            "asset_type": {"_id": asset_type_ids[1], "name": "Building", "code": "AT002"},
        },
    ]


def test_composite_ref_three_levels_xyz(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
            <country_name>Lithuania</country_name>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
            <country_name>Poland</country_name>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                | type       | ref      | source              | access
    example                                 |            |          |                     |
      | data                                | dask/xml   |          | {path}              |
      |   |   | Country                     |            | code     | /root/order         |
      |   |   |   | code                    | string     |          | country_code        | open
      |   |   |   | name                    | string     |          | country_name        | open
      |   |   | Vendor                      |            | code         | /root/order         |
      |   |   |   | code                    | string     |          | vendor_code         | open
      |   |   |   | country                 | ref required | Country | country_code        | open
      |   |   |   | country.code            | string     |          | country_code        | open
      |   |   |   | country.name            | string     |          | country_name        | open
      |   |   | Order                       |            |          | /root/order         |
      |   |   |   | id                      | string     |          | id                  | open
      |   |   |   | vendor                  | ref required | Vendor | vendor_code         | open
      |   |   |   | vendor.country.code     | string     |          | country_code        | open
      |   |   |   | vendor.country.name     | string     |          | country_name        | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])

    vendor_resp = app.get("/example/Vendor")
    vendor_ids = [vendor_object["_id"] for vendor_object in vendor_resp.json()["_data"]]

    country_resp = app.get("/example/Country")
    country_ids = [country_object["_id"] for country_object in country_resp.json()["_data"]]

    resp = app.get("/example/Order")
    assert resp.status_code == 200

    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD001",
            "vendor": {
                "_id": vendor_ids[0],
                "country": {
                    "_id": country_ids[0],
                    "code": "LT",
                    "name": "Lithuania",
                },
            },
        },
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD002",
            "vendor": {
                "_id": vendor_ids[1],
                "country": {
                    "_id": country_ids[1],
                    "code": "PL",
                    "name": "Poland",
                },
            },
        },
    ]


def test_composite_ref_four_levels_xyze(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
            <region_code>REG001</region_code>
            <region_name>Vilnius Region</region_name>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
            <region_code>REG002</region_code>
            <region_name>Warsaw Region</region_name>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source              | access
    example                                       |            |          |                     |
      | data                                      | dask/xml   |          | {path}              |
      |   |   | Region                            |            | code     | /root/order         |
      |   |   |   | code                          | string     |          | region_code         | open
      |   |   |   | name                          | string     |          | region_name         | open
      |   |   | Country                           |            | code     | /root/order         |
      |   |   |   | code                          | string     |          | country_code        | open
      |   |   |   | region                        | ref required | Region  | region_code         | open
      |   |   |   | region.code                   | string     |          | region_code         | open
      |   |   |   | region.name                   | string     |          | region_name         | open
      |   |   | Vendor                            |            | code     | /root/order         |
      |   |   |   | code                          | string     |          | vendor_code         | open
      |   |   |   | country                       | ref required | Country | country_code        | open
      |   |   |   | country.code                  | string     |          | country_code        | open
      |   |   |   | country.region.code           | string     |          | region_code         | open
      |   |   |   | country.region.name           | string     |          | region_name         | open
      |   |   | Order                             |            | id       | /root/order         |
      |   |   |   | id                            | string     |          | id                  | open
      |   |   |   | vendor                        | ref required | Vendor  | vendor_code         | open
      |   |   |   | vendor.country.code           | string     |          | country_code        | open
      |   |   |   | vendor.country.region.code    | string     |          | region_code         | open
      |   |   |   | vendor.country.region.name    | string     |          | region_name         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])
    app.authmodel("example/Region", ["getall"])

    vendor_resp = app.get("/example/Vendor")
    vendor_ids = [vendor_object["_id"] for vendor_object in vendor_resp.json()["_data"]]

    country_resp = app.get("/example/Country")
    country_ids = [country_object["_id"] for country_object in country_resp.json()["_data"]]

    region_resp = app.get("/example/Region")
    region_ids = [region_object["_id"] for region_object in region_resp.json()["_data"]]

    resp = app.get("/example/Order")
    assert resp.status_code == 200
    data = resp.json()["_data"]

    assert data == [
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD001",
            "vendor": {
                "_id": vendor_ids[0],
                "country": {
                    "_id": country_ids[0],
                    "code": "LT",
                    "region": {
                        "_id": region_ids[0],
                        "code": "REG001",
                        "name": "Vilnius Region",
                    },
                },
            },
        },
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD002",
            "vendor": {
                "_id": vendor_ids[1],
                "country": {
                    "_id": country_ids[1],
                    "code": "PL",
                    "region": {
                        "_id": region_ids[1],
                        "code": "REG002",
                        "name": "Warsaw Region",
                    },
                },
            },
        },
    ]


def test_incorrect_composite_property(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
            <country_name>Lithuania</country_name>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
            <country_name>Poland</country_name>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    with pytest.raises(PartialIncorrectProperty):
        context, manifest = prepare_manifest(
            rc,
            f"""
        d | r | b | m | property                | type       | ref      | source              | access
        example                                 |            |          |                     |
          | data                                | dask/xml   |          | {path}              |
          |   |   | Country                     |            | code     | /root/order         |
          |   |   |   | code                    | string     |          | country_code        | open
          |   |   |   | name                    | string     |          | country_name        | open
          |   |   | Vendor                      |            | code         | /root/order         |
          |   |   |   | code                    | string     |          | vendor_code         | open
          |   |   |   | country                 | ref required | Country | country_code        | open
          |   |   |   | country.code            | string     |          | country_code        | open
          |   |   |   | country.name            | string     |          | country_name        | open
          |   |   | Order                       |            |          | /root/order         |
          |   |   |   | id                      | string     |          | id                  | open
          |   |   |   | vendor                  | ref required | Vendor | vendor_code         | open
          |   |   |   | vendor.incorrect.code   | string     |          | country_code        | open
          |   |   |   | vendor.country.name     | string     |          | country_name        | open
        """,
            mode=Mode.external,
        )


def test_incorrect_composite_property_primary_key_values(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
            <country_name>Lithuania</country_name>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
            <country_name>Poland</country_name>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                | type       | ref      | source              | access
    example                                 |            |          |                     |
      | data                                | dask/xml   |          | {path}              |
      |   |   | Country                     |            | code     | /root/order         |
      |   |   |   | code                    | string     |          | country_code        | open
      |   |   |   | name                    | string     |          | country_name        | open
      |   |   | Vendor                      |            | code         | /root/order         |
      |   |   |   | code                    | string     |          | vendor_code         | open
      |   |   |   | country                 | ref required | Country | vendor_code        | open
      |   |   |   | country.code            | string     |          | country_code        | open
      |   |   |   | country.name            | string     |          | country_name        | open
      |   |   | Order                       |            |          | /root/order         |
      |   |   |   | id                      | string     |          | id                  | open
      |   |   |   | vendor                  | ref required | Vendor | vendor_code         | open
      |   |   |   | vendor.country.code   | string     |          | country_code        | open
      |   |   |   | vendor.country.name     | string     |          | country_name        | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)

    app.authmodel("example/Order", ["getall"])

    resp = app.get("/example/Order")
    assert resp.status_code == 400
    assert resp.json()["errors"][0]["code"] == "NoPrimaryKeyCandidatesFound"


def test_composite_ref_level_2_no_id(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property              | type         | ref     | source       | level | access
    example                               |              |         |              |       |
      | data                              | dask/xml     |         | {path}       |       |
      |   |   | Country                   |              | code    | /root/order  |       |
      |   |   |   | code                  | string       |         | country_code | 5     | open
      |   |   | Vendor                    |              | code    | /root/order  |       |
      |   |   |   | code                  | string       |         | vendor_code  | 5     | open
      |   |   |   | country               | ref required | Country | country_code | 2     | open
      |   |   |   | country.code          | string       |         | country_code |       | open
      |   |   | Item                      |              | code    | /root/order  |       |
      |   |   |   | code                  | string       |         | id           | 5     | open
      |   |   |   | vendor                | ref required | Vendor  | vendor_code  | 5     | open
      |   |   |   | vendor.code           | string       |         | vendor_code  |       | open
      |   |   |   | vendor.country.code   | string       |         | country_code |       | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Item", ["getall"])
    app.authmodel("example/Vendor", ["getall"])
    app.authmodel("example/Country", ["getall"])

    vendor_resp = app.get("/example/Vendor")
    vendor_ids = [vendor_id["_id"] for vendor_id in vendor_resp.json()["_data"]]

    resp = app.get("/example/Item")
    assert resp.status_code == 200
    data = resp.json()["_data"]

    assert data == [
        {
            "_type": "example/Item",
            "_id": ANY,
            "_revision": None,
            "code": "ORD001",
            "vendor": {"_id": vendor_ids[0], "code": "VEND001", "country": {"code": "LT"}},
        },
        {
            "_type": "example/Item",
            "_id": ANY,
            "_revision": None,
            "code": "ORD002",
            "vendor": {"_id": vendor_ids[1], "code": "VEND002", "country": {"code": "PL"}},
        },
    ]


def test_composite_ref_four_levels_composite_2_level(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ORD001</id>
            <vendor_code>VEND001</vendor_code>
            <country_code>LT</country_code>
            <region_code>REG001</region_code>
            <region_name>Vilnius Region</region_name>
        </order>
        <order>
            <id>ORD002</id>
            <vendor_code>VEND002</vendor_code>
            <country_code>PL</country_code>
            <region_code>REG002</region_code>
            <region_name>Warsaw Region</region_name>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | code     | /root/order   |            |
      |   |   |   | code                          | string     |          | region_code   |            | open
      |   |   |   | name                          | string     |          | region_name   |            | open
      |   |   | Country                           |            | code     | /root/order   |            |
      |   |   |   | code                          | string     |          | country_code  |            | open
      |   |   |   | region                        | ref required | Region  | region_code  | 2          | open
      |   |   |   | region.code                   | string     |          | region_code   |            | open
      |   |   |   | region.name                   | string     |          | region_name   |            | open
      |   |   | Vendor                            |            | code     | /root/order   |            |
      |   |   |   | code                          | string     |          | vendor_code   |            | open
      |   |   |   | country                       | ref required | Country | country_code | 2          | open
      |   |   |   | country.code                  | string     |          | country_code  |            | open
      |   |   |   | country.region.code           | string     |          | region_code   |            | open
      |   |   |   | country.region.name           | string     |          | region_name   |            | open
      |   |   | Order                             |            | id       | /root/order   |            |
      |   |   |   | id                            | string     |          | id            |            | open
      |   |   |   | vendor                        | ref required | Vendor | vendor_code   | 2          | open
      |   |   |   | vendor.country.code           | string     |          | country_code  |            | open
      |   |   |   | vendor.country.region.code    | string     |          | region_code   |            | open
      |   |   |   | vendor.country.region.name    | string     |          | region_name   |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Order", ["getall"])

    resp = app.get("/example/Order")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD001",
            "vendor": {
                "code": "VEND001",
                "country": {"code": "LT", "region": {"code": "REG001", "name": "Vilnius Region"}},
            },
        },
        {
            "_type": "example/Order",
            "_id": ANY,
            "_revision": None,
            "id": "ORD002",
            "vendor": {
                "code": "VEND002",
                "country": {"code": "PL", "region": {"code": "REG002", "name": "Warsaw Region"}},
            },
        },
    ]


def test_for_id_uuid(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>ed76eda3-7922-4a7d-9ba8-62828ca0ae98</id>
            <code>ORD001</code>
        </order>
        <order>
            <id>1590ab44-6463-4da7-8862-3598f6e83924</id>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | _id      | /root/order   |            |
      |   |   |   | code                          | string     |          | code          |            | open
      |   |   |   | _id                           | uuid       |          | id            |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": "ed76eda3-7922-4a7d-9ba8-62828ca0ae98", "_revision": None, "code": "ORD001"},
        {"_type": "example/Region", "_id": "1590ab44-6463-4da7-8862-3598f6e83924", "_revision": None, "code": "ORD002"},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/ed76eda3-7922-4a7d-9ba8-62828ca0ae98")
        # Expected, XML does not support getone operations


def test_for_id_integer(rc: RawConfig, tmp_path: Path):
    xml = """
    <root>
        <order>
            <id>123</id>
            <code>ORD001</code>
        </order>
        <order>
            <id>1234</id>
            <code>ORD002</code>
        </order>
    </root>
    """
    path = tmp_path / "test.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property                      | type       | ref      | source        | level      | access
    example                                       |            |          |               |            |
      | data                                      | dask/xml   |          | {path}        |            |
      |   |   | Region                            |            | _id      | /root/order   |            |
      |   |   |   | code                          | string     |          | code          |            | open
      |   |   |   | _id                           | integer       |          | id            |            | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/Region", ["getall", "getone"])

    resp = app.get("/example/Region")
    assert resp.status_code == 200
    data = resp.json()["_data"]
    assert data == [
        {"_type": "example/Region", "_id": 123, "_revision": None, "code": "ORD001"},
        {"_type": "example/Region", "_id": 1234, "_revision": None, "code": "ORD002"},
    ]
    with pytest.raises(NotImplementedError):
        app.get("/example/Region/123")
        # Expected, XML does not support getone operations
