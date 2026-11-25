import logging
from pathlib import Path

import pytest
from responses import GET, POST, RequestsMock

from spinta.core.config import RawConfig
from spinta.core.enums import Mode
from spinta.exceptions import WsdlReadError
from spinta.manifests.wsdl.helpers import is_wsdl_path, read_schema
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import load_manifest, prepare_manifest

DATA = "tests/manifests/wsdl/data"


def test_wsdl_with_schemas_referencing_each_other(rc: RawConfig):
    # schema blocks use each other's types, `Asmuo` type is used by both a root and a nested element
    manifest = load_manifest(rc, f"{DATA}/skola_sodrai.wsdl")
    assert (
        manifest
        == """
    d | r | b | m | property        | type     | ref            | source                                                          | prepare               | title
    skola_sodrai                    |          |                |                                                                 |                       | SkolaSodraiService
      | wsdl                        | wsdl     |                | tests/manifests/wsdl/data/skola_sodrai.wsdl                     |                       |
      | skola_sodrai                | soap     |                | skolasodrai.skolasodraiPortType.skolasodraiPortType.SkolaSodrai | wsdl(wsdl)            | SkolaSodrai
                                    | param    | asm_kodas      | asm_kodas                                                       | input()               |
                                    | param    | gim_data       | gim_data                                                        | input()               |
                                    | param    | klausejo_kodas | klausejo_kodas                                                  | input()               |
                                    | param    | pavarde        | pavarde                                                         | input()               |
                                    | param    | sdn            | sdn                                                             | input()               |
                                    | param    | sds            | sds                                                             | input()               |
                                    | param    | tikslas        | tikslas                                                         | input()               |
                                    | param    | vardas         | vardas                                                          | input()               |
                                    |          |                |                                                                 |                       |
      |   |   | Asmuo/:part         |          |                |                                                                 |                       |
      |   |   |   | asm_kodas       | integer  |                | asm_kodas                                                       |                       |
      |   |   |   | gim_data        | date     |                | gim_data                                                        |                       |
      |   |   |   | pavadinimas     | string   |                | pavadinimas                                                     |                       |
      |   |   |   | pavarde         | string   |                | pavarde                                                         |                       |
      |   |   |   | sdn             | integer  |                | sdn                                                             |                       |
      |   |   |   | sds             | string   |                | sds                                                             |                       |
      |   |   |   | skola           | ref      | Skola          | skola                                                           | expand()              |
      |   |   |   | vardas          | string   |                | vardas                                                          |                       |
                                    |          |                |                                                                 |                       |
      |   |   | Skola/:part         |          |                |                                                                 |                       |
      |   |   |   | rezultatas      | string   |                | rezultatas                                                      |                       |
                                    |          |                |                                                                 |                       |
      |   |   | SkolaSodraiResponse |          |                | /                                                               |                       |
      |   |   |   | asmuo           | ref      | Asmuo          | asmuo                                                           | expand()              |
      |   |   |   | laikas          | datetime |                | laikas                                                          |                       |
      |   |   |   | asm_kodas       | integer  |                |                                                                 | param(asm_kodas)      |
      |   |   |   | gim_data        | date     |                |                                                                 | param(gim_data)       |
      |   |   |   | klausejo_kodas  | integer  |                |                                                                 | param(klausejo_kodas) |
      |   |   |   | pavarde         | string   |                |                                                                 | param(pavarde)        |
      |   |   |   | sdn             | integer  |                |                                                                 | param(sdn)            |
      |   |   |   | sds             | string   |                |                                                                 | param(sds)            |
      |   |   |   | tikslas         | integer  |                |                                                                 | param(tikslas)        |
      |   |   |   | vardas          | string   |                |                                                                 | param(vardas)         |
    """
    )


def test_wsdl_with_arrays_and_nested_request(rc: RawConfig):
    manifest = load_manifest(rc, f"{DATA}/atvira_sodra.wsdl")
    assert (
        manifest
        == """
    d | r | b | m | property                   | type             | ref                      | source                                                       | prepare
    atvira_sodra                               |                  |                          |                                                              |
      | wsdl                                   | wsdl             |                          | tests/manifests/wsdl/data/atvira_sodra.wsdl                  |
      | get_chart_list                         | soap             |                          | AtviraPortService.AtviraPortSoap11.AtviraPort.GetChartList   | wsdl(wsdl)
                                               | param            | chart_code               | chartCode                                                    | input()
                                               |                  |                          |                                                              |
      |   |   | Chart/:part                    |                  |                          |                                                              |
      |   |   |   | code                       | string required  |                          | Code                                                         |
      |   |   |   | config_parameters[]        | backref          | ChartConfigParameter     | ConfigParameters                                             | expand()
      |   |   |   | description                | string required  |                          | Description                                                  |
      |   |   |   | get_chart_list_response    | ref              | GetChartListResponse     |                                                              |
      |   |   |   | name                       | string required  |                          | Name                                                         |
                                               |                  |                          |                                                              |
      |   |   | ChartConfigParameter/:part     |                  |                          |                                                              |
      |   |   |   | chart                      | ref              | Chart                    |                                                              |
      |   |   |   | label                      | string required  |                          | Label                                                        |
      |   |   |   | name                       | string required  |                          | Name                                                         |
                                               |                  |                          |                                                              |
      |   |   | GetChartListResponse           |                  |                          | /                                                            |
      |   |   |   | chart_list_result[]        | backref          | Chart                    | ChartListResult                                              | expand()
      |   |   |   | message                    | string required  |                          | Message                                                      |
      |   |   |   | success                    | boolean required |                          | Success                                                      |
      |   |   |   | chart_code                 | string           |                          |                                                              | param(chart_code)
      | get_chart_params                       | soap             |                          | AtviraPortService.AtviraPortSoap11.AtviraPort.GetChartParams | wsdl(wsdl)
                                               | param            | chart_code               | chartCode                                                    | input()
                                               | param            | param_code               | paramCode                                                    | input()
                                               |                  |                          |                                                              |
      |   |   | ChartUniqueDataParameter/:part |                  |                          |                                                              |
      |   |   |   | get_chart_params_response  | ref              | GetChartParamsResponse   |                                                              |
      |   |   |   | name                       | string required  |                          | Name                                                         |
      |   |   |   | value[]                    | string required  |                          | Value                                                        |
                                               |                  |                          |                                                              |
      |   |   | GetChartParamsResponse         |                  |                          | /                                                            |
      |   |   |   | message                    | string required  |                          | Message                                                      |
      |   |   |   | parameter_value[]          | backref required | ChartUniqueDataParameter | ParameterValue                                               | expand()
      |   |   |   | success                    | boolean required |                          | Success                                                      |
      |   |   |   | chart_code                 | string           |                          |                                                              | param(chart_code)
      |   |   |   | param_code                 | string           |                          |                                                              | param(param_code)
      | get_chart_data                         | soap             |                          | AtviraPortService.AtviraPortSoap11.AtviraPort.GetChartData   | wsdl(wsdl)
                                               | param            | chart_code               | chartCode                                                    | input()
                                               | param            | name                     | DataParameter/Name                                           | input()
                                               | param            | value                    | DataParameter/Value                                          | input()
                                               |                  |                          |                                                              |
      |   |   | ChartData/:part                |                  |                          |                                                              |
      |   |   |   | get_chart_data_response    | ref              | GetChartDataResponse     |                                                              |
      |   |   |   | id                         | string required  |                          | Id                                                           |
      |   |   |   | label                      | string required  |                          | Label                                                        |
      |   |   |   | name                       | string required  |                          | Name                                                         |
      |   |   |   | val1                       | string           |                          | Val1                                                         |
      |   |   |   | val10                      | string           |                          | Val10                                                        |
      |   |   |   | val2                       | string           |                          | Val2                                                         |
      |   |   |   | val3                       | string           |                          | Val3                                                         |
      |   |   |   | val4                       | string           |                          | Val4                                                         |
      |   |   |   | val5                       | string           |                          | Val5                                                         |
      |   |   |   | val6                       | string           |                          | Val6                                                         |
      |   |   |   | val7                       | string           |                          | Val7                                                         |
      |   |   |   | val8                       | string           |                          | Val8                                                         |
      |   |   |   | val9                       | string           |                          | Val9                                                         |
                                               |                  |                          |                                                              |
      |   |   | GetChartDataResponse           |                  |                          | /                                                            |
      |   |   |   | chart_data_result[]        | backref          | ChartData                | ChartDataResult                                              | expand()
      |   |   |   | message                    | string required  |                          | Message                                                      |
      |   |   |   | success                    | boolean required |                          | Success                                                      |
      |   |   |   | chart_code                 | string           |                          |                                                              | param(chart_code)
      |   |   |   | name                       | string           |                          |                                                              | param(name)
      |   |   |   | value                      | string           |                          |                                                              | param(value)
    """
    )


def test_wsdl_11(rc: RawConfig):
    # SOAP 1.2 port of the same operations is skipped, operation without response schema still gets a model
    manifest = load_manifest(rc, f"{DATA}/country_11.wsdl")
    assert (
        manifest
        == """
    d | r | b | m | property       | type            | ref  | source                                                | prepare     | title
    country_11                     |                 |      |                                                       |             | CountryService
      | wsdl                       | wsdl            |      | tests/manifests/wsdl/data/country_11.wsdl             |             |
      | get_country                | soap            |      | CountryService.CountryPort.CountryPortType.GetCountry | wsdl(wsdl)  | GetCountry
                                   | param           | code | code                                                  | input()     |
                                   |                 |      |                                                       |             |
      |   |   | GetCountryResponse |                 |      | /                                                     |             |
      |   |   |   | name           | string required |      | name                                                  |             |
      |   |   |   | population     | integer         |      | population                                            |             |
      |   |   |   | code           | string          |      |                                                       | param(code) |
      | ping                       | soap            |      | CountryService.CountryPort.CountryPortType.Ping       | wsdl(wsdl)  | Ping
                                   | param           | host | host                                                  | input()     |
                                   |                 |      |                                                       |             |
      |   |   | Ping               |                 |      | /                                                     |             |
      |   |   |   | host           | string          |      |                                                       | param(host) |
    """
    )


def test_wsdl_20(rc: RawConfig, caplog: pytest.LogCaptureFixture):
    # non-SOAP endpoints are skipped
    with caplog.at_level(logging.WARNING):
        manifest = load_manifest(rc, f"{DATA}/country_20.wsdl")
    assert "soap backend reads only WSDL 1.1" in caplog.text
    assert (
        manifest
        == """
    d | r | b | m | property       | type            | ref  | source                                                     | prepare
    country_20                     |                 |      |                                                            |
      | wsdl                       | wsdl            |      | tests/manifests/wsdl/data/country_20.wsdl                  |
      | get_country                | soap            |      | CountryService.CountryEndpoint.CountryInterface.GetCountry | wsdl(wsdl)
                                   | param           | code | code                                                       | input()
                                   |                 |      |                                                            |
      |   |   | GetCountryResponse |                 |      | /                                                          |
      |   |   |   | name           | string required |      | name                                                       |
      |   |   |   | code           | string          |      |                                                            | param(code)
    """
    )


def test_wsdl_edge_cases(rc: RawConfig, caplog: pytest.LogCaptureFixture):
    # schema annotation, `xs:choice` and `xs:extension` in requests and responses, SOAP header,
    # a response shared by two operations, root element copy of `Item`, RPC parts with simple types
    with caplog.at_level(logging.WARNING):
        manifest = load_manifest(rc, f"{DATA}/edge_cases.wsdl")
    assert "SOAP header 'auth' of WSDL operation 'FindItem' is not generated" in caplog.text
    assert (
        manifest
        == """
    d | r | b | m | property    | type             | ref    | source                                          | prepare              | title
    edge_cases                  |                  |        |                                                 |                      | EdgeCases
      | wsdl                    | wsdl             |        | tests/manifests/wsdl/data/edge_cases.wsdl       |                      |
      | find_item               | soap             |        | EdgeService.EdgePort.EdgePortType.FindItem      | wsdl(wsdl)           | FindItem
                                | param            | lang   | lang                                            | input()              |
                                | param            | id     | id                                              | input()              |
                                | param            | title  | title                                           | input()              |
                                |                  |        |                                                 |                      |
      |   |   | Detail/:part    |                  |        |                                                 |                      |
      |   |   |   | note        | string required  |        | note                                            |                      |
                                |                  |        |                                                 |                      |
      |   |   | Item1           |                  |        | /                                               |                      |
      |   |   |   | code        | string required  |        | code                                            |                      |
      |   |   |   | detail      | ref required     | Detail | detail                                          | expand()             |
      |   |   |   | lang        | string           |        |                                                 | param(lang)          |
      |   |   |   | id          | integer          |        |                                                 | param(id)            |
      |   |   |   | title       | string           |        |                                                 | param(title)         |
      | find_other_item         | soap             |        | EdgeService.EdgePort.EdgePortType.FindOtherItem | wsdl(wsdl)           | FindOtherItem
                                | param            | lang   | lang                                            | input()              |
                                | param            | id     | id                                              | input()              |
                                | param            | title  | title                                           | input()              |
                                |                  |        |                                                 |                      |
      |   |   | FindOtherItem   |                  |        | /                                               |                      |
      |   |   |   | code        | string required  |        | code                                            |                      |
      |   |   |   | detail      | ref required     | Detail | detail                                          | expand()             |
      |   |   |   | lang        | string           |        |                                                 | param(lang)          |
      |   |   |   | id          | integer          |        |                                                 | param(id)            |
      |   |   |   | title       | string           |        |                                                 | param(title)         |
      | list_items              | soap             |        | EdgeService.EdgePort.EdgePortType.ListItems     | wsdl(wsdl)           | ListItems
                                |                  |        |                                                 |                      |
      |   |   | Item/:part      |                  |        |                                                 |                      |
      |   |   |   | code        | string required  |        | code                                            |                      |
      |   |   |   | detail      | ref required     | Detail | detail                                          | expand()             |
                                |                  |        |                                                 |                      |
      |   |   | ItemsResponse   |                  |        | /                                               |                      |
      |   |   |   | count       | integer required |        | count                                           |                      |
      |   |   |   | item        | ref required     | Item   | item                                            | expand()             |
      | last_query              | soap             |        | EdgeService.EdgePort.EdgePortType.LastQuery     | wsdl(wsdl)           | LastQuery
                                |                  |        |                                                 |                      |
      |   |   | BaseQuery/:part |                  |        |                                                 |                      |
      |   |   |   | lang        | string required  |        | lang                                            |                      |
                                |                  |        |                                                 |                      |
      |   |   | Find            |                  |        | /                                               | extends('BaseQuery') |
      |   |   |   | id          | integer required |        | id                                              |                      |
                                |                  |        |                                                 |                      |
      |   |   | Find1           |                  |        | /                                               | extends('BaseQuery') |
      |   |   |   | title       | string required  |        | title                                           |                      |
      | get_rate                | soap             |        | EdgeService.EdgePort.EdgePortType.GetRate       | wsdl(wsdl)           | GetRate
                                | param            | code   | code                                            | input()              |
                                |                  |        |                                                 |                      |
      |   |   | GetRate         |                  |        | /                                               |                      |
      |   |   |   | rate        | number           |        | rate                                            |                      |
      |   |   |   | date        | date             |        | date                                            |                      |
      |   |   |   | code        | string           |        |                                                 | param(code)          |
    """
    )


def test_wsdl_single_value_response(rc: RawConfig, tmp_path: Path, caplog: pytest.LogCaptureFixture):
    path = tmp_path / "service.wsdl"
    path.write_text(
        WSDL_TEMPLATE.format(
            types="",
            messages='<wsdl:message name="Out"><wsdl:part name="result" type="xs:string"/></wsdl:message>',
        )
    )
    with caplog.at_level(logging.WARNING):
        load_manifest(rc, path)
    assert "Response of WSDL operation 'Op' is a single value" in caplog.text


def test_wsdl_type_from_external_schema(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "service.wsdl"
    path.write_text(
        WSDL_TEMPLATE.format(
            types="""
                <xs:schema>
                    <xs:import namespace="http://example.com/other" schemaLocation="other.xsd"/>
                    <xs:element name="OpResponse" type="other:Result"/>
                </xs:schema>
            """,
            messages='<wsdl:message name="Out"><wsdl:part name="body" element="tns:OpResponse"/></wsdl:message>',
        )
    )
    with pytest.raises(WsdlReadError, match="'Result' is not defined in WSDL types"):
        load_manifest(rc, path)


def test_wsdl_invalid_xsd(rc: RawConfig, tmp_path: Path):
    path = tmp_path / "service.wsdl"
    path.write_text(
        WSDL_TEMPLATE.format(
            types="""
                <xs:schema>
                    <xs:element name="OpResponse">
                        <xs:complexType>
                            <xs:sequence>
                                <xs:element name="code" type="xs:string" maxOccurs="invalid"/>
                            </xs:sequence>
                        </xs:complexType>
                    </xs:element>
                </xs:schema>
            """,
            messages='<wsdl:message name="Out"><wsdl:part name="body" element="tns:OpResponse"/></wsdl:message>',
        )
    )
    with pytest.raises(WsdlReadError, match="unsupported XSD in WSDL types"):
        load_manifest(rc, path)


def test_wsdl_cyclic_extension_does_not_hang(tmp_path: Path):
    path = tmp_path / "service.wsdl"
    path.write_text(
        WSDL_TEMPLATE.format(
            types="""
                <xs:schema>
                    <xs:complexType name="A">
                        <xs:complexContent>
                            <xs:extension base="tns:B">
                                <xs:sequence><xs:element name="a" type="xs:string"/></xs:sequence>
                            </xs:extension>
                        </xs:complexContent>
                    </xs:complexType>
                    <xs:complexType name="B">
                        <xs:complexContent>
                            <xs:extension base="tns:A">
                                <xs:sequence><xs:element name="b" type="xs:string"/></xs:sequence>
                            </xs:extension>
                        </xs:complexContent>
                    </xs:complexType>
                    <xs:element name="OpResponse" type="tns:A"/>
                </xs:schema>
            """,
            messages='<wsdl:message name="Out"><wsdl:part name="body" element="tns:OpResponse"/></wsdl:message>',
        )
    )
    models = [data["name"] for _, data in read_schema(str(path)) if data["type"] == "model"]
    assert models == ["service/B", "service/OpResponse"]


WSDL_TEMPLATE = """
<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/" xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
                  xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://example.com/"
                  xmlns:other="http://example.com/other" targetNamespace="http://example.com/">
    <wsdl:types>{types}</wsdl:types>
    {messages}
    <wsdl:portType name="PortType">
        <wsdl:operation name="Op"><wsdl:output message="tns:Out"/></wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="Binding" type="tns:PortType">
        <soap:binding style="rpc" transport="http://schemas.xmlsoap.org/soap/http"/>
        <wsdl:operation name="Op"><wsdl:output><soap:body use="literal"/></wsdl:output></wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="Service">
        <wsdl:port name="Port" binding="tns:Binding"><soap:address location="http://example.com/"/></wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


def test_wsdl_from_url(rc: RawConfig, responses: RequestsMock):
    url = "https://example.com/country/?wsdl"
    with open(f"{DATA}/country_20.wsdl") as f:
        responses.add(GET, url, body=f.read())
    manifest = load_manifest(rc, url)
    assert (
        manifest
        == """
    d | r | b | m | property       | type            | ref  | source                                                     | prepare
    country_service                |                 |      |                                                            |
      | wsdl                       | wsdl            |      | https://example.com/country/?wsdl                          |
      | get_country                | soap            |      | CountryService.CountryEndpoint.CountryInterface.GetCountry | wsdl(wsdl)
                                   | param           | code | code                                                       | input()
                                   |                 |      |                                                            |
      |   |   | GetCountryResponse |                 |      | /                                                          |
      |   |   |   | name           | string required |      | name                                                       |
      |   |   |   | code           | string          |      |                                                            | param(code)
    """
    )


@pytest.mark.parametrize(
    "path, expected",
    [
        ("service.wsdl", True),
        ("SERVICE.WSDL", True),
        ("https://example.com/service?wsdl", True),
        ("https://example.com/service?WSDL", True),
        ("https://example.com/service/?wsdl&token=abc", True),
        ("https://example.com/service.wsdl?token=abc", True),
        ("https://example.com/service.xml", False),
        ("service.xsd", False),
        ("service.xml", False),
    ],
)
def test_is_wsdl_path(path: str, expected: bool):
    assert is_wsdl_path(path) is expected


def test_is_wsdl_path_detects_xml_by_root_element(tmp_path: Path):
    wsdl = tmp_path / "service.xml"
    wsdl.write_text(Path(f"{DATA}/country_11.wsdl").read_text())
    plain = tmp_path / "data.xml"
    plain.write_text("<data><item>1</item></data>")
    assert is_wsdl_path(str(wsdl)) is True
    assert is_wsdl_path(str(plain)) is False


@pytest.mark.parametrize(
    "content",
    [
        '<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">',
        "<root/>",
        '<root xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"/>',
    ],
)
def test_wsdl_invalid_document(rc: RawConfig, tmp_path: Path, content: str):
    path = tmp_path / "service.wsdl"
    path.write_text(content)
    with pytest.raises(WsdlReadError):
        load_manifest(rc, path)


def test_wsdl_missing_file(rc: RawConfig, tmp_path: Path):
    with pytest.raises(WsdlReadError):
        load_manifest(rc, tmp_path / "missing.wsdl")


def test_wsdl_url_not_found(rc: RawConfig, responses: RequestsMock):
    url = "https://example.com/country/?wsdl"
    responses.add(GET, url, status=404)
    with pytest.raises(WsdlReadError):
        load_manifest(rc, url)


def test_wsdl_external_entities_are_not_resolved(rc: RawConfig, tmp_path: Path):
    secret = tmp_path / "secret.txt"
    secret.write_text("secret")
    path = tmp_path / "service.wsdl"
    path.write_text(
        f'<!DOCTYPE d [<!ENTITY secret SYSTEM "{secret.as_uri()}">]>'
        '<wsdl:description xmlns:wsdl="http://www.w3.org/ns/wsdl" name="&secret;"/>'
    )
    with pytest.raises(WsdlReadError):
        load_manifest(rc, path)


def test_wsdl_given_dataset_name(rc: RawConfig):
    manifest = load_manifest(rc.fork({"given_dataset_name": "datasets/gov/example"}), f"{DATA}/country_20.wsdl")
    assert list(manifest.get_objects()["dataset"]) == ["datasets/gov/example"]


def test_copy_wsdl(rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    # `copy` checks names, so array properties must be valid there too
    cli.invoke(rc, ["copy", "-o", tmp_path / "result.csv", f"{DATA}/atvira_sodra.wsdl"])
    result = load_manifest(rc, tmp_path / "result.csv")
    assert "atvira_sodra/GetChartParamsResponse" in result.get_objects()["model"]


def test_copy_invalid_wsdl_without_traceback(rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path):
    path = tmp_path / "service.wsdl"
    path.write_text("<root/>")
    result = cli.invoke(rc, ["copy", "-o", tmp_path / "result.csv", path], fail=False)
    assert result.exit_code == 1
    assert "Can't read WSDL" in result.stderr
    assert "Traceback" not in result.output


def test_copied_wsdl_reads_soap_response(rc: RawConfig, cli: SpintaCliRunner, tmp_path: Path, responses: RequestsMock):
    # zeep returns the only child of a response element, so the generated model must read its fields
    responses.add(
        POST,
        "http://example.com/city",
        status=200,
        content_type="text/xml; charset=utf-8",
        body="""
            <ns0:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="city_app">
                <ns0:Body>
                    <ns1:CityOutputResponse>
                        <ns1:CityOutput><ns1:id>100</ns1:id><ns1:name>Vilnius</ns1:name></ns1:CityOutput>
                        <ns1:CityOutput><ns1:id>101</ns1:id><ns1:name>Kaunas</ns1:name></ns1:CityOutput>
                    </ns1:CityOutputResponse>
                </ns0:Body>
            </ns0:Envelope>
        """,
    )
    cli.invoke(rc, ["copy", "-o", tmp_path / "result.csv", "tests/datasets/backends/wsdl/data/wsdl.xml"])
    context, _ = prepare_manifest(rc, tmp_path / "result.csv", mode=Mode.external)
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("wsdl/CityOutput", ["getall"])

    response = app.get("/wsdl/CityOutput")
    assert listdata(response, "id", "name", sort=False) == [(100, "Vilnius"), (101, "Kaunas")]
