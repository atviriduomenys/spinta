from pathlib import Path

import pytest
from responses import GET, RequestsMock

from spinta.core.config import RawConfig
from spinta.exceptions import WsdlReadError
from spinta.manifests.wsdl.helpers import is_wsdl_path
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest

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


def test_wsdl_20(rc: RawConfig):
    # non-SOAP endpoints are skipped
    manifest = load_manifest(rc, f"{DATA}/country_20.wsdl")
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
