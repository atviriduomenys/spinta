import csv
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.manifest import load_manifest_and_context
from tests.manifests.wsdl.test_wsdl import (
    _build_local_reference_wsdl,
    _build_remote_reference_wsdl,
    COUNTRY_WSDL,
    COUNTRY_WSDL_2_0,
    COUNTRY_WSDL_2_0_NAMESPACE_VARIANT,
    COUNTRY_WSDL_DUPLICATE_EMBEDDED_TYPE,
    COUNTRY_WSDL_DUPLICATE_MESSAGE,
    COUNTRY_WSDL_NAMESPACE_VARIANT,
    COUNTRY_WSDL_WITHOUT_SOAP_ACTION,
    FAULT_WSDL_2_0,
    LOCAL_REFERENCED_RESPONSE_TYPES_XSD,
    MULTI_OPERATION_WSDL,
    MULTI_OPERATION_WSDL_2_0,
    NESTED_TYPES_WSDL_2_0,
    NON_SOAP_BINDING_WSDL_2_0,
    SCALAR_TYPES_WSDL,
    SCALAR_TYPES_WSDL_2_0,
)


def _user_property_names(model) -> set[str]:
    return {name for name in model.properties if not name.startswith("_")}


def test_copy_wsdl_downloads_remote_wsdl_and_generates_manifest(
    rc: RawConfig,
    cli: SpintaCliRunner,
    monkeypatch,
    tmp_path: Path,
):
    raw_url = "wsdl+https://example.com/country?wsdl"
    normalized_url = "https://example.com/country?wsdl"
    output = tmp_path / "result.csv"

    class FakeResponse:
        def __init__(self, data: bytes):
            self._data = data

        def read(self) -> bytes:
            return self._data

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request_url: str):
        parsed = urlparse(request_url)
        assert parsed.scheme == "https"
        assert parsed.netloc == "example.com"
        assert parsed.path == "/country"
        assert parse_qs(parsed.query, keep_blank_values=True) == {"wsdl": [""]}
        return FakeResponse(COUNTRY_WSDL.encode())

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            raw_url,
        ],
    )

    assert output.exists()
    rendered = list(csv.DictReader(output.read_text().splitlines()))
    contract_rows = [row for row in rendered if row["resource"] == "contract"]

    assert len(contract_rows) == 1
    assert contract_rows[0]["resource"] == "contract"
    assert contract_rows[0]["source"] == normalized_url
    assert contract_rows[0]["title"] == "CountryService"

    context, manifest = load_manifest_and_context(rc, output)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == normalized_url

    soap_resource = dataset.resources["country_port_get_country"]
    assert soap_resource.type == "soap"
    assert str(soap_resource.prepare) == "wsdl(contract)"
    assert soap_resource.external == "CountryService.CountryPort.CountryPortType.GetCountry"

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert request_model.external.resource.name == "country_port_get_country"
    assert response_model.external.resource.name == "country_port_get_country"
    assert {"code"}.issubset(request_model.properties)
    assert {"name", "population"}.issubset(response_model.properties)


def test_copy_wsdl_reports_unreachable_remote_source(
    rc: RawConfig,
    cli: SpintaCliRunner,
    monkeypatch,
    tmp_path: Path,
):
    output = tmp_path / "result.csv"

    def fake_urlopen(request_url: str):
        raise URLError("temporary failure in name resolution")

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            "wsdl+https://example.com/country?wsdl",
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "Remote WSDL resource 'https://example.com/country?wsdl' is unavailable." in result.stderr
    assert "temporary failure in name resolution" in result.stderr


def test_copy_wsdl_reports_remote_auth_failure(
    rc: RawConfig,
    cli: SpintaCliRunner,
    monkeypatch,
    tmp_path: Path,
):
    output = tmp_path / "result.csv"
    headers = Message()

    def fake_urlopen(request_url: str):
        raise HTTPError(request_url, 401, "Unauthorized", hdrs=headers, fp=None)

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            "wsdl+https://example.com/country?wsdl",
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert (
        "Authentication failed while reading remote WSDL resource 'https://example.com/country?wsdl'." in result.stderr
    )
    assert "HTTP status: 401." in result.stderr
    assert "Unauthorized" in result.stderr


def test_copy_wsdl_reports_remote_not_found(
    rc: RawConfig,
    cli: SpintaCliRunner,
    monkeypatch,
    tmp_path: Path,
):
    output = tmp_path / "result.csv"
    headers = Message()

    def fake_urlopen(request_url: str):
        raise HTTPError(request_url, 404, "Not Found", hdrs=headers, fp=None)

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            "wsdl+https://example.com/country?wsdl",
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "Remote WSDL resource 'https://example.com/country?wsdl' is unavailable." in result.stderr
    assert "HTTP status: 404. Not Found" in result.stderr


def test_copy_wsdl_reports_remote_server_error(
    rc: RawConfig,
    cli: SpintaCliRunner,
    monkeypatch,
    tmp_path: Path,
):
    output = tmp_path / "result.csv"
    headers = Message()

    def fake_urlopen(request_url: str):
        raise HTTPError(request_url, 500, "Internal Server Error", hdrs=headers, fp=None)

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            "wsdl+https://example.com/country?wsdl",
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "Remote WSDL server error while reading resource 'https://example.com/country?wsdl'." in result.stderr
    assert "HTTP status: 500." in result.stderr
    assert "Internal Server Error" in result.stderr


def test_copy_wsdl_reports_malformed_local_xml_without_traceback(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "broken.wsdl"
    output = tmp_path / "result.csv"
    path.write_text(
        """
        <wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/">
            <wsdl:types>
                <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            </wsdl:types>
        </wsdl:definitions>
        """
    )

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert f"Malformed WSDL XML in '{path}'." in result.stderr
    assert "mismatched tag" in result.stderr
    assert "Traceback" not in result.stderr


def test_copy_wsdl_reports_malformed_remote_xml_without_traceback(
    rc: RawConfig,
    cli: SpintaCliRunner,
    monkeypatch,
    tmp_path: Path,
):
    output = tmp_path / "result.csv"
    url = "wsdl+https://example.com/broken?wsdl"
    normalized_url = "https://example.com/broken?wsdl"

    class FakeResponse:
        def __init__(self, data: bytes):
            self._data = data

        def read(self) -> bytes:
            return self._data

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request_url: str):
        assert request_url == normalized_url
        return FakeResponse(
            b'<wsdl:definitions xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"><wsdl:types></wsdl:definitions>'
        )

    monkeypatch.setattr("spinta.manifests.wsdl.helpers.urllib.request.urlopen", fake_urlopen)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            url,
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert f"Malformed WSDL XML in '{normalized_url}'." in result.stderr
    assert "mismatched tag" in result.stderr
    assert "Traceback" not in result.stderr


def test_copy_wsdl_warns_on_missing_referenced_schema_but_writes_output(
    rc: RawConfig,
    cli: SpintaCliRunner,
    monkeypatch,
    tmp_path: Path,
):
    path = tmp_path / "country-remote-ref.wsdl"
    output = tmp_path / "result.csv"
    schema_url = "https://example.com/country-types.xsd"
    path.write_text(_build_remote_reference_wsdl(schema_url))

    def fail_urlopen(request_url: str):
        raise URLError("temporary failure in name resolution")

    monkeypatch.setattr("spinta.manifests.wsdl.xsd.helpers.urlopen", fail_urlopen)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
        fail=False,
    )

    assert result.exit_code == 0
    assert output.exists()
    assert f"Warning: Skipped referenced schema while reading WSDL '{path}'." in result.stderr
    assert "Remote schema resource 'https://example.com/country-types.xsd' is unavailable." in result.stderr

    context, manifest = load_manifest_and_context(rc, output)
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert _user_property_names(request_model) == set()
    assert _user_property_names(response_model) == set()


def test_copy_wsdl_warns_on_malformed_referenced_schema_but_writes_output(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country-malformed-ref.wsdl"
    referenced_schema = tmp_path / "country-types.xsd"
    output = tmp_path / "result.csv"
    path.write_text(_build_local_reference_wsdl(referenced_schema.name))
    referenced_schema.write_text('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element')

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
        fail=False,
    )

    assert result.exit_code == 0
    assert output.exists()
    assert f"Warning: Skipped referenced schema while reading WSDL '{path}'." in result.stderr
    assert f"Malformed referenced schema XML in '{referenced_schema.resolve()}'" in result.stderr

    context, manifest = load_manifest_and_context(rc, output)
    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert _user_property_names(request_model) == set()
    assert _user_property_names(response_model) == set()


def test_copy_wsdl_reports_namespace_ambiguity_without_traceback(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country-duplicate-message.wsdl"
    output = tmp_path / "result.csv"
    path.write_text(COUNTRY_WSDL_DUPLICATE_MESSAGE)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "Ambiguous WSDL QName" in result.stderr
    assert "GetCountryInput" in result.stderr
    assert "Duplicate expanded QName conflict" in result.stderr
    assert "Traceback" not in result.stderr


def test_copy_wsdl_reports_embedded_schema_ambiguity_without_traceback(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country-duplicate-embedded-type.wsdl"
    output = tmp_path / "result.csv"
    path.write_text(COUNTRY_WSDL_DUPLICATE_EMBEDDED_TYPE)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
        fail=False,
    )

    assert result.exit_code == 1
    assert "Ambiguous WSDL QName" in result.stderr
    assert "GetCountryResponse" in result.stderr
    assert "multiple embedded schema model candidates" in result.stderr
    assert "Traceback" not in result.stderr


def test_copy_wsdl_referenced_schema_output_is_loadable(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country-local-ref.wsdl"
    referenced_schema = tmp_path / "country-types.xsd"
    output = tmp_path / "result.csv"
    path.write_text(_build_local_reference_wsdl(referenced_schema.name))
    referenced_schema.write_text(LOCAL_REFERENCED_RESPONSE_TYPES_XSD)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    contract_rows = [row for row in rendered if row["resource"] == "contract"]

    assert len(contract_rows) == 1
    assert contract_rows[0]["source"] == str(path)
    assert contract_rows[0]["title"] == "CountryService"

    context, manifest = load_manifest_and_context(rc, output)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == str(path)

    soap_resource = dataset.resources["country_port_get_country"]
    assert soap_resource.type == "soap"
    assert str(soap_resource.prepare) == "wsdl(contract)"
    assert soap_resource.external == "CountryService.CountryPort.CountryPortType.GetCountry"

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert request_model.external.resource.name == "country_port_get_country"
    assert response_model.external.resource.name == "country_port_get_country"
    assert {"code"}.issubset(request_model.properties)
    assert {"name", "population"}.issubset(response_model.properties)
    assert response_model.properties["name"].dtype.name == "string"
    assert response_model.properties["population"].dtype.name == "integer"
    assert response_model.properties["population"].dtype.required is False


def test_copy_wsdl_generates_loadable_dsa_manifest(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country.wsdl"
    output = tmp_path / "result.csv"
    path.write_text(COUNTRY_WSDL)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    context, manifest = load_manifest_and_context(rc, output)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == str(path)

    soap_resource = dataset.resources["country_port_get_country"]
    assert soap_resource.type == "soap"
    assert str(soap_resource.prepare) == "wsdl(contract)"
    assert soap_resource.external == "CountryService.CountryPort.CountryPortType.GetCountry"

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert request_model.external.resource.name == "country_port_get_country"
    assert response_model.external.resource.name == "country_port_get_country"
    assert {"code"}.issubset(request_model.properties)
    assert {"name", "population"}.issubset(response_model.properties)


def test_copy_wsdl_2_0_generates_loadable_dsa_manifest(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country-v2.wsdl"
    output = tmp_path / "result-v2.csv"
    path.write_text(COUNTRY_WSDL_2_0)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    contract_rows = [row for row in rendered if row["resource"] == "contract"]

    assert len(contract_rows) == 1
    assert contract_rows[0]["source"] == str(path)
    assert contract_rows[0]["title"] == "CountryService"
    assert any(row["resource"] == "country_endpoint_get_country" for row in rendered)

    context, manifest = load_manifest_and_context(rc, output)

    dataset = commands.get_dataset(context, manifest, "services/country_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == str(path)

    soap_resource = dataset.resources["country_endpoint_get_country"]
    assert soap_resource.type == "soap"
    assert str(soap_resource.prepare) == "wsdl(contract)"
    assert soap_resource.external == "CountryService.CountryEndpoint.CountryInterface.GetCountry"

    request_model = commands.get_model(context, manifest, "services/country_service/GetCountryRequest")
    response_model = commands.get_model(context, manifest, "services/country_service/GetCountryResponse")

    assert request_model.external.resource.name == "country_endpoint_get_country"
    assert response_model.external.resource.name == "country_endpoint_get_country"
    assert request_model.external.name == "GetCountryRequest"
    assert response_model.external.name == "GetCountryResponse"
    assert {"code"}.issubset(request_model.properties)
    assert {"name", "population"}.issubset(response_model.properties)


@pytest.mark.parametrize(
    "filename, output_name, wsdl, expected_resources",
    [
        (
            "registry.wsdl",
            "result.csv",
            MULTI_OPERATION_WSDL,
            {
                "registry_port_get_country": "RegistryService.RegistryPort.RegistryPortType.GetCountry",
                "registry_port_list_countries": "RegistryService.RegistryPort.RegistryPortType.ListCountries",
            },
        ),
        (
            "registry-v2.wsdl",
            "result-v2.csv",
            MULTI_OPERATION_WSDL_2_0,
            {
                "registry_endpoint_get_country": "RegistryService.RegistryEndpoint.RegistryInterface.GetCountry",
                "registry_endpoint_list_countries": "RegistryService.RegistryEndpoint.RegistryInterface.ListCountries",
            },
        ),
    ],
)
def test_copy_wsdl_operation_structure_is_preserved_per_operation(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    filename: str,
    output_name: str,
    wsdl: str,
    expected_resources: dict[str, str],
):
    path = tmp_path / filename
    output = tmp_path / output_name
    path.write_text(wsdl)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    resources = {row["resource"] for row in rendered if row["resource"]}
    sources = {row["source"] for row in rendered if row["source"]}

    assert set(expected_resources).issubset(resources)
    assert set(expected_resources.values()).issubset(sources)
    assert {
        "GetCountryRequest",
        "GetCountryResponse",
        "ListCountriesRequest",
        "ListCountriesResponse",
    }.issubset(sources)

    context, manifest = load_manifest_and_context(rc, output)

    dataset = commands.get_dataset(context, manifest, "services/registry_service")
    assert dataset.resources["contract"].type == "wsdl"
    assert dataset.resources["contract"].external == str(path)

    for resource_name, resource_external in expected_resources.items():
        resource = dataset.resources[resource_name]
        assert resource.type == "soap"
        assert resource.external == resource_external

    expected_model_resources = {
        "GetCountryRequest": next(name for name in expected_resources if name.endswith("get_country")),
        "GetCountryResponse": next(name for name in expected_resources if name.endswith("get_country")),
        "ListCountriesRequest": next(name for name in expected_resources if name.endswith("list_countries")),
        "ListCountriesResponse": next(name for name in expected_resources if name.endswith("list_countries")),
    }
    expected_property_sources = {
        "GetCountryRequest": {"country_code": "countryCode"},
        "GetCountryResponse": {"country_name": "countryName"},
        "ListCountriesRequest": {"region_code": "regionCode"},
        "ListCountriesResponse": {
            "result_count": "resultCount",
            "status_text": "statusText",
        },
    }

    for model_name, resource_name in expected_model_resources.items():
        model = commands.get_model(context, manifest, f"services/registry_service/{model_name}")
        assert model.external.resource.name == resource_name
        assert model.external.name == model_name

        for property_name, property_source in expected_property_sources[model_name].items():
            assert model.properties[property_name].external.name == property_source


@pytest.mark.parametrize(
    "filename, output_name, wsdl, expected_resource_params",
    [
        (
            "registry.wsdl",
            "result.csv",
            MULTI_OPERATION_WSDL,
            {
                "registry_port_get_country": {
                    "style": "document",
                    "transport": "http://schemas.xmlsoap.org/soap/http",
                    "address": "https://example.com/registry",
                    "soapAction": "urn:registry#GetCountry",
                },
                "registry_port_list_countries": {
                    "style": "document",
                    "transport": "http://schemas.xmlsoap.org/soap/http",
                    "address": "https://example.com/registry",
                    "soapAction": "urn:registry#ListCountries",
                },
            },
        ),
        (
            "registry-v2.wsdl",
            "result-v2.csv",
            MULTI_OPERATION_WSDL_2_0,
            {
                "registry_endpoint_get_country": {
                    "protocol": "http://www.w3.org/2003/05/soap/bindings/HTTP/",
                    "address": "https://example.com/registry",
                },
                "registry_endpoint_list_countries": {
                    "protocol": "http://www.w3.org/2003/05/soap/bindings/HTTP/",
                    "address": "https://example.com/registry",
                },
            },
        ),
    ],
)
def test_copy_wsdl_soap_binding_metadata_is_rendered_into_dsa_output(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    filename: str,
    output_name: str,
    wsdl: str,
    expected_resource_params: dict[str, dict[str, str]],
):
    path = tmp_path / filename
    output = tmp_path / output_name
    path.write_text(wsdl)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    current_resource = None
    rendered_resource_params: dict[str, dict[str, dict[str, str]]] = {}
    for row in rendered:
        if row["resource"]:
            current_resource = row["resource"]
        if row["type"] == "param" and current_resource in expected_resource_params:
            rendered_resource_params.setdefault(current_resource, {})[row["ref"]] = row

    assert set(rendered_resource_params) == set(expected_resource_params)
    for resource_name, expected_params in expected_resource_params.items():
        assert set(rendered_resource_params[resource_name]) == set(expected_params)
        for param_name, expected_value in expected_params.items():
            row = rendered_resource_params[resource_name][param_name]
            assert row["source"] == expected_value
            assert row["prepare"] == ""

    context, manifest = load_manifest_and_context(rc, output)
    dataset = commands.get_dataset(context, manifest, "services/registry_service")
    for resource_name, expected_params in expected_resource_params.items():
        resource = dataset.resources[resource_name]
        params = {param.name: param for param in resource.params}
        assert set(params) == set(expected_params)
        for param_name, expected_value in expected_params.items():
            assert params[param_name].source == [expected_value]


@pytest.mark.parametrize(
    "filename, output_name, wsdl, expected_resource_addresses",
    [
        (
            "registry.wsdl",
            "result.csv",
            MULTI_OPERATION_WSDL,
            {
                "registry_port_get_country": "https://example.com/registry",
                "registry_port_list_countries": "https://example.com/registry",
            },
        ),
        (
            "registry-v2.wsdl",
            "result-v2.csv",
            MULTI_OPERATION_WSDL_2_0,
            {
                "registry_endpoint_get_country": "https://example.com/registry",
                "registry_endpoint_list_countries": "https://example.com/registry",
            },
        ),
    ],
)
def test_copy_wsdl_endpoint_urls_are_preserved_for_supported_soap_resources(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    filename: str,
    output_name: str,
    wsdl: str,
    expected_resource_addresses: dict[str, str],
):
    path = tmp_path / filename
    output = tmp_path / output_name
    path.write_text(wsdl)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    current_resource = None
    rendered_addresses: dict[str, str] = {}
    for row in rendered:
        if row["resource"]:
            current_resource = row["resource"]
        if (
            row["type"] == "param"
            and current_resource is not None
            and row["ref"] == "address"
            and current_resource in expected_resource_addresses
        ):
            rendered_addresses[current_resource] = row["source"]
            assert row["prepare"] == ""

    assert rendered_addresses == expected_resource_addresses

    context, manifest = load_manifest_and_context(rc, output)
    dataset = commands.get_dataset(context, manifest, "services/registry_service")

    for resource_name, expected_address in expected_resource_addresses.items():
        resource = dataset.resources[resource_name]
        params = {param.name: param for param in resource.params}

        assert params["address"].source == [expected_address]


def test_copy_wsdl_optional_soap_action_is_omitted_when_not_declared(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country-no-soap-action.wsdl"
    output = tmp_path / "result.csv"
    path.write_text(COUNTRY_WSDL_WITHOUT_SOAP_ACTION)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    current_resource = None
    rendered_resource_params: dict[str, dict[str, dict[str, str]]] = {}
    for row in rendered:
        if row["resource"]:
            current_resource = row["resource"]
        if row["type"] == "param" and current_resource == "country_port_get_country":
            rendered_resource_params.setdefault(current_resource, {})[row["ref"]] = row

    assert set(rendered_resource_params) == {"country_port_get_country"}
    assert set(rendered_resource_params["country_port_get_country"]) == {"style", "transport", "address"}
    assert rendered_resource_params["country_port_get_country"]["style"]["source"] == "document"
    assert (
        rendered_resource_params["country_port_get_country"]["transport"]["source"]
        == "http://schemas.xmlsoap.org/soap/http"
    )
    assert rendered_resource_params["country_port_get_country"]["address"]["source"] == "https://example.com/country"
    assert "soapAction" not in rendered_resource_params["country_port_get_country"]

    context, manifest = load_manifest_and_context(rc, output)
    dataset = commands.get_dataset(context, manifest, "services/country_service")
    resource = dataset.resources["country_port_get_country"]
    params = {param.name: param for param in resource.params}

    assert set(params) == {"style", "transport", "address"}
    assert params["style"].source == ["document"]
    assert params["transport"].source == ["http://schemas.xmlsoap.org/soap/http"]
    assert params["address"].source == ["https://example.com/country"]
    assert "soapAction" not in params


@pytest.mark.parametrize(
    "filename, output_name, wsdl, resource_name, expected_external, expected_params",
    [
        (
            "country-namespace-variant.wsdl",
            "result.csv",
            COUNTRY_WSDL_NAMESPACE_VARIANT,
            "country_port_get_country",
            "CountryService.CountryPort.CountryPortType.GetCountry",
            {
                "style": "document",
                "transport": "http://schemas.xmlsoap.org/soap/http",
                "address": "https://example.com/country",
                "soapAction": "urn:country#GetCountry",
            },
        ),
        (
            "country-v2-namespace-variant.wsdl",
            "result-v2.csv",
            COUNTRY_WSDL_2_0_NAMESPACE_VARIANT,
            "country_endpoint_get_country",
            "CountryService.CountryEndpoint.CountryInterface.GetCountry",
            {
                "protocol": "http://www.w3.org/2003/05/soap/bindings/HTTP/",
                "address": "https://example.com/country",
            },
        ),
    ],
)
def test_copy_wsdl_soap_namespace_handling_is_preserved_for_supported_bindings(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    filename: str,
    output_name: str,
    wsdl: str,
    resource_name: str,
    expected_external: str,
    expected_params: dict[str, str],
):
    path = tmp_path / filename
    output = tmp_path / output_name
    path.write_text(wsdl)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    current_resource = None
    rendered_resource_params: dict[str, dict[str, dict[str, str]]] = {}
    for row in rendered:
        if row["resource"]:
            current_resource = row["resource"]
        if row["type"] == "param" and current_resource is not None and current_resource == resource_name:
            rendered_resource_params.setdefault(current_resource, {})[row["ref"]] = row

    assert any(row["resource"] == resource_name and row["source"] == expected_external for row in rendered)
    assert set(rendered_resource_params) == {resource_name}
    assert set(rendered_resource_params[resource_name]) == set(expected_params)
    for param_name, expected_value in expected_params.items():
        assert rendered_resource_params[resource_name][param_name]["source"] == expected_value

    context, manifest = load_manifest_and_context(rc, output)
    dataset = commands.get_dataset(context, manifest, "services/country_service")
    resource = dataset.resources[resource_name]
    params = {param.name: param for param in resource.params}

    assert resource.type == "soap"
    assert resource.external == expected_external
    assert set(params) == set(expected_params)
    for param_name, expected_value in expected_params.items():
        assert params[param_name].source == [expected_value]


@pytest.mark.parametrize(
    "filename, output_name, wsdl, expected_resource",
    [
        (
            "scalars.wsdl",
            "result.csv",
            SCALAR_TYPES_WSDL,
            "scalar_port_get_scalar",
        ),
        (
            "scalars-v2.wsdl",
            "result-v2.csv",
            SCALAR_TYPES_WSDL_2_0,
            "scalar_endpoint_get_scalar",
        ),
    ],
)
def test_copy_wsdl_soap_happy_path_still_passes_after_structure_contract_tightening(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
    filename: str,
    output_name: str,
    wsdl: str,
    expected_resource: str,
):
    path = tmp_path / filename
    output = tmp_path / output_name
    path.write_text(wsdl)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    assert result.exit_code == 0
    assert "Traceback" not in result.stderr
    assert output.exists()

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    assert rendered
    assert any(row["resource"] == "contract" for row in rendered)
    assert any(row["resource"] == expected_resource for row in rendered)
    assert any(row["property"] == "website" and row["type"] == "url required" for row in rendered)


def test_copy_wsdl_2_0_renders_nested_fields_into_dsa_csv(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "nested-v2.wsdl"
    output = tmp_path / "nested-v2.csv"
    path.write_text(NESTED_TYPES_WSDL_2_0)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    rendered_types = {
        row["property"]: row["type"] for row in rendered if row["property"] in {"location_city", "location_zip"}
    }
    rendered_sources = {
        row["property"]: row["source"] for row in rendered if row["property"] in {"location_city", "location_zip"}
    }

    assert rendered_types == {
        "location_city": "string",
        "location_zip": "integer",
    }
    assert rendered_sources == {
        "location_city": "location/city",
        "location_zip": "location/zip",
    }

    context, manifest = load_manifest_and_context(rc, output)

    response_model = commands.get_model(context, manifest, "services/nested_service/GetNestedResponse")
    assert {"location_city", "location_zip"}.issubset(response_model.properties)
    assert response_model.properties["location_city"].external.name == "location/city"
    assert response_model.properties["location_zip"].external.name == "location/zip"
    assert response_model.properties["location_city"].dtype.required is False
    assert response_model.properties["location_zip"].dtype.required is False


@pytest.mark.xfail(reason="Advanced WSDL 2.0 fault coverage is deferred to backlog scope.", strict=False)
def test_copy_wsdl_2_0_faults_generate_explicit_fault_model(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "faults-v2.wsdl"
    output = tmp_path / "faults-v2.csv"
    path.write_text(FAULT_WSDL_2_0)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    context, manifest = load_manifest_and_context(rc, output)

    fault_model = commands.get_model(context, manifest, "services/fault_service/GetCountryFault")
    assert fault_model.external.name == "GetCountryFault"
    assert {"code", "message"}.issubset(fault_model.properties)


@pytest.mark.xfail(reason="Advanced WSDL 2.0 non-SOAP binding behavior is deferred to backlog scope.", strict=False)
def test_copy_wsdl_2_0_non_soap_binding_has_explicit_behavior(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "http-binding-v2.wsdl"
    output = tmp_path / "http-binding-v2.csv"
    path.write_text(NON_SOAP_BINDING_WSDL_2_0)

    result = cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
        fail=False,
    )

    assert result.exit_code != 0
    assert "unsupported" in result.stderr.lower()


def test_copy_wsdl_renders_xsd_scalar_types_into_dsa_csv(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "scalars.wsdl"
    output = tmp_path / "result.csv"
    path.write_text(SCALAR_TYPES_WSDL)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    rendered = list(csv.DictReader(output.read_text().splitlines()))
    expected_types = {
        "name": "string required",
        "enabled": "boolean required",
        "created_on": "date required",
        "updated_at": "datetime required",
        "opens_at": "time required",
        "score": "number required",
        "count": "integer required",
        "website": "url required",
    }
    rendered_types = {row["property"]: row["type"] for row in rendered if row["property"] in expected_types}
    rendered_sources = {row["property"]: row["source"] for row in rendered if row["property"] in expected_types}

    assert rendered_types == expected_types
    assert rendered_sources == {name: name for name in expected_types}

    context, manifest = load_manifest_and_context(rc, output)

    response_model = commands.get_model(context, manifest, "services/scalar_service/GetScalarResponse")
    assert response_model.properties["name"].dtype.name == "string"
    assert response_model.properties["enabled"].dtype.name == "boolean"
    assert response_model.properties["created_on"].dtype.name == "date"
    assert response_model.properties["updated_at"].dtype.name == "datetime"
    assert response_model.properties["opens_at"].dtype.name == "time"
    assert response_model.properties["score"].dtype.name == "number"
    assert response_model.properties["count"].dtype.name == "integer"
    assert response_model.properties["website"].dtype.name == "url"


def test_copy_wsdl_generated_manifest_passes_check_cli(
    rc: RawConfig,
    cli: SpintaCliRunner,
    tmp_path: Path,
):
    path = tmp_path / "country.wsdl"
    output = tmp_path / "result.csv"
    config_path = tmp_path / "config"
    config_path.mkdir()
    rc = rc.fork({"config_path": config_path, "default_auth_client": None})
    path.write_text(COUNTRY_WSDL)

    cli.invoke(
        rc,
        [
            "copy",
            "-o",
            output,
            path,
        ],
    )

    result = cli.invoke(
        rc,
        [
            "check",
            output,
        ],
    )

    assert result.exit_code == 0
