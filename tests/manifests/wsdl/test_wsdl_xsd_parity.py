from pathlib import Path
from textwrap import dedent

from spinta import commands
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from tests.manifests.wsdl.test_wsdl import COUNTRY_WSDL_DEFAULT_NAMESPACE_SCHEMA_TYPES, COUNTRY_WSDL_QNAME_PREFIX_VARIANT, NESTED_TYPES_WSDL, SCALAR_TYPES_WSDL


SCALAR_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:scalars" xmlns:tns="urn:scalars">
    <xs:element name="GetScalarRequest">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="id" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="GetScalarResponse">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="name" type="xs:string" />
                <xs:element name="enabled" type="xs:boolean" />
                <xs:element name="created_on" type="xs:date" />
                <xs:element name="updated_at" type="xs:dateTime" />
                <xs:element name="opens_at" type="xs:time" />
                <xs:element name="score" type="xs:decimal" />
                <xs:element name="count" type="xs:int" />
                <xs:element name="website" type="xs:anyURI" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""


NESTED_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:nested" xmlns:tns="urn:nested">
    <xs:element name="GetNestedRequest">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="code" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="GetNestedResponse">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="location" minOccurs="0">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="city" type="xs:string" />
                            <xs:element name="zip" type="xs:int" />
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""


COUNTRY_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country" xmlns:tns="urn:country">
    <xs:element name="GetCountryRequest">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="code" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="GetCountryResponse">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="name" type="xs:string" />
                <xs:element name="population" type="xs:int" minOccurs="0" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
</xs:schema>
"""


COUNTRY_DEFAULT_NAMESPACE_TYPES_XSD = """
<xs:schema xmlns="urn:country" xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:country" elementFormDefault="qualified">
    <xs:complexType name="GetCountryResponseType">
        <xs:sequence>
            <xs:element name="name" type="xs:string" />
            <xs:element name="population" type="xs:int" minOccurs="0" />
        </xs:sequence>
    </xs:complexType>
    <xs:element name="GetCountryRequest">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="code" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="GetCountryResponse" type="GetCountryResponseType" />
</xs:schema>
"""


REFERENCED_REQUIRED_TYPES_XSD = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:tns="urn:ref" targetNamespace="urn:ref">
    <xs:complexType name="AddressType">
        <xs:sequence>
            <xs:element name="city" type="xs:string" />
            <xs:element name="zip" type="xs:int" minOccurs="0" />
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="ResultType">
        <xs:sequence>
            <xs:element name="status" type="xs:string" />
            <xs:element name="address" type="tns:AddressType" />
        </xs:sequence>
    </xs:complexType>
    <xs:element name="GetRefRequest">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="id" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
    </xs:element>
    <xs:element name="GetRefResponse" type="tns:ResultType" />
</xs:schema>
"""


REFERENCED_REQUIRED_TYPES_WSDL = """
<wsdl:definitions
    xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"
    xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:tns="urn:ref"
    targetNamespace="urn:ref"
    name="RefService"
>
    <wsdl:types>
        <xs:schema targetNamespace="urn:ref">
            <xs:complexType name="AddressType">
                <xs:sequence>
                    <xs:element name="city" type="xs:string" />
                    <xs:element name="zip" type="xs:int" minOccurs="0" />
                </xs:sequence>
            </xs:complexType>
            <xs:complexType name="ResultType">
                <xs:sequence>
                    <xs:element name="status" type="xs:string" />
                    <xs:element name="address" type="tns:AddressType" />
                </xs:sequence>
            </xs:complexType>
            <xs:element name="GetRefRequest">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="id" type="xs:string" />
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
            <xs:element name="GetRefResponse" type="tns:ResultType" />
        </xs:schema>
    </wsdl:types>
    <wsdl:message name="GetRefInput">
        <wsdl:part name="parameters" element="tns:GetRefRequest" />
    </wsdl:message>
    <wsdl:message name="GetRefOutput">
        <wsdl:part name="parameters" element="tns:GetRefResponse" />
    </wsdl:message>
    <wsdl:portType name="RefPortType">
        <wsdl:operation name="GetRef">
            <wsdl:input message="tns:GetRefInput" />
            <wsdl:output message="tns:GetRefOutput" />
        </wsdl:operation>
    </wsdl:portType>
    <wsdl:binding name="RefBinding" type="tns:RefPortType">
        <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http" />
        <wsdl:operation name="GetRef">
            <soap:operation soapAction="urn:ref#GetRef" />
        </wsdl:operation>
    </wsdl:binding>
    <wsdl:service name="RefService">
        <wsdl:port name="RefPort" binding="tns:RefBinding">
            <soap:address location="https://example.com/ref" />
        </wsdl:port>
    </wsdl:service>
</wsdl:definitions>
"""


REF_DTYPES = {"ref", "backref", "ref_backref"}


def test_wsdl_scalar_schema_output_matches_xsd_and_xsd2(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(tmp_path, "scalar", SCALAR_TYPES_XSD, SCALAR_TYPES_WSDL)

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/scalar_service/GetScalarResponse")
    xsd_model = commands.get_model(xsd_context, xsd_manifest, "scalar/GetScalarResponse")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "scalar/GetScalarResponse")

    wsdl_snapshot = _canonical_public_properties(wsdl_model)
    xsd_snapshot = _canonical_public_properties(xsd_model)
    xsd2_snapshot = _canonical_public_properties(xsd2_model)

    assert wsdl_snapshot == xsd_snapshot == xsd2_snapshot
    assert wsdl_snapshot["website"]["type"] == "url"


def test_wsdl_raw_scalar_schema_output_matches_xsd_and_xsd2(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(tmp_path, "scalar", SCALAR_TYPES_XSD, SCALAR_TYPES_WSDL)

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/scalar_service/schema/GetScalarResponse")
    xsd_model = commands.get_model(xsd_context, xsd_manifest, "scalar/GetScalarResponse")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "scalar/GetScalarResponse")

    wsdl_snapshot = _canonical_public_properties(wsdl_model)
    xsd_snapshot = _canonical_public_properties(xsd_model)
    xsd2_snapshot = _canonical_public_properties(xsd2_model)

    assert wsdl_snapshot == xsd_snapshot == xsd2_snapshot
    assert wsdl_snapshot["website"]["type"] == "url"
    assert wsdl_model.external.name == "/GetScalarResponse"
    assert xsd_model.external.name == "/GetScalarResponse"
    assert xsd2_model.external.name == "/GetScalarResponse"


def test_wsdl_nested_schema_output_matches_xsd_pipelines(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(tmp_path, "nested", NESTED_TYPES_XSD, NESTED_TYPES_WSDL)

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/nested_service/GetNestedResponse")
    xsd_location_model = commands.get_model(xsd_context, xsd_manifest, "nested/Location")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "nested/GetNestedResponse")

    wsdl_snapshot = _canonical_public_properties(wsdl_model)
    xsd_snapshot = _canonical_public_properties(
        xsd_location_model,
        name_prefix="location",
        source_prefix="location",
        parent_required=False,
    )
    xsd2_snapshot = _canonical_public_properties(xsd2_model)

    assert wsdl_snapshot == {
        "location_city": {"type": "string", "required": False, "source": "location/city"},
        "location_zip": {"type": "integer", "required": False, "source": "location/zip"},
    }
    assert wsdl_snapshot == xsd_snapshot == xsd2_snapshot
    assert xsd2_model.properties["location"].dtype.name == "ref"
    assert xsd2_model.properties["location"].dtype.model.name == "nested/Location"


def test_wsdl_raw_nested_schema_output_matches_xsd_pipelines(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(tmp_path, "nested", NESTED_TYPES_XSD, NESTED_TYPES_WSDL)

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/nested_service/schema/GetNestedResponse")
    wsdl_location_model = commands.get_model(wsdl_context, wsdl_manifest, "services/nested_service/schema/Location")
    xsd_location_model = commands.get_model(xsd_context, xsd_manifest, "nested/Location")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "nested/GetNestedResponse")
    xsd2_location_model = commands.get_model(xsd2_context, xsd2_manifest, "nested/Location")

    wsdl_snapshot = _canonical_public_properties(wsdl_model)
    xsd_snapshot = _canonical_public_properties(
        xsd_location_model,
        name_prefix="location",
        source_prefix="location",
        parent_required=False,
    )
    xsd2_snapshot = _canonical_public_properties(xsd2_model)
    wsdl_location_snapshot = _canonical_public_properties(wsdl_location_model)
    xsd2_location_snapshot = _canonical_public_properties(xsd2_location_model)

    assert wsdl_snapshot == {
        "location_city": {"type": "string", "required": False, "source": "location/city"},
        "location_zip": {"type": "integer", "required": False, "source": "location/zip"},
    }
    assert wsdl_snapshot == xsd_snapshot == xsd2_snapshot
    assert wsdl_location_snapshot == xsd2_location_snapshot == {
        "city": {"type": "string", "required": True, "source": "city"},
        "zip": {"type": "integer", "required": True, "source": "zip"},
    }
    assert wsdl_model.properties["location"].dtype.name == "ref"
    assert wsdl_model.properties["location"].dtype.model.name == "services/nested_service/schema/Location"
    assert xsd2_model.properties["location"].dtype.model.name == "nested/Location"


def test_wsdl_namespace_aware_schema_output_matches_xsd_and_xsd2(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(
        tmp_path,
        "country-namespace-aware",
        COUNTRY_TYPES_XSD,
        COUNTRY_WSDL_QNAME_PREFIX_VARIANT,
    )

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_request = commands.get_model(wsdl_context, wsdl_manifest, "services/country_service/GetCountryRequest")
    wsdl_response = commands.get_model(wsdl_context, wsdl_manifest, "services/country_service/GetCountryResponse")
    xsd_request = commands.get_model(xsd_context, xsd_manifest, "country_namespace_aware/GetCountryRequest")
    xsd_response = commands.get_model(xsd_context, xsd_manifest, "country_namespace_aware/GetCountryResponse")
    xsd2_request = commands.get_model(xsd2_context, xsd2_manifest, "country_namespace_aware/GetCountryRequest")
    xsd2_response = commands.get_model(xsd2_context, xsd2_manifest, "country_namespace_aware/GetCountryResponse")

    assert _canonical_public_properties(wsdl_request) == _canonical_public_properties(xsd_request) == _canonical_public_properties(xsd2_request) == {
        "code": {"type": "string", "required": True, "source": "code"},
    }
    assert _canonical_public_properties(wsdl_response) == _canonical_public_properties(xsd_response) == _canonical_public_properties(xsd2_response) == {
        "name": {"type": "string", "required": True, "source": "name"},
        "population": {"type": "integer", "required": False, "source": "population"},
    }


def test_wsdl_default_namespace_schema_output_matches_xsd_pipelines(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(
        tmp_path,
        "country-default-namespace",
        COUNTRY_DEFAULT_NAMESPACE_TYPES_XSD,
        COUNTRY_WSDL_DEFAULT_NAMESPACE_SCHEMA_TYPES,
    )

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_request = commands.get_model(wsdl_context, wsdl_manifest, "services/country_service/GetCountryRequest")
    wsdl_response = commands.get_model(wsdl_context, wsdl_manifest, "services/country_service/GetCountryResponse")
    xsd_request = commands.get_model(xsd_context, xsd_manifest, "country_default_namespace/GetCountryRequest")
    xsd_response = commands.get_model(xsd_context, xsd_manifest, "country_default_namespace/GetCountryResponse")
    xsd2_request = commands.get_model(xsd2_context, xsd2_manifest, "country_default_namespace/GetCountryRequest")
    xsd2_response = commands.get_model(xsd2_context, xsd2_manifest, "country_default_namespace/GetCountryResponse")

    assert _canonical_public_properties(wsdl_request) == _canonical_public_properties(xsd_request) == _canonical_public_properties(xsd2_request) == {
        "code": {"type": "string", "required": True, "source": "code"},
    }
    assert _canonical_public_properties(wsdl_response) == _canonical_public_properties(xsd_response) == _canonical_public_properties(xsd2_response) == {
        "name": {"type": "string", "required": True, "source": "name"},
        "population": {"type": "integer", "required": False, "source": "population"},
    }


def test_wsdl_raw_partial_nested_model_features_match_xsd2(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(tmp_path, "nested", NESTED_TYPES_XSD, NESTED_TYPES_WSDL)

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/nested_service/schema/Location")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "nested/Location")

    assert wsdl_model.features == xsd2_model.features == "/:part"
    assert _canonical_public_properties(wsdl_model) == _canonical_public_properties(xsd2_model)
    assert (wsdl_model.external.name if wsdl_model.external else None) in {None, ""}
    assert (xsd2_model.external.name if xsd2_model.external else None) in {None, ""}


def test_wsdl_reference_and_required_flag_parity_matches_xsd_pipelines(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(
        tmp_path,
        "ref-required",
        REFERENCED_REQUIRED_TYPES_XSD,
        REFERENCED_REQUIRED_TYPES_WSDL,
    )

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/ref_service/GetRefResponse")
    xsd_model = commands.get_model(xsd_context, xsd_manifest, "ref_required/GetRefResponse")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "ref_required/GetRefResponse")

    wsdl_snapshot = _canonical_public_properties(wsdl_model)
    xsd_snapshot = _canonical_public_properties(xsd_model)
    xsd2_snapshot = _canonical_public_properties(xsd2_model)

    assert wsdl_snapshot == {
        "address_city": {"type": "string", "required": True, "source": "address/city"},
        "address_zip": {"type": "integer", "required": False, "source": "address/zip"},
        "status": {"type": "string", "required": True, "source": "status"},
    }
    assert wsdl_snapshot == xsd_snapshot == xsd2_snapshot
    assert xsd_model.properties["address"].dtype.name == "ref"
    assert xsd2_model.properties["address"].dtype.name == "ref"
    assert xsd2_model.properties["address"].dtype.model.name == "ref_required/AddressType"


def test_wsdl_raw_reference_schema_output_matches_xsd_pipelines(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(
        tmp_path,
        "ref-required",
        REFERENCED_REQUIRED_TYPES_XSD,
        REFERENCED_REQUIRED_TYPES_WSDL,
    )

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd_context, xsd_manifest = load_manifest_and_context(rc, xsd_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/ref_service/schema/GetRefResponse")
    wsdl_address_model = commands.get_model(wsdl_context, wsdl_manifest, "services/ref_service/schema/AddressType")
    xsd_model = commands.get_model(xsd_context, xsd_manifest, "ref_required/GetRefResponse")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "ref_required/GetRefResponse")
    xsd2_address_model = commands.get_model(xsd2_context, xsd2_manifest, "ref_required/AddressType")

    wsdl_snapshot = _canonical_public_properties(wsdl_model)
    xsd_snapshot = _canonical_public_properties(xsd_model)
    xsd2_snapshot = _canonical_public_properties(xsd2_model)
    wsdl_address_snapshot = _canonical_public_properties(wsdl_address_model)
    xsd2_address_snapshot = _canonical_public_properties(xsd2_address_model)

    assert wsdl_snapshot == {
        "address_city": {"type": "string", "required": True, "source": "address/city"},
        "address_zip": {"type": "integer", "required": False, "source": "address/zip"},
        "status": {"type": "string", "required": True, "source": "status"},
    }
    assert wsdl_snapshot == xsd_snapshot == xsd2_snapshot
    assert wsdl_address_snapshot == xsd2_address_snapshot == {
        "city": {"type": "string", "required": True, "source": "city"},
        "zip": {"type": "integer", "required": False, "source": "zip"},
    }
    assert wsdl_model.properties["address"].dtype.name == "ref"
    assert wsdl_model.properties["address"].dtype.model.name == "services/ref_service/schema/AddressType"
    assert xsd_model.properties["address"].dtype.name == "ref"
    assert xsd2_model.properties["address"].dtype.model.name == "ref_required/AddressType"


def test_wsdl_raw_partial_reference_model_features_match_xsd2(rc: RawConfig, tmp_path: Path):
    xsd_path, wsdl_path = _write_overlap_fixtures(
        tmp_path,
        "ref-required",
        REFERENCED_REQUIRED_TYPES_XSD,
        REFERENCED_REQUIRED_TYPES_WSDL,
    )

    wsdl_context, wsdl_manifest = load_manifest_and_context(rc, wsdl_path)
    xsd2_context, xsd2_manifest = load_manifest_and_context(rc, f"xsd2+file://{xsd_path}")

    wsdl_model = commands.get_model(wsdl_context, wsdl_manifest, "services/ref_service/schema/AddressType")
    xsd2_model = commands.get_model(xsd2_context, xsd2_manifest, "ref_required/AddressType")

    assert wsdl_model.features == xsd2_model.features == "/:part"
    assert _canonical_public_properties(wsdl_model) == _canonical_public_properties(xsd2_model)
    assert (wsdl_model.external.name if wsdl_model.external else None) in {None, ""}
    assert (xsd2_model.external.name if xsd2_model.external else None) in {None, ""}


def _write_overlap_fixtures(tmp_path: Path, stem: str, xsd: str, wsdl: str) -> tuple[Path, Path]:
    xsd_path = tmp_path / f"{stem}.xsd"
    wsdl_path = tmp_path / f"{stem}.wsdl"
    xsd_path.write_text(dedent(xsd))
    wsdl_path.write_text(dedent(wsdl))
    return xsd_path, wsdl_path


def _canonical_public_properties(
    model,
    *,
    name_prefix: str = "",
    source_prefix: str = "",
    parent_required: bool = True,
) -> dict[str, dict[str, object]]:
    properties: dict[str, dict[str, object]] = {}
    for prop_name, prop in model.properties.items():
        if prop_name.startswith("_"):
            continue

        dtype = prop.dtype
        if dtype.name in REF_DTYPES:
            nested_model = getattr(dtype, "model", None)
            if nested_model is None:
                continue

            nested_name_prefix = _join_name_prefix(name_prefix, prop_name)
            nested_source_prefix = _join_source_prefix(
                source_prefix,
                _normalize_source(prop.external.name if prop.external else prop_name),
            )
            properties.update(
                _canonical_public_properties(
                    nested_model,
                    name_prefix=nested_name_prefix,
                    source_prefix=nested_source_prefix,
                    parent_required=parent_required and bool(dtype.required),
                )
            )
            continue

        canonical_name = _join_name_prefix(name_prefix, prop_name)
        canonical_source = _normalize_source(prop.external.name if prop.external else prop_name)
        canonical_source = _join_source_prefix(source_prefix, canonical_source)
        properties[canonical_name] = {
            "type": _normalize_dtype(dtype.name),
            "required": parent_required and bool(dtype.required),
            "source": canonical_source,
        }

    return properties


def _normalize_dtype(dtype: str) -> str:
    if dtype == "uri":
        return "url"
    return dtype


def _normalize_source(source: str) -> str:
    if not source:
        return ""
    if source == "text()":
        return ""
    if source.endswith("/text()"):
        source = source[:-7]
    return source.replace(".", "/")


def _join_name_prefix(prefix: str, name: str) -> str:
    name = name.replace(".", "_")
    if not prefix:
        return name
    return f"{prefix}_{name}"


def _join_source_prefix(prefix: str, source: str) -> str:
    if not prefix:
        return source
    if not source:
        return prefix
    return f"{prefix}/{source}"