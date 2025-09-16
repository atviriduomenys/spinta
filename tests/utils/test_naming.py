import pytest

from spinta.utils.naming import Deduplicator, is_valid_model_name, is_valid_property_name
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name
from spinta.utils.naming import is_valid_namespace_name


@pytest.mark.parametrize(
    "source,target",
    [
        ("COUNTRY", "Country"),
        ("COUNTRY_CODE", "CountryCode"),
        ("country_code", "CountryCode"),
        ("country_CODE", "CountryCODE"),
        ("countryCode", "CountryCode"),
        ("Country code", "CountryCode"),
        ("CountryCode", "CountryCode"),
        ("EUCountryCodes", "EUCountryCodes"),
        ("ŠaliųŽemės", "SaliuZemes"),
        ("COUNTRY_CODE_2", "CountryCode2"),
        ("2COUNTRY_CODE", "N2CountryCode"),
    ],
)
def test_to_model_name(source: str, target: str):
    assert to_model_name(source) == target


@pytest.mark.parametrize(
    "source,target",
    [
        ("COUNTRY", "country"),
        ("COUNTRY_CODE", "country_code"),
        ("COUNTRY_CODE2", "country_code2"),
        ("2COUNTRY_CODE", "n2country_code"),
        ("country_code", "country_code"),
        ("country_CODE", "country_code"),
        ("countryCode", "country_code"),
        ("Country code", "country_code"),
        ("CountryCode", "country_code"),
        ("EUCountryCodes", "eu_country_codes"),
        ("ŠaliųŽemės", "saliu_zemes"),
        ("_name", "name"),
        ("__name", "name"),
    ],
)
def test_to_property_name(source: str, target: str):
    assert to_property_name(source) == target


def test_deduplicate():
    deduplicate = Deduplicator()
    assert deduplicate("name") == "name"
    assert deduplicate("name") == "name1"
    assert deduplicate("name") == "name2"
    assert deduplicate("name") == "name3"
    assert deduplicate("name1") == "name11"
    assert deduplicate("name1") == "name12"


def test_deduplicate_template():
    deduplicate = Deduplicator("_{}")
    assert deduplicate("name") == "name"
    assert deduplicate("name") == "name_1"
    assert deduplicate("name") == "name_2"
    assert deduplicate("name") == "name_3"
    assert deduplicate("name_1") == "name_1_1"
    assert deduplicate("name_1") == "name_1_2"


@pytest.mark.parametrize(
    "namespace,valid",
    [
        ("a", True),
        ("a1", True),
        ("a_1", True),
        ("a/a", True),
        ("a/a1", True),
        ("a/a_1", True),
        ("a/a/a", True),
        ("a/a/a1", True),
        ("a/a/a_1", True),
        ("ab", True),
        ("ab/cd", True),
        ("1", False),
        ("a/1", False),
        ("A", False),
        ("a/aA", False),
        ("_a", False),
        ("a/_a", False),
        ("-", False),
    ],
)
def test_is_valid_namespace_name(namespace: str, valid: bool):
    assert is_valid_namespace_name(namespace) == valid


@pytest.mark.parametrize(
    "model,valid",
    [
        ("M", True),
        ("Model", True),
        ("ModelName", True),
        ("ModelName1", True),
        ("1ModelName", False),
        ("modelName", False),
        ("_ModelName", False),
        ("ModelName_", False),
        ("Model_Name", False),
        ("Model/Name", False),
    ],
)
def test_is_valid_model_name(model: str, valid: bool):
    assert is_valid_model_name(model) == valid


@pytest.mark.parametrize(
    "prop,valid",
    [
        ("property", True),
        ("property1", True),
        ("property_", True),
        ("prop_prop", True),
        ("prop_prop1", True),
        ("prop_prop_", True),
        ("a_b_c", True),
        ("a_1", True),
        ("a", True),
        ("A", False),
        ("1", False),
        ("a/b", False),
        ("aA", False),
    ],
)
def test_is_valid_property_name(prop: str, valid: bool):
    assert is_valid_property_name(prop) == valid
