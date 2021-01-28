import pytest

from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


@pytest.mark.parametrize('source,target', [
    ('COUNTRY', 'Country'),
    ('COUNTRY_CODE', 'CountryCode'),
    ('country_code', 'CountryCode'),
    ('country_CODE', 'CountryCODE'),
    ('countryCode', 'CountryCode'),
    ('Country code', 'CountryCode'),
    ('CountryCode', 'CountryCode'),
    ('EUCountryCodes', 'EUCountryCodes'),
    ('ŠaliųŽemės', 'SaliuZemes'),
    ('COUNTRY_CODE_2', 'CountryCode2'),
    ('2COUNTRY_CODE', 'N2CountryCode'),
])
def test_to_model_name(source: str, target: str):
    assert to_model_name(source) == target


@pytest.mark.parametrize('source,target', [
    ('COUNTRY', 'country'),
    ('COUNTRY_CODE', 'country_code'),
    ('COUNTRY_CODE2', 'country_code2'),
    ('2COUNTRY_CODE', 'n2country_code'),
    ('country_code', 'country_code'),
    ('country_CODE', 'country_code'),
    ('countryCode', 'country_code'),
    ('Country code', 'country_code'),
    ('CountryCode', 'country_code'),
    ('EUCountryCodes', 'eu_country_codes'),
    ('ŠaliųŽemės', 'saliu_zemes'),
])
def test_to_property_name(source: str, target: str):
    assert to_property_name(source) == target
