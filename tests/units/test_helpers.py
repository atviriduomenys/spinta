import pytest

from spinta.units.helpers import is_si_unit
from spinta.units.helpers import is_time_unit


@pytest.mark.parametrize('unit', [
    '1D',
    'D',
    'Y',
    '3M',
    '12H',
])
def test_valid_time_unit(unit: str):
    assert is_time_unit(unit)


@pytest.mark.parametrize('unit', [
    'D1',
    'd',
    'YY',
    '',
    ' D',
    'D ',
])
def test_invalid_time_unit(unit: str):
    assert not is_time_unit(unit)


@pytest.mark.parametrize('unit', [
    'm',
    '1m',
    '10m',
    'm^2',
    'm²',
    'km¹⁰',
    'kg⋅m²⋅s⁻³⋅A⁻¹',
    'kg*m^2*s^-3⋅A^-1',
    '8kg⋅m²⋅s⁻³⋅A⁻¹',
])
def test_valid_si_unit(unit: str):
    assert is_si_unit(unit)


@pytest.mark.parametrize('unit', [
    'D',
    '1D',
    'meter',
])
def test_invalid_si_unit(unit: str):
    assert not is_si_unit(unit)
