import pytest

from spinta.units.helpers import is_si_unit
from spinta.units.helpers import is_time_unit


@pytest.mark.parametrize(
    "unit",
    [
        "1D",
        "D",
        "Y",
        "3M",
        "12H",
    ],
)
def test_valid_time_unit(unit: str):
    assert is_time_unit(unit)


@pytest.mark.parametrize(
    "unit",
    [
        "D1",
        "d",
        "YY",
        "",
        " D",
        "D ",
    ],
)
def test_invalid_time_unit(unit: str):
    assert not is_time_unit(unit)


@pytest.mark.parametrize(
    "unit",
    [
        "m",
        "1m",
        "10m",
        "m^2",
        "m²",
        "km¹⁰",
        "kg⋅m²⋅s⁻³⋅A⁻¹",
        "kg*m^2*s^-3⋅A^-1",
        "8kg⋅m²⋅s⁻³⋅A⁻¹",
        "mg/l",
        "g/m^2",
        "mg/m^3",
        "mm",
        "U/m^2",
        "U/m^3",
        "%",
        "ha",
        "min",
        "h",
        "bar",
        "U",
        "10^6s",
        "10⁶s",
        "μ/m³",
        "yr",
        "3mo",
        "yr 2mo 4wk",
        "°C",
        "°",
    ],
)
def test_valid_unit(unit: str):
    assert is_si_unit(unit)


@pytest.mark.parametrize(
    "unit",
    [
        "D",
        "1D",
        "meter",
    ],
)
def test_invalid_si_unit(unit: str):
    assert not is_si_unit(unit)
