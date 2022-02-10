import re

from spinta.types.datatype import DataType
from spinta.types.datatype import Date
from spinta.types.datatype import DateTime
from spinta.types.geometry.components import Geometry
from spinta.types.datatype import Integer
from spinta.types.datatype import Number

_time_unit_re = re.compile(r'^\d*[YMQWDHTSLUN]$')


def is_time_unit(unit: str) -> bool:
    return _time_unit_re.match(unit) is not None


# Borrowed from https://stackoverflow.com/a/3573731/475477
_prefix = '(Y|Z|E|P|T|G|M|k|h|da|d|c|m|µ|n|p|f|a|z|y)'
_unit = '(m|g|s|A|K|mol|cd|Hz|N|Pa|J|W|C|V|F|Ω|S|Wb|T|H|lm|lx|Bq|Gy|Sv|kat|l|L|B)'
_power = r'([⁺⁻]?[¹²³⁴⁵⁶⁷⁸⁹][⁰¹²³⁴⁵⁶⁷⁸⁹]*|\^[+-]?[1-9]\d*)'
_unit_and_prefix = '(' + _prefix + '?' + _unit + _power + '?|1)'
_multiplied = _unit_and_prefix + '(?:[⋅·*]' + _unit_and_prefix + ')*'
_with_denominator = _multiplied + '(?:/' + _multiplied + ')?'
_si_unit_re = re.compile(r'^\d*' + _with_denominator + '$')


def is_si_unit(unit: str) -> bool:
    return _si_unit_re.match(unit) is not None


def is_unit(dtype: DataType, unit: str) -> bool:
    if isinstance(dtype, (Date, DateTime)):
        return is_time_unit(unit)
    if isinstance(dtype, (Integer, Number, Geometry)):
        return is_si_unit(unit)
    return False
