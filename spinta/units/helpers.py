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


# Initial regular expressions were borrowed from
# https://stackoverflow.com/a/3573731/475477

_prefix = '''
( Y     # 10^24  (yotta)
| Z     # 10^21  (zetta)
| E     # 10^18  (exa)
| P     # 10^15  (peta)
| T     # 10^12  (tera)
| G     # 10^9   (giga)
| M     # 10^6   (mega)
| k     # 10^3   (kilo)
| h     # 10^2   (hecto)
| da    # 10^1   (deca)
| d     # 10^-1  (deci)
| c     # 10^-2  (centi)
| m     # 10^-3  (milli)
| µ     # 10^-6  (micro)
| n     # 10^-9  (nano)
| p     # 10^-12 (pico)
| f     # 10^-15 (femto)
| a     # 10^-18 (atto)
| z     # 10^-21 (zepto)
| y     # 10^-24 (yocto)
)'''

_unit = '''
# SI Base Units
( m     # metre
| g     # gram
| s     # second
| A     # ampere
| K     # kelvin
| mol   # mole
| cd    # candela

# Named units derived from SI Base Units
| Hz    # hertz
| rad   # radian
| sr    # steradian
| N     # newton
| Pa    # pascal
| J     # joule
| W     # watt
| C     # coulomb
| V     # volt
| F     # farad
| Ω     # ohm
| S     # siemens
| Wb    # weber
| T     # tesla
| H     # henry
| °C    # degree Celsius
| lm    # lumen
| lx    # lux
| Bq    # becquerel
| Gy    # gray
| Sv    # sievert
| kat   # katal

# Non-SI units officially accepted for use with the SI
| min   # minute
| h     # hour
| d     # day
| au    # astronomical unit
| °     # degree
| ′     # arcminute
| ″     # arcsecond
| ha    # hectare
| l     # litre
| L     # litre
| t     # tonne
| Da    # dalton
| eV    # electronvolt
| Np    # neper
| B     # bel
| dB    # decibel

# Non-SI units not officially sanctioned for use with the SI
| Gal   # gal (acceleration)
| u     # unified atomic mass unit
| var   # volt-ampere reactive

# Non-SI time units
| yr    # year
| wk    # week
| mo    # month

# Other Non-SI units
| pc        # parsec
| c₀ | c_0  # natural unit of speed
| ħ         # natural unit of action
| mₑ | m_e  # natural unit of mass
| e         # atomic unit of charge
| a₀ | a_0  # atomic unit of length
| E_h       # atomic unit of energy
| M         # nautical mile
| kn        # knot
| Å         # ångström
| a         # are
| b         # barn
| bar       # bar
| atm       # standard atmosphere
| Ci        # curie
| R         # roentgen
| rem       # rem
| erg       # erg
| dyn       # dyne
| P         # poise
| st        # stokes
| Mx        # maxwell
| G         # gauss
| Oe        # ørsted
| sb        # stilb
| ph        # phot
| Torr      # torr
| kgf       # kilogram-force
| cal       # calorie
| μ         # micron
| xu        # x-unit
| γ         # gamma (mass, magnetic flux density)
| λ         # lambda
| Jy        # jansky
| mmHg      # millimetre of mercury

# Special units
| U     # unit
| %     # percent
)'''
_power = r'([⁺⁻]?[¹²³⁴⁵⁶⁷⁸⁹][⁰¹²³⁴⁵⁶⁷⁸⁹]*|\^[+-]?[1-9]\d*)'
_unit_and_prefix = '((' + _prefix + '?' + _unit + ')' + _power + '?|1)'
_multiplied = _unit_and_prefix + '(?:[⋅·*]' + _unit_and_prefix + ')*'
_with_denominator = _multiplied + '(?:/' + _multiplied + ')?'
_expr = r'\d*' + _power + '?' + _with_denominator
_si_unit_re = re.compile('^' + _expr + r'(?:\ ' + _expr + ')*$', flags=re.VERBOSE)


def is_si_unit(unit: str) -> bool:
    return _si_unit_re.match(unit) is not None


def is_unit(dtype: DataType, unit: str) -> bool:
    if isinstance(dtype, (Date, DateTime)):
        return is_time_unit(unit)
    if isinstance(dtype, (Integer, Number, Geometry)):
        return is_si_unit(unit)
    return False
