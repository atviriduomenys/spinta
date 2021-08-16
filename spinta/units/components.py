import dataclasses


@dataclasses.dataclass
class Unit:
    quantity: str
    symbol: str
    name: str
    value: float    # unit value in base units


units = [
    Unit('time', 's', 'second', 1),
    Unit('time', 'm', 'second', 60),
    Unit('time', 'h', 'second', 60 * 60),
    Unit('time', 'd', 'second', 60 * 60 * 24),
    Unit('time', 'w', 'second', 60 * 60 * 24 * 7),
    Unit('length', 'm', 'metre', 1),
    Unit('mass', 'kg', 'kilogram', 1),
    Unit('electric current', 'A', 'ampere', 1),
    Unit('thermodynamic temperature', 'K', 'kelvin', 1),
    Unit('amount of substance', 'mol', 'mole', 1),
    Unit('luminous intensity', 'cd', 'candela', 1),
]
