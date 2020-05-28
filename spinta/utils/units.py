BYTE_MULTIPLES = {
    'b': 1,
    'k': 1000,
    'm': 1000**2,
    'g': 1000**3,
    't': 1000**4,
    'p': 1000**5,
    'e': 1000**6,
    'z': 1000**7,
    'y': 1000**8,
}


def tobytes(s, default: str = 'b'):
    unit = s[-1].lower()
    if unit in BYTE_MULTIPLES:
        s = s[:-1]
    else:
        unit = default
    value = int(s)
    return value * BYTE_MULTIPLES[unit]


TIME_UNITS = {
    's': 1,
    'm': 60,
    'h': 60 * 60,
    'd': 60 * 60 * 24,
    'w': 60 * 60 * 24 * 7,
    'n': 60 * 60 * 24 * 30,
    'y': 60 * 60 * 24 * 365,
}


def toseconds(s, default: str = 's'):
    unit = s[-1].lower()
    if unit in TIME_UNITS:
        s = s[:-1]
    else:
        unit = default
    value = int(s)
    return value * TIME_UNITS[unit]
