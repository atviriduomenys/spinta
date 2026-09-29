import numbers
from uuid import UUID

_LITERAL_TYPES = (numbers.Number, type(None), str)


def is_str_uuid(value: str) -> bool:
    try:
        uuid_obj = UUID(value, version=4)
    except Exception:
        return False
    return str(uuid_obj) == value


def is_value_literal(value: object) -> bool:
    return isinstance(value, _LITERAL_TYPES)


def is_nan(value: object) -> bool:
    """
    Check for NaN values using IEEE 754 standard.

    IEEE 754 defines that comparing with NaN always returns false,
    including comparison with itself.

    Args:
        value: Value to check for NaN

    Returns:
        True if value is NaN, False otherwise
    """
    try:
        return value != value  # NaN is the only value that is not equal to itself
    except Exception:
        return False
