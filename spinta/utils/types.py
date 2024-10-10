import numbers
from uuid import UUID

_LITERAL_TYPES = (
    numbers.Number, type(None), str
)


def is_str_uuid(value: str) -> bool:
    try:
        uuid_obj = UUID(value, version=4)
    except Exception:
        return False
    return str(uuid_obj) == value


def is_value_literal(value: object) -> bool:
    return isinstance(value, _LITERAL_TYPES)
