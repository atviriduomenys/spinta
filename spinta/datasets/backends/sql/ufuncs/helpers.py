from typing import Any, List


def ensure_list(value: Any) -> List[Any]:
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, list):
        return value
    else:
        return [value]
