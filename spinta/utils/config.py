from typing import Union


def asbool(s: Union[str, bool]) -> bool:
    if isinstance(s, bool):
        return s
    if s in ('true', '1', 'on', 'yes'):
        return True
    if s in ('false', '0', 'off', 'no', ''):
        return False
    raise ValueError(f"Expected a boolean value, got {s!r}.")
