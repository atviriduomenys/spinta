from typing import Any, Generator


def prepare_keymap_values(value: Any) -> Any | list[Any]:
    result = list(_extract_items_from_source(value))
    if len(result) == 1:
        return result[0]
    return result


def _extract_items_from_source(source: Any) -> Generator[Any, None, None]:
    if isinstance(source, dict):
        for item in source.values():
            yield from _extract_items_from_source(item)
    elif isinstance(source, (list, tuple, set)):
        for item in source:
            yield from _extract_items_from_source(item)
    else:
        yield source
