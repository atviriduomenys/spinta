import re
from typing import Set

from unidecode import unidecode

camel_cased_words = re.compile(r'([A-Z][a-z]+)')
split_words_re = re.compile(r'[^a-zA-Z0-9]+')
number_prefix_re = re.compile(r'^([0-9]+)')


def _cleanup(name: str) -> str:
    name = number_prefix_re.sub(r'N\1', name)
    return name


def to_model_name(name: str) -> str:
    name = unidecode(name)
    name = camel_cased_words.sub(r' \1', name).strip()
    words = split_words_re.split(name)
    if name.isupper() or name.islower():
        words = [w.title() for w in words if w]
    else:
        words = [w.title() if w.islower() else w for w in words if w]
    return _cleanup(''.join(words))


def to_property_name(name: str, is_ref: bool = False) -> str:
    name = to_code_name(name)
    if is_ref and name.endswith('_id'):
        name = name[:-3]
    return name


def to_code_name(name: str) -> str:
    name = unidecode(name)
    name = camel_cased_words.sub(r' \1', name).strip()
    words = split_words_re.split(name)
    words = filter(None, words)
    return _cleanup('_'.join(words)).lower()


class Deduplicator:
    _names: Set[str]

    def __init__(self, template: str = '{}'):
        self._names = set()
        self._template = template

    def __call__(self, name: str):
        name_ = name
        i = 0
        while name_ in self._names:
            i += 1
            name_ = name + self._template.format(i)
        self._names.add(name_)
        return name_
