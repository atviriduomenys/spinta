import re

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


def to_property_name(name: str) -> str:
    return to_code_name(name)


def to_code_name(name: str) -> str:
    name = unidecode(name)
    name = camel_cased_words.sub(r' \1', name).strip()
    words = split_words_re.split(name)
    words = [w.lower() for w in words if w]
    return _cleanup('_'.join(words))
