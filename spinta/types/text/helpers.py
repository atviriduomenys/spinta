from typing import List

from spinta.types.text.components import Text


def determine_language_property_for_text(dtype: Text, prioritized_langs: list, default_langs: list):
    existing_langs = list(dtype.langs.keys())
    selected_lang = determine_langauge_for_text(existing_langs, prioritized_langs, default_langs)
    return dtype.langs[selected_lang]


def determine_langauge_for_text(existing_langs: List[str], prioritized_langs: List[str], default_langs: List[str]):
    selected_lang = None
    if prioritized_langs:
        for lang in prioritized_langs:
            if lang in existing_langs:
                selected_lang = lang
                break
    if not selected_lang and default_langs:
        for lang in default_langs:
            if lang in existing_langs:
                selected_lang = lang
                break
    if not selected_lang:
        selected_lang = existing_langs[0]
    return selected_lang
