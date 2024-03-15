from spinta.types.text.components import Text


def determine_language_for_text(dtype: Text, langs: list, default_langs: list):
    existing_langs = list(dtype.langs.keys())
    lang_prop = None
    if langs:
        for lang in langs:
            if lang in dtype.langs:
                lang_prop = dtype.langs[lang]
                break
    if not lang_prop and default_langs:
        for lang in default_langs:
            if lang in dtype.langs:
                lang_prop = dtype.langs[lang]
                break
    if not lang_prop:
        lang_prop = dtype.langs[existing_langs[0]]
    return lang_prop
