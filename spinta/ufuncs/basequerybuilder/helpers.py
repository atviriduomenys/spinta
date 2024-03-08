from spinta.components import Property
from spinta.types.text.components import Text
from spinta.types.text.helpers import determine_language_for_text
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder


def get_language_column(env: BaseQueryBuilder, dtype: Text):
    default_langs = env.context.get('config').languages
    prop = determine_language_for_text(dtype, env.query_params.lang_priority, default_langs)
    column = env.backend.get_column(env.table, prop)
    return column


def get_column_with_extra(env: BaseQueryBuilder, prop: Property):
    if isinstance(prop.dtype, Text):
        return get_language_column(env, prop.dtype)
    return env.backend.get_column(env.table, prop)
