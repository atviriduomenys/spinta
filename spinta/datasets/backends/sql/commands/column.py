from spinta import commands
from spinta.components import Property
from spinta.datasets.backends.sql.components import Sql
from spinta.exceptions import NoExternalName, PropertyNotFound
from spinta.types.datatype import DataType
from spinta.types.text.components import Text
import sqlalchemy as sa


@commands.get_column.register(Sql, Property)
def get_column(backend: Sql, prop: Property, **kwargs):
    return commands.get_column(backend, prop.dtype, **kwargs)


@commands.get_column.register(Sql, DataType)
def get_column(backend: Sql, dtype: DataType, table: sa.Table, **kwargs):
    prop = dtype.prop
    if prop.external is None or not prop.external.name:
        raise NoExternalName(prop)
    if prop.external.name not in table.c:
        raise PropertyNotFound(
            prop.model,
            property=prop.name,
            external=prop.external.name,
        )
    return table.c[prop.external.name]


@commands.get_column.register(Sql, Text)
def get_column(backend: Sql, dtype: Text, table: sa.Table, langs: list = [], default_langs: list = [], push: bool = False, **kwargs):
    prop = dtype.prop

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

    column_name = lang_prop.external.name
    if not column_name:
        raise NoExternalName(lang_prop)
    if column_name not in table.c:
        raise PropertyNotFound(
            prop.model,
            property=lang_prop.name,
            external=lang_prop.external.name,
        )
    column = table.c[lang_prop.external.name]
    return column
