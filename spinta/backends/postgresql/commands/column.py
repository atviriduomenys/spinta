from typing import List

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name as gcn
from spinta.components import Property
from spinta.datasets.enums import Level
from spinta.exceptions import PropertyNotFound
from spinta.types.datatype import DataType, Ref, BackRef, String
import sqlalchemy as sa

from spinta.types.text.components import Text


def convert_str_to_columns(table: sa.Table, prop: Property, columns: List[str]) -> List[sa.Column]:
    return_list = []
    for column in columns:
        return_list.append(convert_str_to_column(table, prop, column))
    return return_list


def convert_str_to_column(table: sa.Table, prop: Property, column: str) -> sa.Column:
    if column in table.c:
        return table.c[column]
    else:
        raise PropertyNotFound(prop.dtype)


def get_table(backend: PostgreSQL, prop: Property) -> sa.Table:
    if prop.list:
        table = backend.get_table(prop.list, TableType.LIST)
    else:
        table = backend.get_table(prop)
    return table


@commands.get_column.register(PostgreSQL, Property)
def get_column(backend: PostgreSQL, prop: Property, **kwargs):
    return commands.get_column(backend, prop.dtype, **kwargs)


@commands.get_column.register(PostgreSQL, DataType)
def get_column(backend: PostgreSQL, dtype: DataType, table: sa.Table = None, **kwargs):
    prop = dtype.prop
    if table is None:
        table = get_table(backend, prop)
    column_name = gcn(dtype.prop)
    return convert_str_to_column(table, prop, column_name)


@commands.get_column.register(PostgreSQL, String)
def get_column(backend: PostgreSQL, dtype: String, table: sa.Table = None, **kwargs):
    prop = dtype.prop
    parent = prop.parent

    if table is None:
        table = get_table(backend, prop)
    column_name = gcn(parent if parent and isinstance(parent.dtype, Text) else dtype.prop)
    column = convert_str_to_column(table, prop, column_name)

    if parent and isinstance(parent.dtype, Text):
        column = column[dtype.prop.name].label(dtype.prop.place.replace('@', '.'))
    return column


@commands.get_column.register(PostgreSQL, Text)
def get_column(backend: PostgreSQL, dtype: Text, table: sa.Table = None, langs: list = [], default_langs: list = [], push: bool = False, **kwargs):
    prop = dtype.prop
    column_name = gcn(dtype.prop)
    if table is None:
        table = get_table(backend, prop)

    if push:
        column = convert_str_to_column(table, prop, column_name)
        return column

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
    column = convert_str_to_column(table, prop, column_name)
    column = column[lang_prop.name].label(prop.place)
    return column


@commands.get_column.register(PostgreSQL, Ref)
def get_column(backend: PostgreSQL, dtype: Ref, table: sa.Table = None, **kwargs):
    column = gcn(dtype.prop)
    prop = dtype.prop
    columns = []
    if table is None:
        table = get_table(backend, prop)
    if prop.level is None or prop.level > Level.open:
        if prop.list is not None:
            column = '_id'
        else:
            column += '._id'
        return convert_str_to_column(table, prop, column)
    else:
        for refprop in prop.dtype.refprops:
            if prop.list is not None:
                columns.append(refprop.name)
            else:
                columns.append(f'{column}.{refprop.name}')
        return convert_str_to_columns(table, prop, columns)


@commands.get_column.register(PostgreSQL, BackRef)
def get_column(backend: PostgreSQL, dtype: BackRef, table: sa.Table = None, **kwargs):
    prop = dtype.prop
    r_prop = dtype.refprop
    columns = []
    if table is None:
        table = get_table(backend, dtype.prop)
    if r_prop.level is None or r_prop.level > Level.open:
        column = '_id'
        return convert_str_to_column(table, prop, column)
    else:
        for refprop in r_prop.dtype.refprops:
            columns.append(refprop.name)
        return convert_str_to_columns(table, prop, columns)
