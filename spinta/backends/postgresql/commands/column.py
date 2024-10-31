from typing import List

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name as gcn
from spinta.components import Property, Model
from spinta.exceptions import PropertyNotFound
from spinta.types.datatype import ArrayBackRef, DataType, Ref, BackRef, String, ExternalRef, Array
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

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
    column = convert_str_to_column(table, prop, column_name)
    if isinstance(column.type, (sa.types.JSON, JSONB)) and dtype.prop.place != dtype.prop.name:
        return column[dtype.prop.name].label(dtype.prop.place)
    return column


@commands.get_column.register(PostgreSQL, Array)
def get_column(backend: PostgreSQL, dtype: Array, table: sa.Table = None, **kwargs):
    prop = dtype.prop
    if table is None:
        table = get_table(backend, prop)
    column_name = gcn(dtype.prop)
    column = convert_str_to_column(table, prop, column_name)
    return column


@commands.get_column.register(PostgreSQL, ArrayBackRef)
def get_column(backend: PostgreSQL, dtype: ArrayBackRef, table: sa.Table = None, **kwargs):
    return commands.get_column(backend, dtype.items.dtype, **kwargs)


@commands.get_column.register(PostgreSQL, String)
def get_column(backend: PostgreSQL, dtype: String, table: sa.Table = None, **kwargs):
    prop = dtype.prop
    parent = prop.parent

    if table is None:
        table = get_table(backend, prop)
    column_name = gcn(parent if parent and isinstance(parent.dtype, Text) else dtype.prop)
    column = convert_str_to_column(table, prop, column_name)

    if parent and isinstance(parent.dtype, Text):
        return column[dtype.prop.name].label(dtype.prop.place.replace('@', '.'))

    if isinstance(column.type, (sa.types.JSON, JSONB)) and dtype.prop.place != dtype.prop.name:
        return column[dtype.prop.name].label(dtype.prop.place)
    return column


@commands.get_column.register(PostgreSQL, Ref)
def get_column(backend: PostgreSQL, dtype: Ref, table: sa.Table = None, **kwargs):
    column = gcn(dtype.prop, replace=dtype.inherited)
    prop = dtype.prop
    if table is None:
        table = get_table(backend, prop)
    if prop.list is not None:
        column = '_id'
    else:
        column += '._id'
    return convert_str_to_column(table, prop, column)


@commands.get_column.register(PostgreSQL, ExternalRef)
def get_column(backend: PostgreSQL, dtype: Ref, table: sa.Table = None, **kwargs):
    column_prefix = gcn(dtype.prop, replace=dtype.inherited)
    prop = dtype.prop
    if table is None:
        table = get_table(backend, prop)

    result = []
    for ref_prop in dtype.refprops:
        ref_prop_ = ref_prop
        table_ = table
        column = ref_prop.name
        if prop.list is None:
            column = f'{column_prefix}.{ref_prop.name}'
        if ref_prop.name in dtype.properties:
            if isinstance(ref_prop.dtype, type(dtype.properties[ref_prop.name].dtype)):
                ref_prop_ = dtype.properties[ref_prop.name]
                table_ = get_table(backend, ref_prop_)
                column = ref_prop_.place
        result.append(convert_str_to_column(table_, ref_prop_, column))
    return result


@commands.get_column.register(PostgreSQL, BackRef)
def get_column(backend: PostgreSQL, dtype: BackRef, table: sa.Table = None, **kwargs):
    prop = dtype.prop

    if prop.list is not None:
        return None

    r_prop = dtype.refprop
    columns = []
    if table is None:
        table = get_table(backend, dtype.prop)
    if commands.identifiable(r_prop):
        column = '_id'
        return convert_str_to_column(table, prop, column)
    else:
        for refprop in r_prop.dtype.refprops:
            columns.append(refprop.name)
        return convert_str_to_columns(table, prop, columns)


@commands.get_column.register(PostgreSQL, Property, Model)
def get_column(backend: PostgreSQL, prop: Property, model: Model, table: sa.Table = None, **kwargs):
    return commands.get_column(backend, prop.dtype, model, table=table, **kwargs)


@commands.get_column.register(PostgreSQL, DataType, Model)
def get_column(backend: PostgreSQL, prop: Property, model: Model, table: sa.Table = None, **kwargs):
    return None


@commands.get_column.register(PostgreSQL, Ref, Model)
def get_column(backend: PostgreSQL, dtype: Ref, model: Model, table: sa.Table = None, **kwargs):
    prop = dtype.prop
    if table is None:
        table = backend.get_table(model)
    column = '_id'
    return convert_str_to_column(table, prop, column)


@commands.get_column.register(PostgreSQL, ExternalRef, Model)
def get_column(backend: PostgreSQL, dtype: ExternalRef, model: Model, table: sa.Table = None, **kwargs):
    prop = dtype.prop
    if table is None:
        table = backend.get_table(model)

    columns = []
    for refprop in prop.dtype.refprops:
        columns.append(refprop.name)
    return convert_str_to_columns(table, prop, columns)



