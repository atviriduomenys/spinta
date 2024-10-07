from spinta import commands
from spinta.components import Property
from spinta.datasets.backends.sql.components import Sql
from spinta.exceptions import NoExternalName, PropertyNotFound
from spinta.types.datatype import DataType, Ref
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


@commands.get_column.register(Sql, Ref)
def get_column(backend: Sql, dtype: Ref, table: sa.Table, **kwargs):
    prop = dtype.prop
    if prop.external is None or not prop.external.name and not dtype.inherited:
        raise NoExternalName(prop)

    if prop.external.name:
        if prop.external.name not in table.c:
            raise PropertyNotFound(
                prop.model,
                property=prop.name,
                external=prop.external.name,
            )
        return table.c[prop.external.name]
