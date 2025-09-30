from spinta.components import Context
from spinta.backends.postgresql.components import PostgreSQL
from spinta.exceptions import UnsupportedDataTypeConfiguration
from spinta.types.datatype import DataType


def validate_type_assignment(context: Context, backend: PostgreSQL, dtype: DataType):
    if dtype.prop.external and dtype.prop.external.custom_type:
        raise UnsupportedDataTypeConfiguration(dtype, data_type=dtype.name)


def get_column_type(dtype: DataType, default: object):
    return dtype.prop.external and dtype.prop.external.custom_type or default
