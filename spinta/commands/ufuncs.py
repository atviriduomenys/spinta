from spinta.components import Context
from spinta.types.datatype import DataType
from spinta.ufuncs import ufunc


@ufunc(Context, DataType)
def check(context, dtype, this, value, message, **kwargs):
    if bool(value) is False:
        raise Exception(message.format(**kwargs))
    return this[-1]


@ufunc(Context, DataType)
def len(context, dtype, this, value):
    return len(value)
