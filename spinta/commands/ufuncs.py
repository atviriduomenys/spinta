from spinta.components import Context, Model
from spinta.types.datatype import DataType
from spinta.types import dataset
from spinta.ufuncs import ufunc
from spinta import exceptions


@ufunc(Context, DataType, tuple)
def bind(context, dtype, this):
    return this[-1]


@ufunc(Context, (Model, dataset.Model, DataType), tuple, str)
def bind(context, dtype, this, name):  # noqa
    return this[-1][name]


@ufunc(Context, (dataset.Model, DataType), tuple, object)
@ufunc(Context, (dataset.Model, DataType), tuple, object, str)
def check(context, dtype, this, value, message=None, **kwargs):
    if bool(value) is False:
        message = message or "Data check error."
        raise exceptions.InvalidValue(dtype, error=message.format(**kwargs))
    return this[-1]


@ufunc(Context, DataType, tuple, object)
def len_(context, dtype, this, value):
    return len(value)


@ufunc(Context, (dataset.Model, DataType), tuple, object, object)
def eq(context, dtype, this, left, right):
    return left == right
