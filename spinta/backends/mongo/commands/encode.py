import datetime

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Date
from spinta.backends.mongo.components import Mongo


@commands.prepare_for_write.register(Context, Date, Mongo, datetime.date)
def prepare_for_write(
    context: Context,
    dtype: Date,
    backend: Mongo,
    value: datetime.date,
) -> datetime.datetime:
    # prepares date values for Mongo store, they must be converted to datetime
    return datetime.datetime.combine(value, datetime.datetime.min.time())


@commands.cast_backend_to_python.register(Context, Date, Mongo, datetime.datetime)
def cast_backend_to_python(
    context: Context,
    dtype: Date,
    backend: Mongo,
    value: datetime.datetime,
) -> datetime.date:
    return value.date()
