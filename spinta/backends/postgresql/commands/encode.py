from typing import List

import datetime
from typing import Union

from sqlalchemy.engine.result import RowProxy

from spinta import commands
from spinta.components import Context, Model, Action
from spinta.types.datatype import Date, DateTime
from spinta.backends.postgresql.components import PostgreSQL


@commands.prepare_data_for_response.register(Context, Action, Model, PostgreSQL, RowProxy)
def prepare_data_for_response(
    context: Context,
    action: Action,
    model: Model,
    backend: PostgreSQL,
    value: RowProxy,
    *,
    select: List[str] = None,
) -> dict:
    return commands.prepare_data_for_response(
        context,
        action,
        model,
        backend,
        dict(value),
        select=select,
    )


@commands.prepare_for_write.register(Context, DateTime, PostgreSQL, datetime.datetime)
def prepare_for_write(
    context: Context,
    dtype: DateTime,
    backend: PostgreSQL,
    value: datetime.datetime,
) -> Union[datetime.datetime, str]:
    # convert datetime object to isoformat string if it belongs
    # to a nested property
    if dtype.prop.parent is dtype.prop.model:
        return value
    else:
        # XXX: Probably this should not be converted to string, because
        #      if I recall correctly, now nested objects are stored in separate
        #      tables.
        return value.isoformat()


@commands.prepare_for_write.register(Context, Date, PostgreSQL, datetime.date)
def prepare_for_write(
    context: Context,
    dtype: Date,
    backend: PostgreSQL,
    value: datetime.date,
) -> Union[datetime.date, str]:
    # convert date object to isoformat string if it belongs
    # to a nested property
    if dtype.prop.parent is dtype.prop.model:
        return value
    else:
        return value.isoformat()
