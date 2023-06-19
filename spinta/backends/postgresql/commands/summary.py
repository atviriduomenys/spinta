import calendar
import datetime
import re

from spinta.backends.helpers import get_table_name
from spinta.types.datatype import Integer, Number, Boolean, String, Date, DateTime, Time, Ref
from spinta import commands
from spinta.components import Context, Property
from spinta.components import Model
from spinta.exceptions import NotFoundError, PropertyNotFound, NotImplementedFeature
from spinta.exceptions import ItemDoesNotExist
from spinta.backends.postgresql.components import PostgreSQL
from spinta.utils.nestedstruct import flat_dicts_to_nested

from dateutil.relativedelta import relativedelta


@commands.summary.register(Context, Model, PostgreSQL)
def summary(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    prop: str = "",
):
    if prop in model.properties.keys():
        summary_property = model.properties[prop]
    else:
        raise PropertyNotFound(model, property=prop)
    result = commands.summary(context, summary_property.dtype, backend)

    return result


@commands.summary.register(Context, Integer, PostgreSQL)
def summary(
    context: Context,
    dtype: Integer,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection
    yield from _handle_numeric_summary(connection, dtype.prop)


@commands.summary.register(Context, Number, PostgreSQL)
def summary(
    context: Context,
    dtype: Number,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection
    yield from _handle_numeric_summary(connection, dtype.prop)


def _handle_numeric_summary(connection, model_prop: Property):
    try:
        prop = model_prop.name
        model = get_table_name(model_prop)
        min_max = connection.execute(f'SELECT MIN("{prop}") , MAX("{prop}") FROM "{model}"')
        min_value = 0
        max_value = 0
        for item in min_max:
            min_value = item[0]
            max_value = item[1]
        if min_value is None and max_value is None:
            return []
        else:
            if min_value == max_value:
                max_value += 100
            bin_size = (max_value - min_value) / 100
            half_bin_size = bin_size / 2

            # reason for 'max_value + min(0.01, bin_size*0.01)', is that WIDTH_BUCKET max is not inclusive, so you need to increase it by small amount
            result = connection.execute(f'''
                SELECT 
                    WIDTH_BUCKET("{prop}", {min_value}, {max_value + min(0.01, bin_size * 0.01)}, 100) AS bucket, 
                    COUNT(*) AS count,
                    (ARRAY_AGG(_id))[1] AS _id
                FROM "{model}" 
                GROUP BY 
                    bucket 
                ORDER BY 
                    bucket;
            ''')
            buckets = []
            for i in range(100):
                buckets.append(
                    {
                        "bin": i * bin_size + half_bin_size + min_value,
                        "count": 0,
                        "_type": model_prop.model.model_type()
                    }
                )
            for item in result:
                data = flat_dicts_to_nested(dict(item))
                if data["bucket"]:
                    bucket = buckets[data["bucket"] - 1]
                    bucket["count"] = data["count"]
                    if data['count'] == 1:
                        bucket["_id"] = data["_id"]
            yield from buckets
    except NotFoundError:
        raise ItemDoesNotExist(model_prop.model, id=model_prop.name)


@commands.summary.register(Context, Boolean, PostgreSQL)
def summary(
    context: Context,
    dtype: Boolean,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection
    try:
        prop = dtype.prop.name
        model = get_table_name(dtype.prop)

        result = connection.execute(f'''
                SELECT 
                    bin, 
                    COUNT(model.*) AS count, 
                    (ARRAY_AGG(model._id))[1] AS _id
                FROM UNNEST(ARRAY[TRUE, FALSE]) AS bin
                    LEFT OUTER JOIN 
                        "{model}" AS model 
                    ON 
                        bin = "{prop}"
                    GROUP BY 
                        bin
                    ORDER BY
                        bin;
        ''')
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            if data["count"] != 1:
                del data["_id"]
            data['_type'] = dtype.prop.model.model_type()
            yield data
    except NotFoundError:
        raise ItemDoesNotExist(dtype.prop.model, id=dtype.prop.name)


@commands.summary.register(Context, String, PostgreSQL)
def summary(
    context: Context,
    dtype: String,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection

    if not dtype.prop.enum:
        raise NotImplementedFeature(dtype.prop,
                                    feature="Ability to generate summary for String type properties that do not have Enum")
    enum_list = []
    for key, value in dtype.prop.enum.items():
        enum_list.append(value.prepare)
    try:
        prop = dtype.prop.name
        model = get_table_name(dtype.prop)

        result = connection.execute(f'''
                SELECT 
                    bin, 
                    COUNT(model.*) AS count, 
                    (ARRAY_AGG(model._id))[1] AS _id
                FROM UNNEST(ARRAY{enum_list}) AS bin
                    LEFT OUTER JOIN 
                        "{model}" AS model 
                    ON 
                        bin = "{prop}"
                    GROUP BY 
                        bin
                    ORDER BY
                        bin;
                ''')
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            if data["count"] != 1:
                del data["_id"]
            data['_type'] = dtype.prop.model.model_type()
            yield data
    except NotFoundError:
        raise ItemDoesNotExist(dtype.prop.model, id=dtype.prop.name)


@commands.summary.register(Context, Date, PostgreSQL)
def summary(
    context: Context,
    dtype: Date,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection
    yield from _handle_time_summary(connection, dtype.prop)


@commands.summary.register(Context, DateTime, PostgreSQL)
def summary(
    context: Context,
    dtype: DateTime,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection
    yield from _handle_time_summary(connection, dtype.prop)


@commands.summary.register(Context, Time, PostgreSQL)
def summary(
    context: Context,
    dtype: Time,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection
    yield from _handle_time_summary(connection, dtype.prop)


def _handle_time_units(model_prop: Property, value):
    unit = model_prop.unit

    if not unit:
        return _handle_time_units_not_given(model_prop, value)
    else:
        given = _handle_time_units_given(model_prop, value)
        if not given:
            return _handle_time_units_not_given(model_prop, value)
        if isinstance(model_prop.dtype, Time):
            if given.date() != value.date():
                return _handle_time_units_not_given(model_prop, value)
        return given


def _handle_time_units_not_given(model_prop: Property, value):
    if isinstance(model_prop.dtype, (Date, DateTime)):
        return value + datetime.timedelta(days=100)
    else:
        if (value + datetime.timedelta(minutes=100)).hour < value.hour:
            if (value + datetime.timedelta(seconds=100)).hour < value.hour:
                if (value + datetime.timedelta(milliseconds=100)).hour < value.hour:
                    return value + datetime.timedelta(microseconds=100)
                return value + datetime.timedelta(milliseconds=100)
            return value + datetime.timedelta(seconds=100)
        return value + datetime.timedelta(minutes=100)


def _handle_time_units_given(model_prop: Property, value):
    items = _split_string(model_prop.unit)
    number = items[0]
    unit_type = items[2]
    time_units = ['H', 'T', 'S', 'L', 'U', 'N']
    date_units = ['Y', 'Q', 'M', 'W', 'D']
    if isinstance(model_prop.dtype, Time) and unit_type not in time_units:
        return None
    elif isinstance(model_prop.dtype, Date) and unit_type not in date_units:
        return None

    if unit_type == 'Y':
        return value + relativedelta(years=int(number) * 100)
    elif unit_type == 'Q':
        return value + relativedelta(months=int(number) * 3 * 100)
    elif unit_type == 'M':
        return value + relativedelta(months=int(number) * 100)
    elif unit_type == 'W':
        return value + relativedelta(weeks=int(number) * 100)
    elif unit_type == 'D':
        return value + relativedelta(days=int(number) * 100)
    elif unit_type == 'H':
        return value + relativedelta(hours=int(number) * 100)
    elif unit_type == 'T':
        return value + relativedelta(minutes=int(number) * 100)
    elif unit_type == 'S':
        return value + relativedelta(seconds=int(number) * 100)
    elif unit_type == 'L':
        return value + datetime.timedelta(milliseconds=int(number) * 100)
    elif unit_type == 'U':
        return value + datetime.timedelta(microseconds=int(number) * 100)


split_unit_pattern = re.compile(r'^(\d+)(.*?)([a-zA-Z]+)$')


def _split_string(string):
    match = split_unit_pattern.match(string)
    if match:
        groups = match.groups()
        return groups[0], groups[1], groups[2]
    else:
        return None


def _handle_time_summary(connection, model_prop: Property):
    try:
        prop = model_prop.name
        model = get_table_name(model_prop)
        min_value = None
        max_value = None
        if isinstance(model_prop.dtype, (Date, DateTime)):
            min_max = connection.execute(f'SELECT MIN("{prop}"::TIMESTAMP) , MAX("{prop}"::TIMESTAMP) FROM "{model}"')
            for item in min_max:
                min_value = item[0]
                max_value = item[1]
        else:
            min_max = connection.execute(f'SELECT MIN("{prop}") , MAX("{prop}") FROM "{model}"')
            for item in min_max:
                # 1970-01-01 is when postgresql start counting EPOCH
                if item[0] and item[1]:
                    min_value = datetime.datetime.combine(datetime.datetime(1970, 1, 1), item[0])
                    max_value = datetime.datetime.combine(datetime.datetime(1970, 1, 1), item[1])
        if min_value is None and max_value is None:
            return []
        else:
            if min_value == max_value:
                max_value = _handle_time_units(model_prop, min_value)

            bin_size = (max_value - min_value) / 100
            half_bin_size = bin_size / 2
            seq_start = calendar.timegm(min_value.timetuple())
            seq_end = calendar.timegm(max_value.timetuple())
            seq_step = bin_size.total_seconds()

            # reason for 'max_value + min(0.01, bin_size*0.01)', is that WIDTH_BUCKET max is not inclusive, so you need to increase it by small amount
            result = connection.execute(f'''
                SELECT 
                        WIDTH_BUCKET(EXTRACT(EPOCH FROM "{prop}"), {seq_start}, {seq_end + min(0.01, seq_step * 0.01)}, 100) AS bucket, 
                        COUNT(*) AS count,
                        (ARRAY_AGG(_id))[1] AS _id
                    FROM "{model}" 
                    GROUP BY 
                        bucket 
                    ORDER BY 
                        bucket;
                        ''')
            buckets = []
            for i in range(100):
                bucket_bin = i * bin_size + half_bin_size + min_value
                if isinstance(model_prop.dtype, Time):
                    bucket_bin = bucket_bin.time()
                buckets.append(
                    {
                        "bin": str(bucket_bin),
                        "count": 0,
                        "_type": model_prop.model.model_type()
                    }
                )
            for item in result:
                data = flat_dicts_to_nested(dict(item))
                if data["bucket"]:
                    bucket = buckets[data["bucket"] - 1]
                    bucket["count"] = data["count"]
                    if data['count'] == 1:
                        bucket["_id"] = data["_id"]
            yield from buckets
    except NotFoundError:
        raise ItemDoesNotExist(model_prop.model, id=model_prop.name)


@commands.summary.register(Context, Ref, PostgreSQL)
def summary(
    context: Context,
    dtype: Ref,
    backend: PostgreSQL
):
    connection = context.get('transaction').connection

    try:

        prop = dtype.prop.name
        model = get_table_name(dtype.prop)
        result = connection.execute(f'''
        SELECT * FROM "{model}"''')
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            print(data)
        result = connection.execute(f'''
                SELECT 
                    "{prop}._id" as bin, 
                    COUNT(model.*) AS count, 
                    (ARRAY_AGG(model._id))[1] AS _id
                FROM
                    "{model}" AS model 
                    GROUP BY 
                        "{prop}._id"
                    ORDER BY
                        count DESC
                    LIMIT 10;
                ''')
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            print(data)
            if data["count"] != 1:
                del data["_id"]
            data['_type'] = dtype.prop.model.model_type()
            yield data
    except NotFoundError:
        raise ItemDoesNotExist(dtype.prop.model, id=dtype.prop.name)
