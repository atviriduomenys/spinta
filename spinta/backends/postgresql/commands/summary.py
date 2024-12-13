import calendar
import datetime
import sqlalchemy as sa

from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers.name import get_pg_table_name, get_pg_column_name
from spinta.core.ufuncs import Expr
from spinta.types.datatype import Integer, Number, Boolean, String, Date, DateTime, Time, Ref
from spinta import commands
from spinta.components import Context, Property
from spinta.components import Model
from spinta.exceptions import NotFoundError, NotImplementedFeature, InvalidRequestQuery
from spinta.exceptions import ItemDoesNotExist
from spinta.backends.postgresql.components import PostgreSQL
from spinta.types.geometry.components import Geometry
from spinta.ufuncs.summaryenv.components import SummaryEnv, BBox
from spinta.units.helpers import split_time_unit
from spinta.utils.nestedstruct import flat_dicts_to_nested

from dateutil.relativedelta import relativedelta


@commands.summary.register(Context, Model, PostgreSQL, Expr)
def summary(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    query: Expr
):
    env = SummaryEnv(context)
    env = env.init(model)
    env.resolve(query)
    kwargs = {}
    if env.bbox:
        kwargs['bbox'] = env.bbox
    yield from commands.summary(context, env.prop.dtype, backend, **kwargs)


@commands.summary.register(Context, Integer, PostgreSQL)
def summary(
    context: Context,
    dtype: Integer,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection
    yield from _handle_numeric_summary(connection, dtype.prop)


@commands.summary.register(Context, Number, PostgreSQL)
def summary(
    context: Context,
    dtype: Number,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection
    yield from _handle_numeric_summary(connection, dtype.prop)


def _handle_numeric_summary(connection, model_prop: Property):
    try:
        prop = get_pg_column_name(model_prop.place)
        model = get_pg_table_name(get_table_name(model_prop))
        min_value, max_value = connection.execute(f'SELECT MIN("{prop}") , MAX("{prop}") FROM "{model}"').fetchone()
        if min_value is None and max_value is None:
            return []

        if min_value == max_value:
            max_value += 100

        bin_size = (max_value - min_value) / 100
        half_bin_size = bin_size / 2

        # reason for 'max_value + min(0.01, bin_size*0.01)', is that WIDTH_BUCKET max is not inclusive, so you need to increase it by small amount
        result = connection.execute(f'''
            SELECT 
                WIDTH_BUCKET("{prop}", {min_value}, {max_value + min(0.01, bin_size * 0.01)}, 100) AS bucket, 
                COUNT(*) AS count,
                MIN(_id::text) AS _id
            FROM "{model}" 
            GROUP BY 
                bucket 
            ORDER BY 
                bucket;
        ''')
        model_type = model_prop.model.model_type()
        base_bucket_bin = min_value + half_bin_size
        buckets = [
            {
                "bin": base_bucket_bin + i * bin_size,
                "count": 0,
                "_type": model_type
            }
            for i in range(100)
        ]

        for item in result:
            data = flat_dicts_to_nested(dict(item))
            bucket_index = data["bucket"] - 1
            if 0 <= bucket_index < 100:
                buckets[bucket_index]["count"] = data["count"]
                if data['count'] == 1:
                    buckets[bucket_index]["_id"] = data["_id"]
        yield from buckets
    except NotFoundError:
        raise ItemDoesNotExist(model_prop.model, id=model_prop.name)


@commands.summary.register(Context, Boolean, PostgreSQL)
def summary(
    context: Context,
    dtype: Boolean,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection
    try:
        prop = get_pg_column_name(dtype.prop.place)
        model = get_pg_table_name(get_table_name(dtype.prop))

        result = connection.execute(f'''
                SELECT 
                    bin, 
                    COUNT(model.*) AS count, 
                    MIN(model._id::text) AS _id
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
        model_type = dtype.prop.model.model_type()
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            if data["count"] != 1:
                del data["_id"]
            data['_type'] = model_type
            yield data
    except NotFoundError:
        raise ItemDoesNotExist(dtype.prop.model, id=dtype.prop.name)


@commands.summary.register(Context, String, PostgreSQL)
def summary(
    context: Context,
    dtype: String,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection

    if not dtype.prop.enum:
        raise NotImplementedFeature(dtype.prop,
                                    feature="Ability to generate summary for String type properties that do not have Enum")
    enum_list = []
    for key, value in dtype.prop.enum.items():
        enum_list.append(value.prepare)
    try:
        prop = get_pg_column_name(dtype.prop.place)
        model = get_pg_table_name(get_table_name(dtype.prop))

        result = connection.execute(f'''
                SELECT 
                    bin, 
                    COUNT(model.*) AS count, 
                    MIN(model._id::text) AS _id
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
        model_type = dtype.prop.model.model_type()
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            if data["count"] != 1:
                del data["_id"]
            data['_type'] = model_type
            yield data
    except NotFoundError:
        raise ItemDoesNotExist(dtype.prop.model, id=dtype.prop.name)


@commands.summary.register(Context, Date, PostgreSQL)
def summary(
    context: Context,
    dtype: Date,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection
    yield from _handle_time_summary(connection, dtype.prop)


@commands.summary.register(Context, DateTime, PostgreSQL)
def summary(
    context: Context,
    dtype: DateTime,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection
    yield from _handle_time_summary(connection, dtype.prop)


@commands.summary.register(Context, Time, PostgreSQL)
def summary(
    context: Context,
    dtype: Time,
    backend: PostgreSQL,
    **kwargs
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
    items = split_time_unit(model_prop.unit)
    if items is None:
        return None
    number = items[0]
    unit_type = items[1]
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


def _handle_time_summary(connection, model_prop: Property):
    try:
        prop = get_pg_column_name(model_prop.place)
        model = get_pg_table_name(get_table_name(model_prop))
        if isinstance(model_prop.dtype, (Date, DateTime)):
            min_value, max_value = connection.execute(f'SELECT MIN("{prop}"::TIMESTAMP) , MAX("{prop}"::TIMESTAMP) FROM "{model}"').fetchone()
        else:
            min_value, max_value = connection.execute(f'SELECT MIN("{prop}") , MAX("{prop}") FROM "{model}"').fetchone()
            if min_value and max_value:
                min_value = datetime.datetime.combine(datetime.datetime(1970, 1, 1), min_value)
                max_value = datetime.datetime.combine(datetime.datetime(1970, 1, 1), max_value)

        if min_value is None and max_value is None:
            return []

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
                    MIN(_id::text) AS _id
                FROM "{model}" 
                GROUP BY 
                    bucket 
                ORDER BY 
                    bucket;
                    ''')

        base_bucket_bin = min_value + half_bin_size
        is_time_type = isinstance(model_prop.dtype, Time)
        model_type = model_prop.model.model_type()
        buckets = [
            {
                "bin": str((base_bucket_bin + i * bin_size).time() if is_time_type else base_bucket_bin + i * bin_size),
                "count": 0,
                "_type": model_type
            }
            for i in range(100)
        ]
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            bucket_index = data["bucket"] - 1
            if 0 <= bucket_index < 100:
                buckets[bucket_index]["count"] = data["count"]
                if data['count'] == 1:
                    buckets[bucket_index]["_id"] = data["_id"]

        yield from buckets
    except NotFoundError:
        raise ItemDoesNotExist(model_prop.model, id=model_prop.name)


@commands.summary.register(Context, Ref, PostgreSQL)
def summary(
    context: Context,
    dtype: Ref,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection

    try:
        prop = get_pg_column_name(dtype.prop.place)
        key = "_id"
        if not commands.identifiable(dtype.prop):
            if len(dtype.refprops) > 1:
                raise NotImplementedFeature(dtype.prop,
                                            feature="Ability to get summary for Ref type Property, when level is 3 and below and there are multiple refprops")
            key = dtype.refprops[0].name
        model = get_pg_table_name(get_table_name(dtype.prop))
        uri = dtype.model.uri
        prefixes = dtype.model.external.dataset.prefixes
        label = None
        if uri and ":" in uri:
            if uri.startswith(('http://', 'https://')):
                label = uri
            else:
                split = uri.split(":")
                if split[0] in prefixes.keys():
                    label = f'{prefixes[split[0]].uri}{split[1]}'

        result = connection.execute(f'''
                SELECT 
                    "{prop}.{key}" AS bin, 
                    COUNT(model.*) AS count, 
                    MIN(model._id::text) AS _id,
                    MIN(model._created) AS created_at
                FROM
                    "{model}" AS model 
                    GROUP BY 
                        "{prop}.{key}"
                    ORDER BY
                        count DESC,
                        created_at ASC
                    LIMIT 10;
                ''')
        model_type = dtype.prop.model.model_type()
        for item in result:
            data = flat_dicts_to_nested(dict(item))

            del data["created_at"]
            if key == "_id":
                data["bin"] = extract_uuid(str(data["bin"]))
            if label:
                data["label"] = label
            if data["count"] != 1:
                del data["_id"]
            data['_type'] = model_type
            yield data
    except NotFoundError:
        raise ItemDoesNotExist(dtype.prop.model, id=dtype.prop.name)


def extract_uuid(uuid: str) -> str:
    return uuid[:8]


@commands.summary.register(Context, Geometry, PostgreSQL)
def summary(
    context: Context,
    dtype: Geometry,
    backend: PostgreSQL,
    **kwargs
):
    connection = context.get('transaction').connection
    try:
        prop = get_pg_column_name(dtype.prop.place)
        model = get_pg_table_name(get_table_name(dtype.prop))
        bounding_box = ""
        params = {}
        if "bbox" in kwargs:
            bbox = kwargs["bbox"]

            if isinstance(bbox, BBox):
                if dtype.srid:
                    bounding_box = f'WHERE ST_Intersects("{prop}", ST_MakeEnvelope(:x_min, :y_min, :x_max, :y_max, :srid))'
                else:
                    bounding_box = f'WHERE ST_Intersects("{prop}", ST_MakeEnvelope(:x_min, :y_min, :x_max, :y_max))'
            else:
                raise InvalidRequestQuery(query="bbox", format="bbox(min_lon, min_lat, max_lon, max_lat)")

            params = {
                'x_min': bbox.x_min,
                'y_min': bbox.y_min,
                'x_max': bbox.x_max,
                'y_max': bbox.y_max,
                'srid': dtype.srid
            }

        maximum_cluster_amount = 25
        # This is more optimized, since it will only return up to set limit of rows instead of trying to calculate all
        # Limit is the amount of clusters to create
        count_exec = connection.execute(
            sa.text(
                f'''
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT "{prop}"
                    FROM "{model}"
                    {bounding_box}
                    LIMIT {maximum_cluster_amount}
                ) AS filtered_data;
                '''),
            params
        )
        count = 0
        for item in count_exec:
            count = item[0]
        params['count'] = count

        result = connection.execute(
            sa.text(f'''
                WITH clusters AS (
                    SELECT 
                    ST_ClusterKMeans(
                        model."{prop}", 
                        :count
                    ) OVER() AS cluster_id,
                    model."{prop}" AS geom,
                    model._id AS _id,
                    model._created AS created_at
                    FROM "{model}" AS model
                    {bounding_box}
                )
                SELECT 
                    ST_NumGeometries(ST_Collect(geom)) AS cluster,
                    ST_AsText(ST_Centroid(ST_Collect(geom))) AS centroid,
                    MIN(clusters._id::text) AS _id
                FROM clusters
                GROUP BY cluster_id
                ORDER BY MIN(clusters.created_at);
                '''),
            params
        )
        model_type = dtype.prop.model.model_type()
        for item in result:
            data = flat_dicts_to_nested(dict(item))
            if data["cluster"]:
                if data["cluster"] != 1:
                    del data["_id"]
                data['_type'] = model_type
                yield data
    except NotFoundError:
        raise ItemDoesNotExist(dtype.prop.model, id=dtype.prop.name)
