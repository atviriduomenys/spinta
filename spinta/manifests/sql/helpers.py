import dataclasses
from operator import itemgetter
from typing import Any, TypedDict
from typing import Dict
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Tuple

import cachetools
import sqlalchemy as sa
from geoalchemy2.types import Geometry
from sqlalchemy.dialects import postgresql, mysql, sqlite, mssql, oracle
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.sqltypes import _Binary
from sqlalchemy.types import TypeEngine

from spinta import spyna
from spinta.components import Context
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.backends.sql.ufuncs.components import SqlResource, Engine
from spinta.exceptions import UnexpectedFormulaResult
from spinta.utils.imports import full_class_name
from spinta.utils.naming import Deduplicator
from spinta.utils.naming import to_dataset_name
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def read_schema(context: Context, path: str, prepare: str = None, dataset_name: str = ""):
    engine = sa.create_engine(path)
    schema = None
    if prepare:
        env = SqlResource(context).init(path)
        parsed = spyna.parse(prepare)
        converted = asttoexpr(parsed)
        engine = env.resolve(converted)
        engine = env.execute(engine)
        if not isinstance(engine, Engine):
            raise UnexpectedFormulaResult(
                formula=spyna.unparse(converted),
                expected=full_class_name(Engine),
                received=full_class_name(engine),
            )
        schema = engine.schema
        engine = engine.create()

    url = sa.engine.make_url(path)
    dataset = dataset_name if dataset_name else to_dataset_name(url.database) if url.database else "dataset1"
    insp = sa.inspect(engine)
    default_schema = schema or insp.default_schema_name

    schema_mapper: dict[str, _SchemaMapping] = {}
    schemas = insp.get_schema_names() if schema is None else [schema]
    for schema in schemas:
        if is_internal_schema(engine, schema):
            continue

        include_schema_in_name = schema != default_schema
        ds = (dataset + "/" + schema) if include_schema_in_name else dataset
        table_mapping = _create_mapping(insp, insp.get_table_names(schema=schema), schema, ds)

        get_view_names = getattr(insp, "get_view_names", None)
        get_materialized_view_names = getattr(insp, "get_materialized_view_names", None)
        views = []
        if callable(get_view_names):
            views += insp.get_view_names(schema=schema)
        if callable(get_materialized_view_names):
            views += insp.get_materialized_view_names(schema=schema)

        views_mapping = {}
        if views:
            views_ds = f"{ds}/views"
            views_mapping = _create_mapping(insp, views, schema, views_ds)

        schema_map = _SchemaMapping(schema=schema, resource="resource1", dataset=ds)
        schema_map.tables = table_mapping
        schema_map.views = views_mapping
        schema_mapper[schema] = schema_map

    for mapping_data in schema_mapper.values():
        include_schema_in_name = mapping_data.schema != default_schema
        if mapping_data.tables:
            yield from _create_dataset_for_schema(
                schema=mapping_data.schema,
                dataset=mapping_data.dataset,
                dataset_given=mapping_data.dataset_given,
                resource=mapping_data.resource,
                tables=mapping_data.tables,
                prepare=prepare,
                url=url,
                schema_mapper=schema_mapper,
                insp=insp,
                include_schema_in_name=include_schema_in_name,
            )

        if mapping_data.views:
            yield from _create_dataset_for_schema(
                schema=mapping_data.schema,
                dataset=f"{mapping_data.dataset}/views",
                dataset_given=f"{mapping_data.dataset_given}/views",
                resource=mapping_data.resource,
                tables=mapping_data.views,
                prepare=prepare,
                url=url,
                schema_mapper=schema_mapper,
                insp=insp,
                include_schema_in_name=include_schema_in_name,
            )


class _TableMapping(NamedTuple):
    model: str  # full model name
    schema: str
    props: Dict[
        str,  # column
        str,  # property
    ]


_Mapping = Dict[
    str,  # table
    _TableMapping,
]


@dataclasses.dataclass
class _SchemaMapping:
    schema: str
    dataset: str
    resource: str
    dataset_given: str = None
    tables: _Mapping = dataclasses.field(default_factory=dict)
    views: _Mapping = dataclasses.field(default_factory=dict)

    def get_table(self, table: str) -> _TableMapping:
        if table in self.tables:
            return self.tables[table]

        if table in self.views:
            return self.views[table]

        raise KeyError(table)


def _create_mapping(insp: Inspector, tables: list, schema: str, dataset: str) -> _Mapping:
    dedup_model = Deduplicator("{}")
    mapping: _Mapping = {}
    for table in sorted(tables):
        model = to_model_name(table)
        model = dedup_model(model)
        dedup_prop = Deduplicator("_{}")
        props = {}
        for col in insp.get_columns(table, schema=schema):
            prop = to_property_name(col["name"])
            prop = dedup_prop(prop)
            props[col["name"]] = prop
        mapping[table] = _TableMapping(
            dataset + "/" + model,
            schema,
            props,
        )
    return mapping


def _get_primary_key(
    insp: Inspector,
    table: str,
    schema: str,
    mapping: dict[str, _SchemaMapping],
) -> List[str]:
    pk = insp.get_pk_constraint(table, schema=schema)
    return [mapping[schema].get_table(table).props[col] for col in pk["constrained_columns"]]


def _get_table_comment(insp: Inspector, schema: str, table: str) -> str:
    try:
        return insp.get_table_comment(table, schema=schema).get("text")
    except NotImplementedError:
        return ""


def _read_props(
    insp: Inspector,
    table: str,
    schema: str,
    mapping: dict[str, _SchemaMapping],
) -> Iterator[
    Tuple[
        str,
        Dict[str, Any],
    ]
]:
    fkeys, cfkeys = _get_fkeys(insp, table, schema, mapping)

    cols = insp.get_columns(table, schema=schema)
    cols = sorted(cols, key=itemgetter("name"))
    for col in cols:
        name = col["name"]
        prop = mapping[schema].get_table(table).props[name]
        extra = {}

        if name in cfkeys:
            ref = cfkeys[name]
            yield (
                ref.name,
                {
                    "type": "ref",
                    "model": ref.model,
                    "refprops": ref.props,
                },
            )

        if name in fkeys:
            ref = fkeys[name]
            dtype = "ref"
            extra = {
                "model": ref.model,
                "refprops": ref.props,
            }

        else:
            dtype = _get_column_type(col, table)

        yield (
            prop,
            {
                "type": dtype,
                "external": {
                    "name": name,
                },
                "description": col.get("comment") or "",
                **extra,
            },
        )


TYPES = [
    (sa.Boolean, "boolean"),
    (sa.Date, "date"),
    (sa.DateTime, "datetime"),
    (sa.Float, "number"),
    (sa.Integer, "integer"),
    (sa.Numeric, "number"),
    (sa.Text, "string"),
    (sa.Time, "time"),
    # Using _Binary (private class) to catch all binary types including:
    # sa.LargeBinary, mysql.BLOB, mysql.LONGBLOB, oracle.RAW, etc.
    (_Binary, "binary"),
    (sa.String, "string"),
    (sa.VARCHAR, "string"),
    (sa.CHAR, "string"),
    (mysql.BIT, "string"),
    (mysql.VARBINARY, "string"),
    (mysql.VARCHAR, "string"),
    (postgresql.ARRAY, "array"),
    (postgresql.JSON, "object"),
    (postgresql.JSONB, "object"),
    (postgresql.UUID, "string"),
    (postgresql.INTERVAL, "integer"),  # total number of seconds
    (postgresql.OID, "integer"),  # four-byte integer, https://www.postgresql.org/docs/current/datatype-oid.html
    (Geometry, "geometry"),
    (oracle.ROWID, "string"),
    (mssql.MONEY, "number"),  # TODO: https://github.com/atviriduomenys/spinta/issues/40
    (mssql.SMALLMONEY, "number"),  # TODO: https://github.com/atviriduomenys/spinta/issues/40
    (mssql.UNIQUEIDENTIFIER, "string"),  # Example: 6F9619FF-8B86-D011-B42D-00C04FC964FF
]


class _Column(TypedDict):
    type: TypeEngine


def _get_column_type(column: _Column, table: str = None) -> str:
    column_type: TypeEngine = column["type"]
    for cls, name in TYPES:
        if isinstance(column_type, cls):
            return name
    raise TypeError(f"Unknown type {column_type!r} of column {column!r} in table {table!r}.")


class _Ref(NamedTuple):
    name: str  # property name
    model: str  # referenced model
    props: List[str]  # referenced props


def _get_fkeys(
    insp: Inspector,
    table: str,
    schema: str,
    mapping: dict[str, _SchemaMapping],
) -> Tuple[
    Dict[  # foreign keys
        str,  # column (source)
        _Ref,
    ],
    Dict[  # composite foreign keys
        str,  # column (source)
        _Ref,
    ],
]:
    fkeys = {}
    cfkeys = {}
    for fk in insp.get_foreign_keys(table, schema=schema):
        composite = len(fk["constrained_columns"]) > 1

        col = fk["constrained_columns"][0]

        rtable = fk["referred_table"]
        rschema = fk["referred_schema"] or schema

        if composite:
            name = "_".join([mapping[schema].get_table(table).props[c] for c in fk["constrained_columns"]])
        else:
            name = mapping[schema].get_table(table).props[col]

        referenced_model_pkeys = _get_primary_key(insp, rtable, rschema, mapping)
        refprops = [mapping[rschema].get_table(rtable).props[rcol] for rcol in fk["referred_columns"]]
        ref = _Ref(
            name=name,
            model=mapping[rschema].get_table(rtable).model,
            props=[] if referenced_model_pkeys == refprops else refprops,
        )

        if composite:
            cfkeys[col] = ref
        else:
            fkeys[col] = ref

    return fkeys, cfkeys


def _create_dataset_for_schema(
    schema: str,
    dataset: str,
    dataset_given: str | None,
    resource: str,
    tables: dict[str, _TableMapping],
    prepare: str,
    url: object,
    schema_mapper: dict[str, _SchemaMapping],
    insp: Inspector,
    include_schema_in_name: bool,
):
    yield (
        None,
        {
            "type": "dataset",
            "name": dataset,
            "resources": {
                resource: {
                    "type": "sql",
                    "external": str(url.set(password="")),
                    "prepare": prepare,
                },
            },
            "given_name": dataset_given,
        },
    )

    for table in sorted(tables):
        table_data = tables[table]
        schema = schema
        yield (
            None,
            {
                "type": "model",
                "name": table_data.model,
                "external": {
                    "dataset": dataset,
                    "resource": resource,
                    "name": f"{schema}.{table}" if include_schema_in_name else table,
                    "pk": _get_primary_key(insp, table, schema, schema_mapper),
                },
                "description": _get_table_comment(insp, schema, table),
                "properties": dict(_read_props(insp, table, schema, schema_mapper)),
            },
        )


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1024))
def oracle_maintained_schemas(engine: Engine) -> Iterator[str]:
    query = sa.text("""
        SELECT USERNAME 
        FROM ALL_USERS
        WHERE ORACLE_MAINTAINED = 'Y'
    """)
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
        return {r[0].upper() for r in rows}


def is_internal_schema(engine: Engine, schema: str) -> bool:
    if schema is None:
        return False

    dialect = engine.dialect
    if isinstance(dialect, postgresql.dialect):
        if schema == "information_schema":
            return True
        if schema.startswith("pg_"):
            return True
        return False

    elif isinstance(dialect, mysql.dialect):
        return schema in {
            "mysql",
            "information_schema",
            "performance_schema",
            "sys",
        }

    elif isinstance(dialect, sqlite.dialect):
        return schema == "temp"

    elif isinstance(dialect, mssql.dialect):
        return schema in {"sys", "INFORMATION_SCHEMA"}

    elif isinstance(dialect, oracle.dialect):
        schema_upper = schema.upper()
        oracle_schemas = oracle_maintained_schemas(engine)
        return schema_upper in oracle_schemas

    # Fallback: nothing internal by default for unknown/other dialects
    return False
