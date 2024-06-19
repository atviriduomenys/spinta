from operator import itemgetter
from typing import Any, TypedDict
from typing import Dict
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Tuple

import sqlalchemy as sa
from geoalchemy2.types import Geometry
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects import mssql
from sqlalchemy.engine.reflection import Inspector
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


def read_schema(context: Context, path: str, prepare: str = None, dataset_name: str = ''):
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
    dataset = dataset_name if dataset_name else to_dataset_name(url.database) if url.database else 'dataset1'
    insp = sa.inspect(engine)

    table_mapper = [
        {
            "dataset": dataset,
            "dataset_given": dataset_name,
            "resource": 'resource1',
            "mapping": _create_mapping(insp, insp.get_table_names(schema=schema), schema, dataset)
        }
    ]

    get_view_names = getattr(insp, "get_view_names", None)
    get_materialized_view_names = getattr(insp, "get_materialized_view_names", None)
    views = []
    if callable(get_view_names):
        views += insp.get_view_names(schema=schema)
    if callable(get_materialized_view_names):
        views += insp.get_materialized_view_names(schema=schema)

    if views:
        dataset = f'{dataset}/views'
        dataset_name = f'{dataset_name}/views'
        table_mapper.append(
            {
                "dataset": dataset,
                "dataset_given": dataset_name,
                "resource": "resource1",
                "mapping": _create_mapping(insp, views, schema, dataset)
            }
        )

    for mapping_data in table_mapper:
        yield None, {
            'type': 'dataset',
            'name': mapping_data["dataset"],
            'resources': {
                mapping_data["resource"]: {
                    'type': 'sql',
                    'external': str(url.set(password='')),
                    'prepare': prepare
                },
            },
            'given_name': mapping_data["dataset_given"]
        }

        for table in sorted(mapping_data["mapping"]):
            yield None, {
                'type': 'model',
                'name': mapping_data["mapping"][table].model,
                'external': {
                    'dataset': mapping_data["dataset"],
                    'resource': mapping_data["resource"],
                    'name': table,
                    'pk': _get_primary_key(insp, table, schema, mapping_data["mapping"]),
                },
                'description': _get_table_comment(insp, schema, table),
                'properties': dict(_read_props(insp, table, schema, mapping_data["mapping"])),
            }


class _TableMapping(NamedTuple):
    model: str  # full model name
    props: Dict[
        str,    # column
        str,    # property
    ]


_Mapping = Dict[
    str,  # table
    _TableMapping,
]


def _create_mapping(insp: Inspector, tables: list, schema: str, dataset: str) -> _Mapping:
    dedup_model = Deduplicator('{}')
    mapping: _Mapping = {}
    for table in sorted(tables):
        model = to_model_name(table)
        model = dedup_model(model)
        dedup_prop = Deduplicator('_{}')
        props = {}
        for col in insp.get_columns(table, schema=schema):
            prop = to_property_name(col['name'])
            prop = dedup_prop(prop)
            props[col['name']] = prop
        mapping[table] = _TableMapping(
            dataset + '/' + model,
            props,
        )
    return mapping


def _get_primary_key(
    insp: Inspector,
    table: str,
    schema: str,
    mapping: _TableMapping,
) -> List[str]:
    pk = insp.get_pk_constraint(table, schema=schema)
    return [
        mapping[table].props[col]
        for col in pk['constrained_columns']
    ]


def _get_table_comment(insp: Inspector, schema: str, table: str) -> str:
    try:
        return insp.get_table_comment(table, schema=schema).get('text')
    except NotImplementedError:
        return ''


def _read_props(
    insp: Inspector,
    table: str,
    schema: str,
    mapping: _Mapping,
) -> Iterator[Tuple[
    str,
    Dict[str, Any],
]]:
    fkeys, cfkeys = _get_fkeys(insp, table, schema, mapping)

    cols = insp.get_columns(table, schema=schema)
    cols = sorted(cols, key=itemgetter('name'))
    for col in cols:
        name = col['name']
        prop = mapping[table].props[name]
        extra = {}

        if name in cfkeys:
            ref = cfkeys[name]
            yield ref.name, {
                'type': 'ref',
                'model': ref.model,
                'refprops': ref.props,
            }

        if name in fkeys:
            ref = fkeys[name]
            dtype = 'ref'
            extra = {
                'model': ref.model,
                'refprops': ref.props,
            }

        else:
            dtype = _get_column_type(col, table)

        yield prop, {
            'type': dtype,
            'external': {
                'name': name,
            },
            'description': col.get('comment') or '',
            **extra,
        }


TYPES = [
    (sa.Boolean, 'boolean'),
    (sa.Date, 'date'),
    (sa.DateTime, 'datetime'),
    (sa.Float, 'number'),
    (sa.Integer, 'integer'),
    (sa.Numeric, 'number'),
    (sa.Text, 'string'),
    (sa.Time, 'time'),
    (sa.LargeBinary, 'binary'),
    (sa.String, 'string'),
    (sa.VARCHAR, 'string'),
    (sa.CHAR, 'string'),
    (mysql.BIT, 'string'),
    (mysql.VARBINARY, 'string'),
    (mysql.VARCHAR, 'string'),
    (postgresql.ARRAY, 'array'),
    (postgresql.JSON, 'object'),
    (postgresql.JSONB, 'object'),
    (postgresql.UUID, 'string'),
    (postgresql.INTERVAL, 'integer'),  # total number of seconds
    (postgresql.OID, 'integer'),  # four-byte integer, https://www.postgresql.org/docs/current/datatype-oid.html
    (Geometry, 'geometry'),
    (oracle.ROWID, 'string'),
    (oracle.RAW, 'binary'),
    (mssql.MONEY, 'number'),  # TODO: https://github.com/atviriduomenys/spinta/issues/40
    (mssql.SMALLMONEY, 'number'),  # TODO: https://github.com/atviriduomenys/spinta/issues/40
    (mssql.UNIQUEIDENTIFIER, 'string'),  # Example: 6F9619FF-8B86-D011-B42D-00C04FC964FF
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
    name: str    # property name
    model: str   # referenced model
    props: List[str]  # referenced props


def _get_fkeys(
    insp: Inspector,
    table: str,
    schema: str,
    mapping: _Mapping,
) -> Tuple[
    Dict[       # foreign keys
        str,    # column (source)
        _Ref,
    ],
    Dict[       # composite foreign keys
        str,    # column (source)
        _Ref,
    ],
]:
    fkeys = {}
    cfkeys = {}
    for fk in insp.get_foreign_keys(table, schema=schema):
        composite = len(fk['constrained_columns']) > 1

        col = fk['constrained_columns'][0]

        rtable = fk['referred_table']

        if composite:
            name = '_'.join([
                mapping[table].props[c]
                for c in fk['constrained_columns']
            ])
        else:
            name = mapping[table].props[col]

        ref = _Ref(
            name=name,
            model=mapping[rtable].model,
            props=[
                mapping[rtable].props[rcol]
                for rcol in fk['referred_columns']
            ],
        )

        if composite:
            cfkeys[col] = ref
        else:
            fkeys[col] = ref

    return fkeys, cfkeys
