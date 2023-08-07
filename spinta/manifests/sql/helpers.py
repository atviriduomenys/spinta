from operator import itemgetter
from typing import Any
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
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import TypeEngine

from spinta.utils.naming import Deduplicator
from spinta.utils.naming import to_dataset_name
from spinta.utils.naming import to_model_name
from spinta.utils.naming import to_property_name


def read_schema(path: str):
    url = sa.engine.make_url(path)
    dataset = to_dataset_name(url.database) if url.database else 'dataset1'
    resource = 'resource1'
    yield None, {
        'type': 'dataset',
        'name': dataset,
        'resources': {
            resource: {
                'type': 'sql',
                'external': str(url.set(password='')),
            },
        },
    }

    engine = sa.create_engine(path)
    insp = sa.inspect(engine)
    mapping = _create_mapping(insp, dataset)
    for table in sorted(mapping):
        yield None, {
            'type': 'model',
            'name': mapping[table].model,
            'external': {
                'dataset': dataset,
                'resource': resource,
                'name': table,
                'pk': _get_primary_key(insp, table, mapping),
            },
            'description': _get_table_comment(insp, table),
            'properties': dict(_read_props(insp, table, mapping)),
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


def _create_mapping(insp: Inspector, dataset: str) -> _Mapping:
    dedup_model = Deduplicator('{}')
    mapping: _Mapping = {}
    for table in sorted(insp.get_table_names()):
        model = to_model_name(table)
        model = dedup_model(model)
        dedup_prop = Deduplicator('_{}')
        props = {}
        for col in insp.get_columns(table):
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
    mapping: _TableMapping,
) -> List[str]:
    pk = insp.get_pk_constraint(table)
    return [
        mapping[table].props[col]
        for col in pk['constrained_columns']
    ]


def _get_table_comment(insp: Inspector, table: str) -> str:
    try:
        return insp.get_table_comment(table).get('text')
    except NotImplementedError:
        return ''


def _read_props(
    insp: Inspector,
    table: str,
    mapping: _Mapping,
) -> Iterator[Tuple[
    str,
    Dict[str, Any],
]]:
    fkeys, cfkeys = _get_fkeys(insp, table, mapping)

    cols = insp.get_columns(table)
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
            dtype = _get_type(col['type'])

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
    (Geometry, 'geometry'),
    (oracle.ROWID, 'string'),
    (oracle.RAW, 'binary'),
]


def _get_type(sql_type: TypeEngine) -> str:
    for cls, name in TYPES:
        if isinstance(sql_type, cls):
            return name
    raise TypeError(f"Unknown type {sql_type!r}.")


class _Ref(NamedTuple):
    name: str    # property name
    model: str   # referenced model
    props: List[str]  # referenced props


def _get_fkeys(
    insp: Inspector,
    table: str,
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
    for fk in insp.get_foreign_keys(table):
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
