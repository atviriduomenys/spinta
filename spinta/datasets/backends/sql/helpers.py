from collections.abc import Sequence
from typing import Union, Tuple

import geoalchemy2.functions
import sqlalchemy as sa


def _extract_dialect(engine: sa.engine.Engine):
    if engine is not None:
        return engine.dialect.name
    return _DEFAULT_DIALECT_KEY


def _dialect_matches(src_dialect: str, target_dialect: Union[str, Tuple[str]]):
    if not target_dialect or not src_dialect:
        return False

    if isinstance(target_dialect, str):
        return target_dialect in src_dialect
    elif isinstance(target_dialect, tuple):
        for dialect in target_dialect:
            if dialect in src_dialect:
                return True

    return False


def _nulls_asc(column: sa.Column):
    return sa.sql.expression.nullslast(column.asc())


# Reason for column == None is NULLS LAST, because
# if it's NULL it will return 1 and if it's not NULL it will return 0
# when ordering 0 takes priority over 1, so then it will sort first values that are not NULL
def _default_asc(column: sa.Column):
    return sa.case(
        [
            (
                column == None,
                sa.literal_column('1', type_=sa.Integer)
            )
        ],
        else_=sa.literal_column('0', type_=sa.Integer)
    ), column.asc()


def _nulls_desc(column: sa.Column):
    return sa.sql.expression.nullsfirst(column.desc())


# Reason for column != None is NULLS FIRST, because
# if it's NULL it will return 0 and if it's not NULL it will return 1
# when ordering 0 takes priority over 1, so then it will sort first values that are NULL
def _default_desc(column: sa.Column):
    return sa.case(
        [
            (
                column != None,
                sa.literal_column('1', type_=sa.Integer)
            )
        ],
        else_=sa.literal_column('0', type_=sa.Integer)
    ), column.desc()


def _flip_geometry_postgis(column: sa.Column):
    return geoalchemy2.functions.ST_FlipCoordinates(column)


def _default_flip(column: sa.Column):
    return column


def _group_array_sqlite(column: Union[sa.Column, Sequence[sa.Column]]):
    if isinstance(column, Sequence) and not isinstance(column, str):
        column = sa.sql.func.json_array(*column)
    return sa.sql.func.json_group_array(column)


def _group_array_postgresql(column: Union[sa.Column, Sequence[sa.Column]]):
    if isinstance(column, Sequence) and not isinstance(column, str):
        column = sa.sql.func.jsonb_build_array(*column)
    return sa.sql.func.jsonb_agg(column)


def _group_array_mysql(column: Union[sa.Column, Sequence[sa.Column]]):
    if isinstance(column, Sequence) and not isinstance(column, str):
        column = sa.sql.func.json_array(*column)
    return sa.sql.func.json_arrayagg(column)


def _group_array_mssql(column: Union[sa.Column, Sequence[sa.Column]]):
    if isinstance(column, Sequence) and not isinstance(column, str):
        column = sa.sql.func.json_array(*column)
    # Using concat for mssql, because `json_arrayagg` is only available for Azure SQL version
    # https://learn.microsoft.com/en-us/sql/t-sql/functions/json-array-transact-sql?view=azuresqldb-current
    return sa.sql.func.concat('[', column, ']')


def _default_group_array(column: sa.Column):
    raise NotImplemented("Current Sql dialect currently does not support array aggregation.")


_DEFAULT_DIALECT_KEY = ""


_DESC_DIALECT_MAPPER = {
    ("postgresql", "oracle", "sqlite"): _nulls_desc,
    _DEFAULT_DIALECT_KEY: _default_desc
}
_ASC_DIALECT_MAPPER = {
    ("postgresql", "oracle", "sqlite"): _nulls_asc,
    _DEFAULT_DIALECT_KEY: _default_asc
}
_GEOMETRY_FLIP_DIALECT_MAPPER = {
    "postgresql": _flip_geometry_postgis,
    _DEFAULT_DIALECT_KEY: _default_flip
}
_GROUP_ARRAY_DIALECT_MAPPER = {
    "sqlite": _group_array_sqlite,
    "postgresql": _group_array_postgresql,
    "mssql": _group_array_mssql,
    ("mysql", "mariadb"): _group_array_mysql,
    _DEFAULT_DIALECT_KEY: _default_group_array
}


def _dialect_specific_function(engine: sa.engine.Engine, dialect_function_mapper: dict, **kwargs):
    dialect = _extract_dialect(engine)
    for key, func in dialect_function_mapper.items():
        if key != _DEFAULT_DIALECT_KEY and _dialect_matches(dialect, key):
            return func(**kwargs)

    return dialect_function_mapper[_DEFAULT_DIALECT_KEY](**kwargs)


def _contains_dialect_function(engine: sa.engine.Engine, dialect_function_mapper: dict) -> bool:
    dialect = _extract_dialect(engine)
    for key, func in dialect_function_mapper.items():
        if key != _DEFAULT_DIALECT_KEY and _dialect_matches(dialect, key):
            return True
    return False


def dialect_specific_desc(engine: sa.engine.Engine, column: sa.Column):
    return _dialect_specific_function(
        engine=engine,
        dialect_function_mapper=_DESC_DIALECT_MAPPER,
        column=column
    )


def dialect_specific_asc(engine: sa.engine.Engine, column: sa.Column):
    return _dialect_specific_function(
        engine=engine,
        dialect_function_mapper=_ASC_DIALECT_MAPPER,
        column=column
    )


def dialect_specific_geometry_flip(engine: sa.engine, column: sa.Column):
    return _dialect_specific_function(
        engine=engine,
        dialect_function_mapper=_GEOMETRY_FLIP_DIALECT_MAPPER,
        column=column
    )


def dialect_specific_group_array(engine: sa.engine, column: Union[sa.Column, Sequence[sa.Column]]):
    return _dialect_specific_function(
        engine=engine,
        dialect_function_mapper=_GROUP_ARRAY_DIALECT_MAPPER,
        column=column
    )


def contains_geometry_flip_function(engine: sa.engine) -> bool:
    return _contains_dialect_function(engine, _GEOMETRY_FLIP_DIALECT_MAPPER)
