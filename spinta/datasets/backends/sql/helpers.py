from typing import Union, List, Tuple

import sqlalchemy as sa


def extract_dialect(engine: sa.engine.Engine):
    return engine.dialect.name


def does_dialect_match(src_dialect: str, target_dialect: Union[str, Tuple[str]]):
    if not target_dialect or not src_dialect:
        return False

    if isinstance(target_dialect, str):
        return target_dialect in src_dialect
    elif isinstance(target_dialect, tuple):
        for dialect in target_dialect:
            if dialect in src_dialect:
                return True

    return False


def _postgresql_asc(column: sa.Column):
    return sa.sql.expression.nullslast(column.asc())


def _default_asc(column: sa.Column):
    return column != None, column.asc()


def _postgresql_desc(column: sa.Column):
    return sa.sql.expression.nullsfirst(column.desc())


def _default_desc(column: sa.Column):
    return column == None, column.desc()


_DEFAULT_DIALECT_KEY = ""


_DESC_DIALECT_MAPPER = {
    ("postgresql", "oracle", "sqlite"): _postgresql_desc,
    _DEFAULT_DIALECT_KEY: _default_desc
}
_ASC_DIALECT_MAPPER = {
    ("postgresql", "oracle", "sqlite"): _postgresql_asc,
    _DEFAULT_DIALECT_KEY: _default_asc
}


def _dialect_specific_function(engine: sa.engine.Engine, dialect_function_mapper: dict, **kwargs):
    dialect = extract_dialect(engine)
    for key, func in dialect_function_mapper.items():
        if key != _DEFAULT_DIALECT_KEY and does_dialect_match(dialect, key):
            return func(**kwargs)

    return dialect_function_mapper[_DEFAULT_DIALECT_KEY](**kwargs)


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
