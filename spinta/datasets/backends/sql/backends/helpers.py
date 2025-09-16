import sqlalchemy as sa


def nulls_last_asc(column: sa.Column):
    return sa.sql.expression.nullslast(column.asc())


def nulls_first_desc(column: sa.Column):
    return sa.sql.expression.nullsfirst(column.desc())
