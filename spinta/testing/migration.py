import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from spinta.backends.postgresql.helpers import get_pg_sequence_name
from spinta.backends.postgresql.helpers.name import PG_NAMING_CONVENTION, get_pg_removed_name, get_removed_name
from spinta.utils.sqlalchemy import Convention

pg_identifier_preparer = postgresql.dialect().identifier_preparer


def get_table_unique_constraint_columns(table: sa.Table):
    constraint_columns = []
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            column_names = [column.name for column in constraint.columns]
            constraint_columns.append(column_names)
    return constraint_columns


def get_table_foreign_key_constraint_columns(table: sa.Table):
    constraint_columns = []
    for constraint in table.constraints:
        if isinstance(constraint, sa.ForeignKeyConstraint):
            column_names = [column.name for column in constraint.columns]
            element_names = [element.column.name for element in constraint.elements]
            constraint_columns.append(
                {
                    "constraint_name": constraint.name,
                    "column_names": column_names,
                    "referred_column_names": element_names,
                }
            )
    return constraint_columns


def add_index(index_name: str, table: str, columns: list[str]) -> str:
    column = ",".join(pg_identifier_preparer.quote(column) for column in columns)
    return f"CREATE INDEX {pg_identifier_preparer.quote(index_name)} ON {pg_identifier_preparer.quote(table)} ({column});\n\n"


def add_column_comment(table: str, column: str, comment: str = None) -> str:
    if comment is None:
        comment = column
    escaped_column = pg_identifier_preparer.quote(column)
    return f"COMMENT ON COLUMN {pg_identifier_preparer.quote(table)}.{escaped_column} IS '{comment}';\n\n"


def add_table_comment(table: str, comment: str) -> str:
    return f"COMMENT ON TABLE {pg_identifier_preparer.quote(table)} IS '{comment}';\n\n"


def add_column(table: str, column: str, column_type: str, comment: str = None):
    return (
        f"ALTER TABLE {pg_identifier_preparer.quote(table)} ADD COLUMN {pg_identifier_preparer.quote(column)} {column_type};\n"
        "\n"
        f"{add_column_comment(table=table, column=column, comment=comment)}"
    )


def drop_column(table: str, column: str, comment: str = None):
    removed_name = get_pg_removed_name(column)
    if comment is None:
        comment = removed_name

    return (
        f"{add_column_comment(table=table, column=column, comment=comment)}"
        f"ALTER TABLE {pg_identifier_preparer.quote(table)} RENAME "
        f"{pg_identifier_preparer.quote(column)} TO {pg_identifier_preparer.quote(removed_name)};\n\n"
    )


def rename_column(table: str, column: str, new_name: str, comment: str = None):
    if comment is None:
        comment = new_name
    return (
        f"{add_column_comment(table=table, column=column, comment=comment)}"
        f"ALTER TABLE {pg_identifier_preparer.quote(table)} RENAME "
        f"{pg_identifier_preparer.quote(column)} TO {pg_identifier_preparer.quote(new_name)};\n\n"
    )


def rename_table(table: str, new_name: str, comment: str = None):
    if comment is None:
        comment = new_name

    return (
        f"ALTER TABLE {pg_identifier_preparer.quote(table)} RENAME TO {pg_identifier_preparer.quote(new_name)};\n\n"
        f"{add_table_comment(table=new_name, comment=comment)}"
    )


def rename_changelog(table: str, new_name: str, comment: str = None):
    old_sequence_name = get_pg_sequence_name(name=table)
    new_sequence_name = get_pg_sequence_name(name=new_name)
    return (
        f"{rename_table(table=table, new_name=new_name, comment=comment)}"
        f"ALTER SEQUENCE {pg_identifier_preparer.quote(old_sequence_name)} RENAME TO "
        f"{pg_identifier_preparer.quote(new_sequence_name)};\n\n"
    )


def add_changelog_table(table: str, comment: str = None) -> str:
    if comment is None:
        comment = table

    pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": table}
    index_name = PG_NAMING_CONVENTION[Convention.IX] % {"table_name": table, "column_0_N_name": "_txn"}
    return (
        f'CREATE TABLE "{table}" (\n'
        "    _id BIGSERIAL NOT NULL, \n"
        "    _revision VARCHAR, \n"
        "    _txn UUID, \n"
        "    _rid UUID, \n"
        "    datetime TIMESTAMP WITHOUT TIME ZONE, \n"
        "    action VARCHAR(8), \n"
        "    data JSONB, \n"
        f'    CONSTRAINT "{pk_constraint}" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        f"{add_index(index_name=index_name, table=table, columns=['_txn'])}"
        f"{add_column_comment(table=table, column='_id')}"
        f"{add_column_comment(table=table, column='_revision')}"
        f"{add_column_comment(table=table, column='_txn')}"
        f"{add_column_comment(table=table, column='_rid')}"
        f"{add_column_comment(table=table, column='datetime')}"
        f"{add_column_comment(table=table, column='action')}"
        f"{add_column_comment(table=table, column='data')}"
        f"{add_table_comment(table=table, comment=comment)}"
    )


def add_redirect_table(table: str, comment: str = None) -> str:
    if comment is None:
        comment = table

    pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": table}
    index_name = PG_NAMING_CONVENTION[Convention.IX] % {"table_name": table, "column_0_N_name": "redirect"}
    return (
        f'CREATE TABLE "{table}" (\n'
        "    _id UUID NOT NULL, \n"
        "    redirect UUID, \n"
        f'    CONSTRAINT "{pk_constraint}" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        f"{add_index(index_name, table, ['redirect'])}"
        f"{add_column_comment(table, '_id')}"
        f"{add_column_comment(table, 'redirect')}"
        f"{add_table_comment(table, comment)}"
    )


def drop_table(table: str, remove_model_only: bool = False, comment: str = None):
    removed_name = get_pg_removed_name(table, remove_model_only)
    if comment is None:
        comment = get_removed_name(table, remove_model_only)

    return (
        f"ALTER TABLE {pg_identifier_preparer.quote(table)} RENAME TO {pg_identifier_preparer.quote(removed_name)};\n\n"
        f"{add_table_comment(table=removed_name, comment=comment)}"
    )
