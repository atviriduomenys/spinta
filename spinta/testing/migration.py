import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from spinta.backends.constants import TableType
from spinta.backends.helpers import TableIdentifier
from spinta.backends.postgresql.helpers import get_pg_sequence_name
from spinta.backends.postgresql.helpers.name import (
    PG_NAMING_CONVENTION,
    get_pg_removed_name,
    get_pg_index_name,
    name_changed,
)
from spinta.utils.sqlalchemy import Convention

pg_identifier_preparer = postgresql.dialect().identifier_preparer


def _get_escaped_qualified_name(schema: str | None, table: str) -> str:
    return (
        f"{pg_identifier_preparer.quote(schema)}.{pg_identifier_preparer.quote(table)}"
        if schema
        else pg_identifier_preparer.quote(table)
    )


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


# Index migration helpers
def add_index(table_identifier: TableIdentifier, index_name: str, columns: list[str]) -> str:
    column = ",".join(pg_identifier_preparer.quote(column) for column in columns)
    return f"CREATE INDEX {pg_identifier_preparer.quote(index_name)} ON {table_identifier.pg_escaped_qualified_name} ({column});\n\n"


def rename_index(table_identifier: TableIdentifier, old_index_name: str, new_index_name: str):
    qualified_index_name = (
        f"{pg_identifier_preparer.quote(table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(old_index_name)}"
        if table_identifier.pg_schema_name
        else pg_identifier_preparer.quote(old_index_name)
    )
    return f"ALTER INDEX {qualified_index_name} RENAME TO {pg_identifier_preparer.quote(new_index_name)};\n\n"


def drop_index(table_identifier: TableIdentifier, index_name: str):
    qualified_index_name = (
        f"{pg_identifier_preparer.quote(table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(index_name)}"
        if table_identifier.pg_schema_name
        else pg_identifier_preparer.quote(index_name)
    )
    return f"DROP INDEX {qualified_index_name};\n\n"


def change_index_schema(table_identifier: TableIdentifier, index_name: str, new_schema: str):
    qualified_index_name = (
        f"{pg_identifier_preparer.quote(table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(index_name)}"
        if table_identifier.pg_schema_name
        else pg_identifier_preparer.quote(index_name)
    )
    return f"ALTER INDEX {qualified_index_name} SET SCHEMA {pg_identifier_preparer.quote(new_schema)};\n\n"


# Constraint migration helpers
def rename_constraint(table_identifier: TableIdentifier, constraint_name: str, new_constraint_name: str):
    return (
        f"ALTER TABLE {table_identifier.pg_escaped_qualified_name} "
        f"RENAME CONSTRAINT {pg_identifier_preparer.quote(constraint_name)} TO {pg_identifier_preparer.quote(new_constraint_name)};\n\n"
    )


def drop_constraint(table_identifier: TableIdentifier, constraint_name: str):
    return f"ALTER TABLE {table_identifier.pg_escaped_qualified_name} DROP CONSTRAINT {pg_identifier_preparer.quote(constraint_name)};\n\n"


# Sequence migration helpers
def rename_sequence(table_identifier: TableIdentifier, sequence_name: str, new_sequence_name: str):
    qualified_sequence_name = (
        f"{pg_identifier_preparer.quote(table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(sequence_name)}"
        if table_identifier.pg_schema_name
        else pg_identifier_preparer.quote(sequence_name)
    )
    return f"ALTER SEQUENCE {qualified_sequence_name} RENAME TO {pg_identifier_preparer.quote(new_sequence_name)};\n\n"


def change_sequence_schema(table_identifier: TableIdentifier, sequence_name: str, new_schema: str):
    qualified_sequence_name = (
        f"{pg_identifier_preparer.quote(table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(sequence_name)}"
        if table_identifier.pg_schema_name
        else pg_identifier_preparer.quote(sequence_name)
    )
    return f"ALTER SEQUENCE {qualified_sequence_name} SET SCHEMA {pg_identifier_preparer.quote(new_schema)};\n\n"


# Column migration helpers
def add_column_comment(table_identifier: TableIdentifier, column: str, comment: str | None = None) -> str:
    if comment is None:
        comment = column
    escaped_column = pg_identifier_preparer.quote(column)
    return f"COMMENT ON COLUMN {table_identifier.pg_escaped_qualified_name}.{escaped_column} IS '{comment}';\n\n"


def add_column(table_identifier: TableIdentifier, column: str, column_type: str, comment: str = None):
    return (
        f"ALTER TABLE {table_identifier.pg_escaped_qualified_name} ADD COLUMN {pg_identifier_preparer.quote(column)} {column_type};\n"
        "\n"
        f"{add_column_comment(table_identifier=table_identifier, column=column, comment=comment)}"
    )


def rename_column(table_identifier: TableIdentifier, column: str, new_name: str, comment: str = None):
    if comment is None:
        comment = new_name
    return (
        f"{add_column_comment(table_identifier=table_identifier, column=column, comment=comment)}"
        f"ALTER TABLE {table_identifier.pg_escaped_qualified_name} RENAME "
        f"{pg_identifier_preparer.quote(column)} TO {pg_identifier_preparer.quote(new_name)};\n\n"
    )


def drop_column(table_identifier: TableIdentifier, column: str, comment: str = None):
    removed_name = get_pg_removed_name(column)
    if comment is None:
        comment = removed_name

    return (
        f"{add_column_comment(table_identifier=table_identifier, column=column, comment=comment)}"
        f"ALTER TABLE {table_identifier.pg_escaped_qualified_name} RENAME "
        f"{pg_identifier_preparer.quote(column)} TO {pg_identifier_preparer.quote(removed_name)};\n\n"
    )


# Schema migration helpers
def add_schema(schema: str):
    return f"CREATE SCHEMA IF NOT EXISTS {pg_identifier_preparer.quote(schema)};\n\n"


# Table migration helpers
def add_table_comment(table_identifier: TableIdentifier, comment: str) -> str:
    return f"COMMENT ON TABLE {table_identifier.pg_escaped_qualified_name} IS '{comment}';\n\n"


def add_changelog_table(table_identifier: TableIdentifier, comment: str = None) -> str:
    if table_identifier.table_type != TableType.CHANGELOG:
        table_identifier = table_identifier.change_table_type(new_type=TableType.CHANGELOG)

    if comment is None:
        comment = table_identifier.logical_qualified_name

    full_table_name = table_identifier.pg_table_name
    pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": full_table_name}
    index_name = PG_NAMING_CONVENTION[Convention.IX] % {"table_name": full_table_name, "column_0_N_name": "_txn"}

    return (
        f"CREATE TABLE {table_identifier.pg_escaped_qualified_name} (\n"
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
        f"{add_index(table_identifier=table_identifier, index_name=index_name, columns=['_txn'])}"
        f"{add_column_comment(table_identifier=table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_revision')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_txn')}"
        f"{add_column_comment(table_identifier=table_identifier, column='_rid')}"
        f"{add_column_comment(table_identifier=table_identifier, column='datetime')}"
        f"{add_column_comment(table_identifier=table_identifier, column='action')}"
        f"{add_column_comment(table_identifier=table_identifier, column='data')}"
        f"{add_table_comment(table_identifier=table_identifier, comment=comment)}"
    )


def add_redirect_table(table_identifier: TableIdentifier, comment: str = None) -> str:
    if table_identifier.table_type != TableType.REDIRECT:
        table_identifier = table_identifier.change_table_type(new_type=TableType.REDIRECT)

    if comment is None:
        comment = table_identifier.logical_qualified_name

    full_table_name = table_identifier.pg_table_name
    pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": full_table_name}
    index_name = PG_NAMING_CONVENTION[Convention.IX] % {"table_name": full_table_name, "column_0_N_name": "redirect"}

    return (
        f"CREATE TABLE {table_identifier.pg_escaped_qualified_name} (\n"
        "    _id UUID NOT NULL, \n"
        "    redirect UUID, \n"
        f'    CONSTRAINT "{pk_constraint}" PRIMARY KEY (_id)\n'
        ");\n"
        "\n"
        f"{add_index(table_identifier=table_identifier, index_name=index_name, columns=['redirect'])}"
        f"{add_column_comment(table_identifier=table_identifier, column='_id')}"
        f"{add_column_comment(table_identifier=table_identifier, column='redirect')}"
        f"{add_table_comment(table_identifier=table_identifier, comment=comment)}"
    )


def rename_table(
    old_table_identifier: TableIdentifier,
    new_table_identifier: TableIdentifier,
    comment: str = None,
    rename_pk_constraint: bool = True,
):
    if comment is None:
        comment = new_table_identifier.logical_qualified_name

    query = ""
    if name_changed(old_table_identifier.pg_schema_name, new_table_identifier.pg_schema_name):
        query = f"ALTER TABLE {old_table_identifier.pg_escaped_qualified_name} SET SCHEMA {pg_identifier_preparer.quote(new_table_identifier.pg_schema_name)};\n\n"
        old_table_identifier = TableIdentifier(
            schema=new_table_identifier.pg_schema_name,
            base_name=old_table_identifier.base_name,
            table_type=old_table_identifier.table_type,
            table_arg=old_table_identifier.table_arg,
        )

    query += (
        f"ALTER TABLE {old_table_identifier.pg_escaped_qualified_name} RENAME TO {pg_identifier_preparer.quote(new_table_identifier.pg_table_name)};\n\n"
        f"{add_table_comment(table_identifier=new_table_identifier, comment=comment)}"
    )

    if rename_pk_constraint:
        old_pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": old_table_identifier.pg_table_name}
        new_pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": new_table_identifier.pg_table_name}
        query += f"{rename_constraint(table_identifier=new_table_identifier, constraint_name=old_pk_constraint, new_constraint_name=new_pk_constraint)}"
    return query


def rename_changelog(old_table_identifier: TableIdentifier, new_table_identifier: TableIdentifier, comment: str = None):
    if old_table_identifier.table_type != TableType.CHANGELOG:
        old_table_identifier = old_table_identifier.change_table_type(new_type=TableType.CHANGELOG)

    if new_table_identifier.table_type != TableType.CHANGELOG:
        new_table_identifier = new_table_identifier.change_table_type(new_type=TableType.CHANGELOG)

    old_sequence_name = get_pg_sequence_name(name=old_table_identifier.pg_table_name)
    new_sequence_name = get_pg_sequence_name(name=new_table_identifier.pg_table_name)

    old_txn_index_name = get_pg_index_name(old_table_identifier.pg_table_name, "_txn")
    new_txn_index_name = get_pg_index_name(new_table_identifier.pg_table_name, "_txn")

    sequence_rename = ""
    index_rename = ""
    sequence_rename += rename_sequence(
        table_identifier=new_table_identifier, sequence_name=old_sequence_name, new_sequence_name=new_sequence_name
    )
    index_rename += rename_index(
        table_identifier=new_table_identifier, old_index_name=old_txn_index_name, new_index_name=new_txn_index_name
    )

    old_pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": old_table_identifier.pg_table_name}
    new_pk_constraint = PG_NAMING_CONVENTION[Convention.PK] % {"table_name": new_table_identifier.pg_table_name}

    return (
        f"{rename_table(old_table_identifier=old_table_identifier, new_table_identifier=new_table_identifier, comment=comment, rename_pk_constraint=False)}"
        f"{sequence_rename}"
        f"{rename_constraint(table_identifier=new_table_identifier, constraint_name=old_pk_constraint, new_constraint_name=new_pk_constraint)}"
        f"{index_rename}"
    )


def rename_redirect(old_table_identifier: TableIdentifier, new_table_identifier: TableIdentifier, comment: str = None):
    if old_table_identifier.table_type != TableType.REDIRECT:
        old_table_identifier = old_table_identifier.change_table_type(new_type=TableType.REDIRECT)

    if new_table_identifier.table_type != TableType.REDIRECT:
        new_table_identifier = new_table_identifier.change_table_type(new_type=TableType.REDIRECT)

    old_redirect_index_name = get_pg_index_name(old_table_identifier.pg_table_name, "redirect")
    new_redirect_index_name = get_pg_index_name(new_table_identifier.pg_table_name, "redirect")

    index_rename = ""
    index_rename += rename_index(
        table_identifier=new_table_identifier,
        old_index_name=old_redirect_index_name,
        new_index_name=new_redirect_index_name,
    )

    return (
        f"{rename_table(old_table_identifier=old_table_identifier, new_table_identifier=new_table_identifier, comment=comment)}"
        f"{index_rename}"
    )


def drop_table(table_identifier: TableIdentifier, remove_model_only: bool = False, comment: str = None):
    removed_table_identifier = table_identifier.apply_removed_prefix(remove_model_only=remove_model_only)
    if comment is None:
        comment = removed_table_identifier.logical_qualified_name

    return rename_table(
        old_table_identifier=table_identifier,
        new_table_identifier=removed_table_identifier,
        comment=comment,
        rename_pk_constraint=False,
    )
