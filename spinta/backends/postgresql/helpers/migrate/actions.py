from abc import ABC, abstractmethod
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects import postgresql
from typing import List, Tuple, Dict, Union

import sqlalchemy as sa
from typing import TYPE_CHECKING

from spinta.backends.helpers import TableIdentifier
from spinta.backends.postgresql.helpers.name import name_changed

if TYPE_CHECKING:
    from alembic.operations import Operations

pg_identifier_preparer = postgresql.dialect().identifier_preparer


class MigrationAction(ABC):
    @abstractmethod
    def execute(self, op: "Operations"):
        pass


class CreateTableMigrationAction(MigrationAction):
    def __init__(
        self, table_identifier: TableIdentifier, columns: List[sa.Column], comment: str, indexes: List[sa.Index] = None
    ):
        self.table_identifier = table_identifier
        self.columns = columns
        self.comment = comment
        self.indexes = indexes

    def execute(self, op: "Operations"):
        table_identifier = self.table_identifier
        table_name = table_identifier.pg_table_name
        schema = table_identifier.pg_schema_name
        op.create_table(table_name, *self.columns, schema=schema)
        op.create_table_comment(table_name=table_name, comment=self.comment, schema=schema)

        if self.indexes:
            for index in self.indexes:
                op.create_index(
                    index_name=index.name,
                    table_name=table_name,
                    columns=index.columns,
                    unique=index.unique,
                    schema=schema,
                    **index.kwargs,
                )


class DropTableMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier):
        self.table_identifier = table_identifier

    def execute(self, op: "Operations"):
        op.drop_table(table_name=self.table_identifier.pg_table_name, schema=self.table_identifier.pg_schema_name)


class RenameTableMigrationAction(MigrationAction):
    def __init__(
        self,
        old_table_identifier: TableIdentifier,
        new_table_identifier: TableIdentifier,
        comment: Union[str, bool] = False,
    ):
        self.old_table_identifier = old_table_identifier
        self.new_table_identifier = new_table_identifier
        self.comment = comment

    def execute(self, op: "Operations"):
        if name_changed(self.old_table_identifier.pg_schema_name, self.new_table_identifier.pg_schema_name):
            schema_name = self.new_table_identifier.pg_schema_name
            op.execute(
                f"ALTER TABLE {self.old_table_identifier.pg_escaped_qualified_name} SET SCHEMA {pg_identifier_preparer.quote(schema_name)}"
            )

        if name_changed(self.old_table_identifier.pg_table_name, self.new_table_identifier.pg_table_name):
            op.rename_table(
                old_table_name=self.old_table_identifier.pg_table_name,
                new_table_name=self.new_table_identifier.pg_table_name,
                schema=self.new_table_identifier.pg_schema_name,
            )
        if self.comment is not False:
            op.create_table_comment(
                table_name=self.new_table_identifier.pg_table_name,
                comment=self.comment,
                schema=self.new_table_identifier.pg_schema_name,
            )


class AddColumnMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, column: sa.Column):
        self.table_identifier = table_identifier
        self.column = column

    def execute(self, op: "Operations"):
        op.add_column(
            table_name=self.table_identifier.pg_table_name,
            column=self.column,
            schema=self.table_identifier.pg_schema_name,
        )


class DropColumnMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, column_name: str):
        self.table_identifier = table_identifier
        self.column_name = column_name

    def execute(self, op: "Operations"):
        op.drop_column(
            table_name=self.table_identifier.pg_table_name,
            column_name=self.column_name,
            schema=self.table_identifier.pg_schema_name,
        )


class AlterColumnMigrationAction(MigrationAction):
    def __init__(
        self,
        table_identifier: TableIdentifier,
        column_name: str,
        nullable: bool = None,
        new_column_name: str = None,
        type_=None,
        using: str = None,
        comment: str = False,
    ):
        self.table_identifier = table_identifier
        self.column_name = column_name
        self.nullable = nullable
        self.new_column_name = new_column_name
        self.type_ = type_
        self.using = using
        self.comment = comment

    def execute(self, op: "Operations"):
        op.alter_column(
            table_name=self.table_identifier.pg_table_name,
            column_name=self.column_name,
            nullable=self.nullable,
            new_column_name=self.new_column_name,
            type_=self.type_,
            postgresql_using=self.using,
            comment=self.comment,
            schema=self.table_identifier.pg_schema_name,
        )


class DropConstraintMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, constraint_name: str):
        self.table_identifier = table_identifier
        self.constraint_name = constraint_name

    def execute(self, op: "Operations"):
        op.drop_constraint(
            constraint_name=self.constraint_name,
            table_name=self.table_identifier.pg_table_name,
            schema=self.table_identifier.pg_schema_name,
        )


class DropIndexMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, index_name: str):
        self.table_identifier = table_identifier
        self.index_name = index_name

    def execute(self, op: "Operations"):
        op.drop_index(
            index_name=self.index_name,
            table_name=self.table_identifier.pg_table_name,
            schema=self.table_identifier.pg_schema_name,
        )


class CreateUniqueConstraintMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, constraint_name: str, columns: List[str]):
        self.table_identifier = table_identifier
        self.constraint_name = constraint_name
        self.columns = columns

    def execute(self, op: "Operations"):
        op.create_unique_constraint(
            constraint_name=self.constraint_name,
            table_name=self.table_identifier.pg_table_name,
            columns=self.columns,
            schema=self.table_identifier.pg_schema_name,
        )


class RenameConstraintMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, old_constraint_name: str, new_constraint_name: str):
        self.query = (
            f"ALTER TABLE {table_identifier.pg_escaped_qualified_name} RENAME CONSTRAINT "
            f"{pg_identifier_preparer.quote(old_constraint_name)} "
            f"TO {pg_identifier_preparer.quote(new_constraint_name)}"
        )

    def execute(self, op: "Operations"):
        replaced = self.query.replace(":", "\\:")
        op.execute(replaced)


class CreateIndexMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, index_name: str, columns: List[str], using: str = None):
        self.table_identifier = table_identifier
        self.index_name = index_name
        self.columns = columns
        self.using = using

    def execute(self, op: "Operations"):
        op.create_index(
            index_name=self.index_name,
            table_name=self.table_identifier.pg_table_name,
            columns=self.columns,
            postgresql_using=self.using,
            schema=self.table_identifier.pg_schema_name,
        )


class RenameIndexMigrationAction(MigrationAction):
    def __init__(
        self,
        old_index_name: str,
        new_index_name: str,
        table_identifier: TableIdentifier,
        old_table_identifier: TableIdentifier = None,
    ):
        self.table_identifier = table_identifier
        self.old_table_identifier = old_table_identifier

        target_index = (
            f"{pg_identifier_preparer.quote(table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(old_index_name)}"
            if table_identifier.pg_schema_name
            else pg_identifier_preparer.quote(old_index_name)
        )

        self.query = f"ALTER INDEX {target_index} RENAME TO {pg_identifier_preparer.quote(new_index_name)}"
        self.schema_query = None
        if old_table_identifier:
            target_index = (
                f"{pg_identifier_preparer.quote(old_table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(old_index_name)}"
                if old_table_identifier.pg_schema_name
                else pg_identifier_preparer.quote(old_index_name)
            )
            self.schema_query = (
                f"ALTER INDEX {target_index} SET SCHEMA {pg_identifier_preparer.quote(table_identifier.pg_schema_name)}"
            )

    def execute(self, op: "Operations"):
        if (
            self.schema_query
            and self.old_table_identifier
            and name_changed(self.old_table_identifier.pg_schema_name, self.table_identifier.pg_schema_name)
        ):
            replaced = self.schema_query.replace(":", "\\:")
            op.execute(replaced)

        replaced = self.query.replace(":", "\\:")
        op.execute(replaced)


class DowngradeTransferDataMigrationAction(MigrationAction):
    """Migration Action used to convert columns into external ref columns.

    Args:
        table_identifier: Identifier of currently migrating table
        referenced_table_identifier: Identifier of referenced table (ref.model)
        source_column: Original column which is being converted to external ref (currently system only allows conversion from one column)
        columns: Mapping of new ref column names and referenced table's columns.
        target: Name of foreign table's mapping column

    Example:
        Let's say you have two tables called "Country" and "City", where the "Country" table references the "City" table.
        In the current scenario, the reference level is 4, meaning there is foreign key constraint, and it's mapped through "_id" column.

        The "City" table is referenced using these columns: "city._id".
        It's primary key consist of "id" and "code" columns.

        To convert this reference to level 3 or lower, you can use:

            DowngradeTransferDataMigrationAction(
                table_identifier=TableIdentifier("Country"),
                referenced_table_identifier=TableIdentifier("City"),
                ref_column=Column("city._id", "Country"),
                columns={
                    "city.id": Column("id", "City"),
                    "city.code": Column("code", "City")
                },
                target="_id"
            )
    """

    def __init__(
        self,
        table_identifier: TableIdentifier,
        referenced_table_identifier: TableIdentifier,
        source_column: sa.Column,
        columns: Dict[str, sa.Column],
        target: str,
    ):
        original_table = sa.Table(
            table_identifier.pg_table_name,
            sa.MetaData(),
            source_column._copy(),
            *[sa.Column(key, column.type) for key, column in columns.items()],
            schema=table_identifier.pg_schema_name,
        )
        foreign_table = sa.Table(
            referenced_table_identifier.pg_table_name,
            sa.MetaData(),
            sa.Column("_id", UUID),
            *[column._copy() for column in columns.values()],
            schema=referenced_table_identifier.pg_schema_name,
        )
        if table_identifier.pg_qualified_name == referenced_table_identifier.pg_qualified_name:
            foreign_table = foreign_table.alias()
        self.query = (
            original_table.update()
            .values(**{key: foreign_table.columns[column.name] for key, column in columns.items()})
            .where(original_table.columns[source_column.name] == foreign_table.columns[target])
        )

    def execute(self, op: "Operations"):
        op.execute(self.query)


class UpgradeTransferDataMigrationAction(MigrationAction):
    """Migration Action used to convert multiple columns into one internal ref column ("column._id").

    Args:
        table_identifier: Identifier of currently migrating table
        referenced_table_identifier: Identifier of referenced table (ref.model)
        ref_column: Newly created ref column, ending with "_id", ex: "column._id"
        columns: Dictionary of columns used to migrate, key is "referenced_table" column name. Value is migration table's column

    Example:
        Let's say you have two tables called "Country" and "City", where the "Country" table references the "City" table.
        In the current scenario, the reference level is 3, meaning there are no foreign keys and the reference is done using normal columns.

        The "City" table is referenced using these columns: "city.id", "city.code".

        To convert this reference to level 4 or higher, you can use:

            UpgradeTransferDataMigrationAction(
                table_identifier=TableIdentifier("Country"),
                referenced_table_identifier=TableIdentifier("City"),
                ref_column=Column("city._id"),
                columns={
                    "id": Column("city.id"),
                    "code": Column("city.code")
                }
            )
    """

    def __init__(
        self,
        table_identifier: TableIdentifier,
        referenced_table_identifier: TableIdentifier,
        ref_column: sa.Column,
        columns: Dict[str, sa.Column],
    ):
        original_table = sa.Table(
            table_identifier.pg_table_name,
            sa.MetaData(),
            ref_column._copy(),
            *[column._copy() for column in columns.values()],
            schema=table_identifier.pg_schema_name,
        )
        referenced_table = sa.Table(
            referenced_table_identifier.pg_table_name,
            sa.MetaData(),
            sa.Column("_id", UUID),
            *[sa.Column(key, column.type) for key, column in columns.items()],
            schema=referenced_table_identifier.pg_schema_name,
        )
        if table_identifier.pg_qualified_name == referenced_table_identifier.pg_qualified_name:
            referenced_table = referenced_table.alias()
        self.query = (
            original_table.update()
            .values(**{ref_column.name: referenced_table.columns["_id"]})
            .where(
                sa.and_(
                    *[
                        original_table.columns[column.name] == referenced_table.columns[key]
                        for key, column in columns.items()
                    ]
                )
            )
        )

    def execute(self, op: "Operations"):
        op.execute(self.query)


class CreateForeignKeyMigrationAction(MigrationAction):
    def __init__(
        self,
        source_table_identifier: TableIdentifier,
        referent_table_identifier: TableIdentifier,
        constraint_name: str,
        local_cols: list,
        remote_cols: list,
    ):
        self.constraint_name = constraint_name
        self.source_table_identifier = source_table_identifier
        self.referent_table_identifier = referent_table_identifier
        self.local_cols = local_cols
        self.remote_cols = remote_cols

    def execute(self, op: "Operations"):
        op.create_foreign_key(
            constraint_name=self.constraint_name,
            source_table=self.source_table_identifier.pg_table_name,
            source_schema=self.source_table_identifier.pg_schema_name,
            referent_table=self.referent_table_identifier.pg_table_name,
            referent_schema=self.referent_table_identifier.pg_schema_name,
            local_cols=self.local_cols,
            remote_cols=self.remote_cols,
        )


class RenameSequenceMigrationAction(MigrationAction):
    def __init__(
        self,
        old_name: str,
        new_name: str,
        table_identifier: TableIdentifier,
        old_table_identifier: TableIdentifier = None,
    ):
        self.table_identifier = table_identifier
        self.old_table_identifier = old_table_identifier
        target_sequence = (
            f"{pg_identifier_preparer.quote(table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(old_name)}"
            if table_identifier.pg_schema_name
            else pg_identifier_preparer.quote(old_name)
        )

        self.query = f"ALTER SEQUENCE {target_sequence} RENAME TO {pg_identifier_preparer.quote(new_name)}"
        self.schema_query = None

        if old_table_identifier:
            target_sequence = (
                f"{pg_identifier_preparer.quote(old_table_identifier.pg_schema_name)}.{pg_identifier_preparer.quote(old_name)}"
                if old_table_identifier.pg_schema_name
                else pg_identifier_preparer.quote(old_name)
            )
            self.schema_query = f"ALTER SEQUENCE {target_sequence} SET SCHEMA {pg_identifier_preparer.quote(table_identifier.pg_schema_name)}"

    def execute(self, op: "Operations"):
        if (
            self.schema_query
            and self.old_table_identifier
            and name_changed(self.old_table_identifier.pg_schema_name, self.table_identifier.pg_schema_name)
        ):
            replaced = self.schema_query.replace(":", "\\:")
            op.execute(replaced)

        replaced = self.query.replace(":", "\\:")
        op.execute(replaced)


class TransferJSONDataMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, source: sa.Column, columns: List[Tuple[str, sa.Column]]):
        # Hack to transfer data if columns do not exist
        # Create new table with just required columns exist
        # SQLAlchemy does not allow the use table actions when those columns do not exist
        meta = sa.MetaData()
        temp_table = sa.Table(
            table_identifier.pg_table_name,
            meta,
            source._copy(),
            *[column._copy() for _, column in columns],
            schema=table_identifier.pg_schema_name,
        )
        self.query = temp_table.update().values(
            **{temp_table.columns[column.name].name: source[key].astext for key, column in columns}
        )

    def execute(self, op: "Operations"):
        op.execute(self.query)


class TransferColumnDataToJSONMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, source: sa.Column, columns: List[Tuple[str, sa.Column]]):
        # # Hack to transfer data if columns do not exist
        # # Create new table with just required columns exist
        # # SQLAlchemy does not allow the use table actions when those columns do not exist
        temp_table = sa.Table(
            table_identifier.pg_table_name,
            sa.MetaData(),
            source._copy(),
            *[column._copy() for _, column in columns],
            schema=table_identifier.pg_schema_name,
        )
        copied_source = temp_table.columns[source.name]
        results = []
        for key, value in columns:
            results.append(key)
            results.append(temp_table.columns[value.name])
        self.query = temp_table.update().values(
            **{copied_source.name: copied_source + sa.func.jsonb_build_object(*results)}
        )

    def execute(self, op: "Operations"):
        op.execute(self.query)


class AddEmptyAttributeToJSONMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, source: sa.Column, key: str):
        temp_table = sa.Table(
            table_identifier.pg_table_name,
            sa.MetaData(),
            source._copy(),
            schema=table_identifier.pg_schema_name,
        )
        copied_source = temp_table.columns[source.name]
        self.query = temp_table.update().values(
            **{copied_source.name: copied_source + sa.func.jsonb_build_object(key, None)}
        )

    def execute(self, op: "Operations"):
        op.execute(self.query)


class RenameJSONAttributeMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, source: sa.Column, old_key: str, new_key: str):
        temp_table = sa.Table(
            table_identifier.pg_table_name,
            sa.MetaData(),
            source._copy(),
            schema=table_identifier.pg_schema_name,
        )
        copied_source = temp_table.columns[source.name]
        self.query = (
            temp_table.update()
            .values(
                **{
                    copied_source.name: copied_source
                    - old_key
                    + sa.func.jsonb_build_object(new_key, copied_source[old_key])
                }
            )
            .where(copied_source.has_key(old_key))
        )

    def execute(self, op: "Operations"):
        op.execute(self.query)


class RemoveJSONAttributeMigrationAction(MigrationAction):
    def __init__(self, table_identifier: TableIdentifier, source: sa.Column, key: str):
        temp_table = sa.Table(
            table_identifier.pg_table_name,
            sa.MetaData(),
            source._copy(),
            schema=table_identifier.pg_schema_name,
        )
        copied_source = temp_table.columns[source.name]
        self.query = (
            temp_table.update().values(**{copied_source.name: copied_source - key}).where(copied_source.has_key(key))
        )

    def execute(self, op: "Operations"):
        op.execute(self.query)


class CreateSchemaMigrationAction(MigrationAction):
    def __init__(self, schema_name: str):
        self.schema_name = schema_name
        self.query = f"CREATE SCHEMA IF NOT EXISTS {pg_identifier_preparer.quote(schema_name)}"

    def execute(self, op: "Operations"):
        op.execute(self.query)


class MigrationHandler:
    def __init__(self):
        self.migrations: List[MigrationAction] = []
        self.foreign_key_migration: List[MigrationAction] = []

    def add_action(self, action: MigrationAction, foreign_key: bool = False):
        if foreign_key:
            self.foreign_key_migration.append(action)
        else:
            self.migrations.append(action)

        return self

    def has_constraint_been_dropped(self, constraint_name: str):
        for migration in self.migrations:
            if isinstance(migration, DropConstraintMigrationAction):
                if migration.constraint_name == constraint_name:
                    return True
        for migration in self.foreign_key_migration:
            if isinstance(migration, DropConstraintMigrationAction):
                if migration.constraint_name == constraint_name:
                    return True
        return False

    def run_migrations(self, op: "Operations"):
        for migration in self.migrations:
            migration.execute(op)
        for migration in self.foreign_key_migration:
            migration.execute(op)
