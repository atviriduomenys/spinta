from abc import ABC, abstractmethod
from sqlalchemy.dialects.postgresql import UUID
from typing import List, Tuple, Dict

import sqlalchemy as sa
from alembic.operations import Operations


class MigrationAction(ABC):
    @abstractmethod
    def execute(self, op: Operations):
        pass


class CreateTableMigrationAction(MigrationAction):
    def __init__(self, table_name: str, columns: List[sa.Column]):
        self.table_name = table_name
        self.columns = columns

    def execute(self, op: Operations):
        op.create_table(self.table_name, *self.columns)


class DropTableMigrationAction(MigrationAction):
    def __init__(self, table_name: str):
        self.table_name = table_name

    def execute(self, op: Operations):
        op.drop_table(table_name=self.table_name)


class RenameTableMigrationAction(MigrationAction):
    def __init__(self, old_table_name: str, new_table_name: str):
        self.old_table_name = old_table_name
        self.new_table_name = new_table_name

    def execute(self, op: Operations):
        op.rename_table(old_table_name=self.old_table_name, new_table_name=self.new_table_name)


class AddColumnMigrationAction(MigrationAction):
    def __init__(self, table_name: str, column: sa.Column):
        self.table_name = table_name
        self.column = column

    def execute(self, op: Operations):
        op.add_column(table_name=self.table_name, column=self.column)


class DropColumnMigrationAction(MigrationAction):
    def __init__(self, table_name: str, column_name: str):
        self.table_name = table_name
        self.column_name = column_name

    def execute(self, op: Operations):
        op.drop_column(table_name=self.table_name, column_name=self.column_name)


class AlterColumnMigrationAction(MigrationAction):
    def __init__(self, table_name: str, column_name: str, nullable: bool = None, new_column_name: str = None,
                 type_=None, using: str = None):
        self.table_name = table_name
        self.column_name = column_name
        self.nullable = nullable
        self.new_column_name = new_column_name
        self.type_ = type_
        self.using = using

    def execute(self, op: Operations):
        op.alter_column(
            table_name=self.table_name,
            column_name=self.column_name,
            nullable=self.nullable,
            new_column_name=self.new_column_name,
            type_=self.type_,
            postgresql_using=self.using
        )


class DropConstraintMigrationAction(MigrationAction):
    def __init__(self, table_name: str, constraint_name: str):
        self.table_name = table_name
        self.constraint_name = constraint_name

    def execute(self, op: Operations):
        op.drop_constraint(constraint_name=self.constraint_name, table_name=self.table_name)


class DropIndexMigrationAction(MigrationAction):
    def __init__(self, table_name: str, index_name: str):
        self.table_name = table_name
        self.index_name = index_name

    def execute(self, op: Operations):
        op.drop_index(index_name=self.index_name, table_name=self.table_name)


class CreateUniqueConstraintMigrationAction(MigrationAction):
    def __init__(self, table_name: str, constraint_name: str, columns: List[str]):
        self.table_name = table_name
        self.constraint_name = constraint_name
        self.columns = columns

    def execute(self, op: Operations):
        op.create_unique_constraint(constraint_name=self.constraint_name, table_name=self.table_name,
                                    columns=self.columns)


class RenameConstraintMigrationAction(MigrationAction):
    def __init__(self, table_name: str, old_constraint_name: str, new_constraint_name: str):
        self.query = f'ALTER TABLE "{table_name}" RENAME CONSTRAINT "{old_constraint_name}" TO "{new_constraint_name}"'

    def execute(self, op: Operations):
        replaced = self.query.replace(":", "\\:")
        op.execute(replaced)


class CreatePrimaryKeyMigrationAction(MigrationAction):
    def __init__(self, table_name: str, constraint_name: str, columns: List[str]):
        self.table_name = table_name
        self.constraint_name = constraint_name
        self.columns = columns

    def execute(self, op: Operations):
        op.create_primary_key(constraint_name=self.constraint_name, table_name=self.table_name, columns=self.columns)


class CreateIndexMigrationAction(MigrationAction):
    def __init__(self, table_name: str, index_name: str, columns: List[str], using: str = None):
        self.table_name = table_name
        self.index_name = index_name
        self.columns = columns
        self.using = using

    def execute(self, op: Operations):
        op.create_index(index_name=self.index_name, table_name=self.table_name, columns=self.columns,
                        postgresql_using=self.using)


class RenameIndexMigrationAction(MigrationAction):
    def __init__(self, old_index_name: str, new_index_name: str):
        self.query = f'ALTER INDEX "{old_index_name}" RENAME TO "{new_index_name}"'

    def execute(self, op: Operations):
        replaced = self.query.replace(":", "\\:")
        op.execute(replaced)


class DowngradeTransferDataMigrationAction(MigrationAction):
    """Migration Action used to convert columns into external ref columns.

    Args:
        table_name: Name of currently migrating table
        referenced_table_name: Name of referenced table (ref.model)
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
                table_name="Country",
                referenced_table_name="City",
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
        table_name: str,
        referenced_table_name: str,
        source_column: sa.Column,
        columns: Dict[str, sa.Column],
        target: str
    ):
        original_table = sa.Table(
            table_name,
            sa.MetaData(),
            source_column._copy(),
            *[sa.Column(key, column.type) for key, column in columns.items()]
        )
        foreign_table = sa.Table(
            referenced_table_name,
            sa.MetaData(),
            sa.Column('_id', UUID),
            *[column._copy() for column in columns.values()]
        )
        self.query = original_table.update().values(
            **{
                key: foreign_table.columns[column.name]
                for key, column in columns.items()
            }
        ).where(
            original_table.columns[source_column.name] == foreign_table.columns[target]
        )

    def execute(self, op: Operations):
        op.execute(self.query)


class UpgradeTransferDataMigrationAction(MigrationAction):
    """Migration Action used to convert multiple columns into one internal ref column ("column._id").

    Args:
        table_name: Name of currently migrating table
        referenced_table_name: Name of referenced table (ref.model)
        ref_column: Newly created ref column, ending with "_id", ex: "column._id"
        columns: Dictionary of columns used to migrate, key is "referenced_table" column name. Value is migration table's column

    Example:
        Let's say you have two tables called "Country" and "City", where the "Country" table references the "City" table.
        In the current scenario, the reference level is 3, meaning there are no foreign keys and the reference is done using normal columns.

        The "City" table is referenced using these columns: "city.id", "city.code".

        To convert this reference to level 4 or higher, you can use:

            UpgradeTransferDataMigrationAction(
                table_name="Country",
                referenced_table_name="City",
                ref_column=Column("city._id"),
                columns={
                    "id": Column("city.id"),
                    "code": Column("city.code")
                }
            )
    """

    def __init__(
        self,
        table_name: str,
        referenced_table_name: str,
        ref_column: sa.Column,
        columns: Dict[str, sa.Column]
    ):
        original_table = sa.Table(
            table_name,
            sa.MetaData(),
            ref_column._copy(),
            *[column._copy() for column in columns.values()]
        )
        referenced_table = sa.Table(
            referenced_table_name,
            sa.MetaData(),
            sa.Column('_id', UUID),
            *[sa.Column(key, column.type) for key, column in columns.items()]
        )
        self.query = original_table.update().values(
            **{
                ref_column.name: referenced_table.columns['_id']
            }
        ).where(
            sa.and_(
                *[original_table.columns[column.name] == referenced_table.columns[key] for key, column in
                  columns.items()]
            )
        )

    def execute(self, op: Operations):
        op.execute(self.query)


class CreateForeignKeyMigrationAction(MigrationAction):
    def __init__(self, source_table: str, referent_table: str, constraint_name: str, local_cols: list,
                 remote_cols: list):
        self.constraint_name = constraint_name
        self.source_table = source_table
        self.referent_table = referent_table
        self.local_cols = local_cols
        self.remote_cols = remote_cols

    def execute(self, op: Operations):
        op.create_foreign_key(
            constraint_name=self.constraint_name,
            source_table=self.source_table,
            referent_table=self.referent_table,
            local_cols=self.local_cols,
            remote_cols=self.remote_cols
        )


class RenameSequenceMigrationAction(MigrationAction):
    def __init__(self, old_name: str, new_name: str):
        self.query = f'ALTER SEQUENCE "{old_name}" RENAME TO "{new_name}"'

    def execute(self, op: Operations):
        replaced = self.query.replace(":", "\\:")
        op.execute(replaced)


class TransferJSONDataMigrationAction(MigrationAction):
    def __init__(self, table: str, source: sa.Column, columns: List[Tuple[str, sa.Column]]):
        # Hack to transfer data if columns do not exist
        # Create new table with just required columns exist
        # SQLAlchemy does not allow the use table actions when those columns do not exist
        meta = sa.MetaData()
        temp_table = sa.Table(
            table,
            meta,
            source._copy(),
            *[column._copy() for _, column in columns]
        )
        self.query = temp_table.update().values(
            **{
                temp_table.columns[column.name].name: source[key].astext
                for key, column in columns
            }
        )

    def execute(self, op: Operations):
        op.execute(self.query)


class TransferColumnDataToJSONMigrationAction(MigrationAction):
    def __init__(self, table: str, source: sa.Column, columns: List[Tuple[str, sa.Column]]):
        # # Hack to transfer data if columns do not exist
        # # Create new table with just required columns exist
        # # SQLAlchemy does not allow the use table actions when those columns do not exist
        temp_table = sa.Table(
            table,
            sa.MetaData(),
            source._copy(),
            *[column._copy() for _, column in columns]
        )
        copied_source = temp_table.columns[source.name]
        results = []
        for key, value in columns:
            results.append(key)
            results.append(temp_table.columns[value.name])
        self.query = temp_table.update().values(
            **{
                copied_source.name: copied_source + sa.func.jsonb_build_object(
                    *results
                )
            }
        )

    def execute(self, op: Operations):
        op.execute(self.query)


class AddEmptyAttributeToJSONMigrationAction(MigrationAction):
    def __init__(self, table: str, source: sa.Column, key: str):
        temp_table = sa.Table(
            table,
            sa.MetaData(),
            source._copy(),
        )
        copied_source = temp_table.columns[source.name]
        self.query = temp_table.update().values(
            **{
                copied_source.name: copied_source + sa.func.jsonb_build_object(
                    key, None
                )
            }
        )

    def execute(self, op: Operations):
        op.execute(self.query)


class RenameJSONAttributeMigrationAction(MigrationAction):
    def __init__(self, table: str, source: sa.Column, old_key: str, new_key: str):
        temp_table = sa.Table(
            table,
            sa.MetaData(),
            source._copy(),
        )
        copied_source = temp_table.columns[source.name]
        self.query = temp_table.update().values(
            **{
                copied_source.name: copied_source - old_key + sa.func.jsonb_build_object(new_key,
                                                                                         copied_source[old_key])
            }
        ).where(copied_source.has_key(old_key))

    def execute(self, op: Operations):
        op.execute(self.query)


class RemoveJSONAttributeMigrationAction(MigrationAction):
    def __init__(self, table: str, source: sa.Column, key: str):
        temp_table = sa.Table(
            table,
            sa.MetaData(),
            source._copy(),
        )
        copied_source = temp_table.columns[source.name]
        self.query = temp_table.update().values(
            **{
                copied_source.name: copied_source - key
            }
        ).where(copied_source.has_key(key))

    def execute(self, op: Operations):
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

    def run_migrations(self, op: Operations):
        for migration in self.migrations:
            migration.execute(op)
        for migration in self.foreign_key_migration:
            migration.execute(op)
