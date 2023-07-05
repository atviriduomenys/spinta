from abc import ABC, abstractmethod
from typing import List

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
                 type_=None):
        self.table_name = table_name
        self.column_name = column_name
        self.nullable = nullable
        self.new_column_name = new_column_name
        self.type_ = type_

    def execute(self, op: Operations):
        op.alter_column(
            table_name=self.table_name,
            column_name=self.column_name,
            nullable=self.nullable,
            new_column_name=self.new_column_name,
            type_=self.type_
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
        op.create_unique_constraint(constraint_name=self.constraint_name, table_name=self.table_name, columns=self.columns)


class CreatePrimaryKeyMigrationAction(MigrationAction):
    def __init__(self, table_name: str, constraint_name: str, columns: List[str]):
        self.table_name = table_name
        self.constraint_name = constraint_name
        self.columns = columns

    def execute(self, op: Operations):
        op.create_primary_key(constraint_name=self.constraint_name, table_name=self.table_name, columns=self.columns)


class CreateIndexMigrationAction(MigrationAction):
    def __init__(self, table_name: str, index_name: str, columns: List[str]):
        self.table_name = table_name
        self.index_name = index_name
        self.columns = columns

    def execute(self, op: Operations):
        op.create_index(index_name=self.index_name, table_name=self.table_name, columns=self.columns)


class DowngradeTransferDataMigrationAction(MigrationAction):
    def __init__(self, table_name: str, foreign_table_name: str, columns: List[str]):
        set_string = ""
        for column in columns:
            set_string += f'\n"{column}"={column}'

        self.query = f'''
        UPDATE "{table_name}"
        SET {set_string}
        FROM "{foreign_table_name}"
        WHERE "{table_name}._id" = "{foreign_table_name}._id"
        '''

    def execute(self, op: Operations):
        op.execute(self.query)


class UpgradeTransferDataMigrationAction(MigrationAction):
    def __init__(self, table_name: str, foreign_table_name: str, columns: List[str]):
        where_string = []
        for column in columns:
            where_string.append(f'\n"{column}"={column}')

        self.query = f'''
        UPDATE "{table_name}"
        SET "{table_name}._id"="{foreign_table_name}._id"
        FROM "{foreign_table_name}"
        WHERE {'AND'.join(where_string)}
        '''

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

    def run_migrations(self, op: Operations):
        for migration in self.migrations:
            migration.execute(op)
        for migration in self.foreign_key_migration:
            migration.execute(op)
