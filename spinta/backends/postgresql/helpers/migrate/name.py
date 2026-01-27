from __future__ import annotations

import dataclasses
import json
import os
from typing import Dict

import sqlalchemy as sa
from multipledispatch import dispatch

from spinta.backends.helpers import TableIdentifier, split_logical_name
from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.helpers import get_column_name
from spinta.components import Model, Property
from spinta.exceptions import FileNotFound
from spinta.utils.nestedstruct import get_root_attr


def _get_table_identifier(table: str | sa.Table | Model | TableIdentifier):
    if isinstance(table, TableIdentifier):
        return table
    return get_table_identifier(table, default_pg_schema="public")


class RenameMap:
    @dataclasses.dataclass
    class _Name:
        normal: str
        compressed: str

    @dataclasses.dataclass
    class _TableRename:
        old: TableIdentifier
        new: TableIdentifier | None
        columns: Dict[str, str]

        def get_new_table_identifier(self, fallback: bool = False) -> TableIdentifier | None:
            if self.new is None:
                if fallback:
                    return self.get_old_table_identifier()

                return None

            return self.new

        def get_old_table_identifier(self) -> TableIdentifier:
            return self.old

    tables: Dict[str, _TableRename]

    def __init__(self, rename_src: str | dict):
        self.tables = {}
        self.parse_rename_src(rename_src)

    def _find_new_table(self, name: str) -> _TableRename | None:
        if name in self.tables:
            return self.tables[name]

        for table in self.tables.values():
            table_name = table.old.logical_qualified_name
            if table_name == name:
                return table

        return None

    def _find_old_table(self, name: str) -> _TableRename | None:
        for table in self.tables.values():
            if table.new is None:
                continue

            table_name = table.new.logical_qualified_name
            if table_name == name:
                return table

        return None

    def insert_table(self, old_name: str, new_name: str | None = None):
        old_namespace, old_model, old_table_type, old_table_arg = split_logical_name(old_name)
        new_namespace = new_model = new_table_type = new_table_arg = None
        if new_name:
            new_namespace, new_model, new_table_type, new_table_arg = split_logical_name(new_name)

        self.tables[old_name] = self._TableRename(
            old=TableIdentifier(
                schema=old_namespace, base_name=old_model, table_type=old_table_type, table_arg=old_table_arg
            ),
            new=TableIdentifier(
                schema=new_namespace, base_name=new_model, table_type=new_table_type, table_arg=new_table_arg
            )
            if new_name
            else None,
            columns={},
        )

    def insert_column(self, table_name: str, column_name: str, new_column_name: str):
        if table_name not in self.tables.keys():
            self.insert_table(table_name)
        if column_name == "":
            self.tables[table_name].new = get_table_identifier(table_name)
            return

        self.tables[table_name].columns[column_name] = new_column_name

    def to_new_column_name(
        self,
        table_name: str | sa.Table | Model | TableIdentifier,
        column_name: str,
        root_only: bool = False,
        root_value: str = "",
    ):
        # If table does not have renamed, return given column
        table_identifier = _get_table_identifier(table_name)
        table = self._find_new_table(table_identifier.logical_qualified_name)
        if table is None:
            return column_name

        columns = table.columns

        if column_name in columns:
            return columns[column_name]

        # If column was not directly set, and it cannot be mapped through root node, return it
        if not root_only:
            return column_name

        root_attr = get_root_attr(column_name, initial_root=root_value)
        for old_column_name, new_column_name in columns.items():
            target_root_attr = get_root_attr(old_column_name, initial_root=root_value)
            if root_attr == target_root_attr:
                new_name = get_root_attr(new_column_name, initial_root=root_value)
                return new_name
        return column_name

    def to_old_column_name(
        self,
        table_name: str | sa.Table | Model | TableIdentifier,
        column_name: str,
        root_only: bool = False,
        root_value: str = "",
    ):
        table_identifier = _get_table_identifier(table_name)
        table = self._find_new_table(table_identifier.logical_qualified_name)
        if table is None:
            return column_name

        given_name = get_root_attr(column_name, initial_root=root_value) if root_only else column_name
        for old_column_column, new_column_name in table.columns.items():
            target_name = get_root_attr(new_column_name, initial_root=root_value) if root_only else new_column_name

            if target_name == given_name:
                old_name = get_root_attr(old_column_column, initial_root=root_value) if root_only else old_column_column
                return old_name
        return column_name

    def to_new_table(self, table_name: str | sa.Table | Model | TableIdentifier) -> TableIdentifier:
        table_identifier = _get_table_identifier(table_name)
        table = self._find_new_table(table_identifier.logical_qualified_name)
        if table is None:
            return table_identifier

        new_identifier = table.get_new_table_identifier()
        if new_identifier is not None:
            table_identifier = new_identifier

        return table_identifier

    def to_old_table(self, table_name: str | sa.Table | Model) -> TableIdentifier:
        table_identifier = _get_table_identifier(table_name)
        table = self._find_old_table(table_identifier.logical_qualified_name)
        if table is None:
            return table_identifier

        return table.get_old_table_identifier()

    def parse_rename_src(self, rename_src: str | dict):
        def _parse_dict(src: dict):
            for table, table_data in src.items():
                table_rename = table_data.pop("", None)
                self.insert_table(table, table_rename)
                for column, column_data in table_data.items():
                    self.insert_column(table, column, column_data)

        if rename_src:
            if isinstance(rename_src, str):
                if os.path.exists(rename_src):
                    with open(rename_src, "r") as f:
                        data = json.loads(f.read())
                        _parse_dict(data)
                else:
                    raise FileNotFound(file=rename_src)
            else:
                _parse_dict(rename_src)


@dispatch(str)
def get_full_name(item: str) -> str:
    return item


@dispatch(sa.Table)
def get_full_name(item: sa.Table) -> str:
    if item.comment:
        return item.comment
    return item.name


@dispatch(sa.Column)
def get_full_name(item: sa.Column) -> str:
    if item.comment:
        return item.comment
    return item.name


@dispatch(Model)
def get_full_name(item: Model) -> str:
    return get_table_identifier(item).logical_qualified_name


@dispatch(Property)
def get_full_name(item: Property) -> str:
    return get_column_name(item)
