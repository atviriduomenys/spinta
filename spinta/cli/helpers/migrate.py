import json
import os
from typing import TypedDict, Dict, Union, List, Callable

from spinta.exceptions import FileNotFound
from spinta.utils.nestedstruct import get_root_attr


class MigrateTableRename(TypedDict):
    name: str
    new_name: str
    columns: Dict[str, str]


class MigrateRename:
    tables: Dict[str, MigrateTableRename]

    def __init__(self, rename_src: Union[str, dict]):
        self.tables = {}
        self.parse_rename_src(rename_src)

    def insert_table(self, table_name: str):
        self.tables[table_name] = MigrateTableRename(
            name=table_name,
            new_name=table_name,
            columns={}
        )

    def insert_column(self, table_name: str, column_name: str, new_column_name: str):
        if table_name not in self.tables.keys():
            self.insert_table(table_name)
        if column_name == "":
            self.tables[table_name]["new_name"] = new_column_name
        else:
            self.tables[table_name]["columns"][column_name] = new_column_name

    def get_column_name(self, table_name: str, column_name: str, root_only: bool = False, root_value: str = ""):
        # If table does not have renamed, return given column
        if table_name not in self.tables:
            return column_name

        table = self.tables[table_name]
        columns = table["columns"]

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

    def get_old_column_name(self, table_name: str, column_name: str, root_only: bool = False, root_value: str = ""):
        if table_name not in self.tables:
            return column_name

        table = self.tables[table_name]
        given_name = get_root_attr(column_name, initial_root=root_value) if root_only else column_name
        for old_column_column, new_column_name in table["columns"].items():
            target_name = get_root_attr(new_column_name, initial_root=root_value) if root_only else new_column_name

            if target_name == given_name:
                old_name = get_root_attr(old_column_column, initial_root=root_value) if root_only else old_column_column
                return old_name
        return column_name

    def get_table_name(self, table_name: str):
        if table_name in self.tables.keys():
            return self.tables[table_name]["new_name"]
        return table_name

    def get_old_table_name(self, table_name: str):
        for key, data in self.tables.items():
            if data["new_name"] == table_name:
                return key
        return table_name

    def parse_rename_src(self, rename_src: Union[str, dict]):
        if rename_src:
            if isinstance(rename_src, str):
                if os.path.exists(rename_src):
                    with open(rename_src, 'r') as f:
                        data = json.loads(f.read())
                        for table, table_data in data.items():
                            self.insert_table(table)
                            for column, column_data in table_data.items():
                                self.insert_column(table, column, column_data)
                else:
                    raise FileNotFound(file=rename_src)
            else:
                for table, table_data in rename_src.items():
                    self.insert_table(table)
                    for column, column_data in table_data.items():
                        self.insert_column(table, column, column_data)


class MigrateMeta:
    plan: bool
    autocommit: bool
    rename: MigrateRename
    datasets: List[str]
    migration_extension: Callable

    def __init__(self, plan: bool, autocommit: bool, rename: MigrateRename, datasets: List[str] = None,
                 migration_extension: Callable = None):
        self.plan = plan
        self.rename = rename
        self.autocommit = autocommit
        self.datasets = datasets
        self.migration_extension = migration_extension
