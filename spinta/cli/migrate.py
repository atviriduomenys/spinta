import asyncio
import json
import os
from typing import List, TypedDict, Dict, Union, Callable
from typing import Optional
from typer import Option

import click
from typer import Argument
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.store import load_store
from spinta.cli.helpers.store import prepare_manifest
from spinta.core.context import configure_context
from spinta.exceptions import FileNotFound
from spinta.utils.nestedstruct import get_root_attr


def bootstrap(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
):
    """Initialize backends

    This will create tables and sync manifest to backends.
    """
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests)
    store = prepare_manifest(context, ensure_config_dir=True, full_load=True)

    with context:
        require_auth(context)
        commands.bootstrap(context, store.manifest)


def sync(ctx: TyperContext):
    """Sync source manifests into main manifest

    Single main manifest can be populated from multiple different backends.
    """
    context = ctx.obj
    store = load_store(context)
    commands.load(context, store.internal, into=store.manifest)
    with context:
        require_auth(context)
        coro = commands.sync(context, store.manifest)
        asyncio.run(coro)
    click.echo("Done.")


def migrate(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Manifest files to load"
    )),
    plan: bool = Option(False, '-p', '--plan', help=(
        "If added, prints SQL code instead of executing it"
    ), is_flag=True),
    rename: str = Option(None, '-r', '--rename', help=(
        "JSON file, that maps manifest node renaming (models, properties)"
    )),
    autocommit: bool = Option(False, '-a', '--autocommit', help=(
        "If added, migrate will do atomic transactions, meaning it will automatically commit after each action (use it at your own risk)"
    ))
):
    """Migrate schema change to backends"""
    manifests = convert_str_to_manifest_path(manifests)
    context = configure_context(ctx.obj, manifests)
    store = prepare_manifest(context, ensure_config_dir=True)
    manifest = store.manifest
    migrate_meta = MigrateMeta(
        plan=plan,
        autocommit=autocommit,
        rename=MigrateRename(
            rename_src=rename
        )
    )
    commands.migrate(context, manifest, migrate_meta)


def freeze(ctx: TyperContext):
    """Detect schema changes and create new version

    This will read current manifest structure, compare it with a previous
    freezed version and will generate new migration version if current and last
    versions do not match.
    """
    context = ctx.obj
    # Load just store, manifests will be loaded by freeze command.
    store = load_store(context)
    with context:
        require_auth(context)
        commands.freeze(context, store.manifest)
    click.echo("Done.")


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

    def get_column_name(self, table_name: str, column_name: str, root_only: bool = False):
        given_name = get_root_attr(column_name) if root_only else column_name
        if table_name in self.tables.keys():
            table = self.tables[table_name]
            if column_name in table["columns"].keys():
                return table["columns"][column_name]

            for old_column_name, new_column_name in table["columns"].items():
                target_name = get_root_attr(old_column_name) if root_only else old_column_name
                if target_name == given_name:
                    new_name = get_root_attr(new_column_name) if root_only else new_column_name
                    return new_name
        return column_name

    def get_old_column_name(self, table_name: str, column_name: str, root_only: bool = False):
        given_name = get_root_attr(column_name) if root_only else column_name
        if table_name in self.tables.keys():
            table = self.tables[table_name]
            for old_column_column, new_column_name in table["columns"].items():
                target_name = get_root_attr(new_column_name) if root_only else new_column_name

                if target_name == given_name:
                    old_name = get_root_attr(old_column_column) if root_only else old_column_column
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

    def __init__(self, plan: bool, autocommit: bool, rename: MigrateRename, datasets: List[str] = None, migration_extension: Callable = None):
        self.plan = plan
        self.rename = rename
        self.autocommit = autocommit
        self.datasets = datasets
        self.migration_extension = migration_extension



