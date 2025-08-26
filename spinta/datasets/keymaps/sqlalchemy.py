import datetime
import decimal
import hashlib
import json
import uuid
from collections.abc import Generator
from typing import Optional, Any
from typer import echo
from uuid import UUID

import msgpack
import sqlalchemy as sa
from multipledispatch import dispatch
from sqlalchemy.dialects.sqlite import insert

from spinta import commands
from spinta.cli.helpers.data import ensure_data_dir
from spinta.cli.helpers.script.components import ScriptTarget, ScriptTag
from spinta.cli.helpers.script.helpers import sort_scripts_by_required
from spinta.cli.helpers.upgrade.registry import upgrade_script_registry
from spinta.components import Config
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.datasets.keymaps.components import KeyMap, KeymapSyncData
from spinta.exceptions import KeyMapGivenKeyMissmatch, KeymapMigrationRequired, KeymapDuplicateMapping
from spinta.utils.json import fix_data_for_json


class SqlAlchemyKeyMap(KeyMap):
    dsn: str = None

    migration_table_name: str = "_migrations"
    sync_table_name: str = "_synchronize"
    sync_transaction_size: int = None

    # On duplicate validation warn only, instead of raise error
    duplicate_warn_only: bool = False

    def __init__(self, dsn: str = None):
        self.dsn = dsn
        self.engine = None
        self.metadata = None
        self.conn = None

    def __enter__(self):
        assert self.dsn is not None
        assert self.conn is None
        self.conn = self.engine.connect()
        return self

    def __exit__(self, *exc):
        self.conn.close()
        self.conn = None

    def get_table(self, name) -> sa.Table:
        table = self.metadata.tables.get(name)
        if table is None:
            table = self._create_table(name)
        return table

    def encode(self, name: str, value: object, primary_key=None) -> Optional[str]:
        valid_value = _valid_keymap_value(value)
        if not valid_value:
            return None

        prepared_value = prepare_value(value)
        table = self.get_table(name)
        current_key = self.conn.execute(
            sa.select([table.c.key])
            .where(sa.and_(table.c.value == prepared_value, table.c.redirect.is_(None)))
            .order_by(table.c.modified_at.desc())
            .limit(1)
        ).scalar()

        # Key was found in the table and no primary was given
        if current_key is not None and primary_key is None:
            return current_key

        if primary_key is not None:
            # Given primary key matches found key
            if primary_key == current_key:
                return primary_key

            if current_key is not None and current_key != primary_key:
                raise KeyMapGivenKeyMissmatch(name=name, given_key=primary_key, found_key=current_key)

            current_key = primary_key

        if current_key is None:
            current_key = str(uuid.uuid4())

        self.conn.execute(
            table.insert(),
            {
                "key": current_key,
                "value": prepared_value,
                "redirect": None,
                "modified_at": datetime.datetime.now(),
            },
        )
        return current_key

    def decode(self, name: str, key: str) -> object:
        table = self.get_table(name)
        query = sa.select([table.c.value]).where(table.c.key == key)
        value = self.conn.execute(query).scalar()
        return json.loads(value)

    def contains(self, name: str, value: Any) -> bool:
        valid_value = _valid_keymap_value(value)
        if not valid_value:
            return False

        table = self.get_table(name)
        prepared_value = prepare_value(value)
        query = sa.select([sa.func.count()]).where(table.c.value == prepared_value)
        return self.conn.execute(query).scalar() > 0

    def get_last_synced_id(self, name: str) -> object:
        table = self.get_table(self.sync_table_name)
        query = sa.select([table.c.cid]).where(table.c.model == name)
        value = self.conn.execute(query).scalar()
        return value

    def update_sync_data(self, name: str, cid: Any, time: datetime.datetime):
        table = self.get_table(self.sync_table_name)
        query = insert(table).values(model=name, cid=cid, updated=time)
        query = query.on_conflict_do_update(index_elements=[table.c.model], set_=dict(cid=cid, updated=time))
        self.conn.execute(query)

    def synchronize(self, data: KeymapSyncData):
        # TODO Backwards compatibility, should remove, when models without primary key are reworked
        if not data.identifiable:
            self.__legacy_synchronize(data)
            return

        table: sa.Table = self.get_table(data.name)
        id_ = data.identifier
        redirect = data.redirect
        value_ = data.value
        prepared_value = prepare_value(value_)
        modified = data.data.get("_created")
        if modified is not None:
            modified = datetime.datetime.fromisoformat(modified)

        # Redirect id to another
        if redirect is not None:
            query = (
                table.update()
                .where(table.c.key == id_)
                .values(
                    redirect=redirect,
                    modified_at=modified,
                )
            )
            self.conn.execute(query)
            return

        valid_value = _valid_keymap_value(value_)
        if not valid_value:
            return

        select_query = table.select().where(table.c.key == id_)
        entry = self.conn.execute(select_query).scalar()
        if entry is None:
            query = insert(table).values(key=id_, value=prepared_value, modified_at=modified)
            self.conn.execute(query)
        else:
            update_query = (
                sa.update(table)
                .values(value=prepared_value, redirect=redirect, modified_at=modified)
                .where(table.c.key == id_)
            )
            self.conn.execute(update_query)

    def __legacy_synchronize(self, data: KeymapSyncData):
        """
        Do not run this method anymore, it is here only for backward compatibility.
        Models without a primary key should not be synchronized, but this feature is still not implemented.
        For that reason we still need to run this code (even though it can create duplicate value mapping).
        """
        table: sa.Table = self.get_table(data.name)
        id_ = data.identifier
        redirect = data.redirect
        value_ = data.value
        modified = data.data.get("_created")
        if modified is not None:
            modified = datetime.datetime.fromisoformat(modified)

        if redirect is not None:
            return

        valid_value = _valid_keymap_value(value_)
        if not valid_value:
            return

        prepared_value = prepare_value(value_)
        where_condition = sa.or_(table.c.key == id_, table.c.value == prepared_value)
        select_query = sa.select([sa.func.count()]).select_from(table).where(where_condition)
        count = self.conn.execute(select_query).scalar()
        should_insert = True
        if count == 1:
            should_insert = False
            update_query = (
                sa.update(table)
                .values(key=id_, value=prepared_value, redirect=redirect, modified_at=modified)
                .where(where_condition)
            )
            self.conn.execute(update_query)
        else:
            delete_query = sa.delete(table).where(where_condition)
            self.conn.execute(delete_query)

        if should_insert:
            query = insert(table).values(key=id_, value=prepared_value, redirect=redirect, modified_at=modified)
            self.conn.execute(query)

    def has_synced_before(self) -> bool:
        table = self.get_table(self.sync_table_name)
        count = self.conn.execute(sa.func.count(table.c.model)).scalar()
        return count != 0

    def validate_data(self, name: str):
        table = self.get_table(name)
        dup_values = (
            sa.select(table.c.value)
            .where(table.c.redirect.is_(None))
            .group_by(table.c.value)
            .having(sa.func.count(table.c.value) > 1)
            .subquery()
        )

        affected_key_count_query = sa.select(sa.func.count()).select_from(dup_values)
        affected_row_count_query = (
            sa.select(sa.func.count())
            .select_from(table)
            .where(sa.and_(table.c.value.in_(sa.select(dup_values.c.value)), table.c.redirect.is_(None)))
        )
        affected_key_count = self.conn.execute(affected_key_count_query).scalar_one()
        affected_row_count = self.conn.execute(affected_row_count_query).scalar_one()

        if affected_key_count and affected_row_count:
            if self.duplicate_warn_only:
                echo(
                    (
                        f"WARNING: Keymap's ({self.name!r}) {name!r} key contains {affected_key_count} duplicate value combinations.\n"
                        f"This affects {affected_row_count} keymap entries.\n\n"
                        "Make sure that synchronizing data is valid and is up to date. If it is, try rerunning keymap synchronization.\n"
                        "If the issue persists, you may need to reset this key's keymap data and rerun synchronization again.\n"
                        "If nothing helps contact data provider.\n\n"
                        "To re-enable this error, you can set `duplicate_warn_only: false` parameter in keymap configuration.\n"
                    ),
                    err=True,
                )
            else:
                raise KeymapDuplicateMapping(
                    self, key=name, key_count=affected_key_count, affected_count=affected_row_count
                )

    def contains_migration(self, name: str):
        migrations = self.get_table(self.migration_table_name)

        query = sa.select([sa.func.count()]).where(migrations.c.migration == name)
        count = self.conn.execute(query).scalar()
        return count != 0

    def mark_migration(self, name: str):
        if self.contains_migration(name):
            return

        migrations = self.get_table(self.migration_table_name)
        stmt = migrations.insert().values(migration=name)
        self.conn.execute(stmt)

    def _create_table(self, name: str) -> sa.Table:
        if name == self.sync_table_name:
            table = sa.Table(
                name,
                self.metadata,
                sa.Column("model", sa.Text, primary_key=True),
                sa.Column("cid", sa.BIGINT),
                sa.Column("updated", sa.DateTime),
            )
        elif name == self.migration_table_name:
            table = sa.Table(
                name,
                self.metadata,
                sa.Column("migration", sa.Text, primary_key=True),
                sa.Column("applied_at", sa.DateTime, server_default=sa.func.now()),
            )
        else:
            table = sa.Table(
                name,
                self.metadata,
                sa.Column("key", sa.Text, primary_key=True),
                sa.Column("value", sa.Text, index=True),
                sa.Column("redirect", sa.Text, index=True),
                sa.Column("modified_at", sa.DateTime, index=True),
            )

        table.create(checkfirst=True)
        return table


def _valid_keymap_value(value: object) -> bool:
    if value is None:
        return False

    if isinstance(value, (list, tuple)):
        filtered = [v for v in value if v is not None]
        if len(filtered) == 0:
            return False

    return True


def prepare_value(value):
    return json.dumps(fix_data_for_json(value))


def _hash_value(value):
    if value is None:
        return None

    if isinstance(value, (list, tuple)):
        value = [_encode_value(k) for k in value if k is not None]
        if len(value) == 0:
            return None
    else:
        value = _encode_value(value)
    value = msgpack.dumps(value, strict_types=True)
    return value, hashlib.sha1(value).hexdigest()


@dispatch((datetime.datetime, datetime.date, datetime.time))
def _encode_value(value: (datetime.datetime, datetime.date, datetime.time)):
    return value.isoformat()


@dispatch(decimal.Decimal)
def _encode_value(value: decimal.Decimal):
    return float(value)


@dispatch(tuple)
def _encode_value(value: tuple):
    return list(value)


@dispatch(UUID)
def _encode_value(value: UUID):
    return str(value)


@dispatch(object)
def _encode_value(value: object):
    return value


@commands.configure.register(Context, SqlAlchemyKeyMap)
def configure(context: Context, keymap: SqlAlchemyKeyMap):
    rc: RawConfig = context.get("rc")
    config: Config = context.get("config")
    dsn = rc.get("keymaps", keymap.name, "dsn", required=True)
    sync_transaction_size = rc.get("keymaps", keymap.name, "sync_transaction_size", default=10000, cast=int)
    ensure_data_dir(config.data_path)
    dsn = dsn.format(data_dir=config.data_path)
    if dsn.startswith("sqlite:///"):
        dsn = dsn.replace("sqlite:///", "sqlite+spinta:///")
    keymap.sync_transaction_size = sync_transaction_size
    keymap.dsn = dsn
    keymap.duplicate_warn_only = rc.get("keymaps", keymap.name, "duplicate_warn_only", cast=bool, default=False)


@commands.prepare.register(Context, SqlAlchemyKeyMap)
def prepare(context: Context, keymap: SqlAlchemyKeyMap, **kwargs):
    keymap.engine = sa.create_engine(keymap.dsn)
    keymap.metadata = sa.MetaData(keymap.engine)

    fresh = is_fresh_database(context, keymap)
    if fresh:
        initialize_meta_tables(keymap)
    else:
        validate_migrations(context, keymap)


@commands.sync.register(Context, SqlAlchemyKeyMap)
def sync(context: Context, keymap: SqlAlchemyKeyMap, *, data: Generator[KeymapSyncData]):
    transaction_size = keymap.sync_transaction_size
    transaction = None
    try:
        transaction = keymap.conn.begin()
        for i, row in enumerate(data):
            if transaction_size is not None and i % transaction_size == 0 and i != 0:
                if transaction.is_active:
                    transaction.commit()

                transaction = keymap.conn.begin()

            keymap.synchronize(row)
            yield row
    finally:
        if transaction is not None and transaction.is_active:
            transaction.commit()


def validate_migrations(context: Context, keymap: SqlAlchemyKeyMap):
    config = context.get("config")
    if config.upgrade_mode:
        return

    migration_scripts = upgrade_script_registry.get_all(
        targets={ScriptTarget.SQLALCHEMY_KEYMAP.value}, tags={ScriptTag.DB_MIGRATION.value}
    )
    filtered = sort_scripts_by_required(migration_scripts)
    for script in filtered.values():
        if script.check(context):
            raise KeymapMigrationRequired(keymap, migration=script.name)


def initialize_meta_tables(keymap: SqlAlchemyKeyMap):
    # Sync table, get_table auto creates tables if not found
    with keymap:
        keymap.get_table(keymap.sync_table_name)

        # Migrations table
        keymap.get_table(keymap.migration_table_name)

        # Mark all migration scripts as already executed
        migration_scripts = upgrade_script_registry.get_all(
            targets={ScriptTarget.SQLALCHEMY_KEYMAP.value}, tags={ScriptTag.DB_MIGRATION.value}
        )
        filtered = sort_scripts_by_required(migration_scripts)
        for script in filtered.values():
            keymap.mark_migration(script.name)


def is_fresh_database(context: Context, keymap: SqlAlchemyKeyMap) -> bool:
    insp = sa.inspect(keymap.engine)
    tables = insp.get_table_names()
    if keymap.sync_table_name in tables:
        return False

    if keymap.migration_table_name in tables:
        return False

    if not len(tables):
        return True

    tables = [table for table in tables if not table.startswith("_")]
    manifest = context.get("store").manifest
    for table in tables:
        # Remove sub properties, like "example/data/Model_prop0_prop1" so it becomes "example/data/Model"
        table_name = table.split("_")[0]
        if commands.has_model(context, manifest, table_name):
            return False

    return True
