from collections.abc import Generator
from typing import Optional, Any

import datetime
import uuid
import hashlib
import decimal
from uuid import UUID

import msgpack
import sqlalchemy as sa

from spinta import commands
from spinta.cli.helpers.data import ensure_data_dir
from spinta.components import Config
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.datasets.keymaps.components import KeyMap
from sqlalchemy.dialects.sqlite import insert

from spinta.datasets.keymaps.sync import KeymapData
from spinta.exceptions import KeyMapGivenKeyMissmatch

from multipledispatch import dispatch


class SqlAlchemyKeyMap(KeyMap):
    dsn: str = None

    sync_table_name: str = '_synchronize'
    sync_transaction_size: int = None

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

    def get_table(self, name):
        if name not in self.metadata.tables:
            if name == self.sync_table_name:
                table = sa.Table(
                    name, self.metadata,
                    sa.Column('model', sa.Text, primary_key=True),
                    sa.Column('cid', sa.BIGINT),
                    sa.Column('updated', sa.DateTime),
                )
            else:
                table = sa.Table(
                    name, self.metadata,
                    sa.Column('key', sa.Text, primary_key=True),
                    sa.Column('hash', sa.Text, unique=True, index=True),
                    sa.Column('value', sa.LargeBinary),
                )
            table.create(checkfirst=True)
        return self.metadata.tables[name]

    def encode(self, name: str, value: object, primary_key=None) -> Optional[str]:
        # Make value msgpack serializable.

        hash_return = _hash_value(value)
        if hash_return is None:
            return None

        value, hashed = hash_return

        table = self.get_table(name)
        current_key = self.conn.execute(
            sa.select([table.c.key]).where(
                table.c.hash == hashed
            )
        ).scalar()

        # Key was found in the table and no primary was given
        if current_key is not None and primary_key is None:
            return current_key

        if primary_key is not None:
            # Given primary key matches found key
            if primary_key == current_key:
                return primary_key

            if current_key is not None and current_key != primary_key:
                raise KeyMapGivenKeyMissmatch(
                    name=name,
                    given_key=primary_key,
                    found_key=current_key
                )

            current_key = primary_key

        if current_key is None:
            current_key = str(uuid.uuid4())

        self.conn.execute(table.insert(), {
            'key': current_key,
            'hash': hashed,
            'value': value,
        })
        return current_key

    def decode(self, name: str, key: str) -> object:
        table = self.get_table(name)
        query = sa.select([table.c.value]).where(table.c.key == key)
        value = self.conn.execute(query).scalar()
        value = msgpack.loads(value, raw=False)
        return value

    def contains(self, name: str, value: Any) -> bool:
        result = _hash_value(value)

        if result is None:
            return False

        encoded_value, encoded_hash = result

        table = self.get_table(name)
        query = sa.select([sa.func.count()]).where(
            sa.and_(
                table.c.value == encoded_value,
                table.c.hash == encoded_hash
            )
        )
        return self.conn.execute(query).scalar() > 0

    def get_last_synced_id(self, name: str) -> object:
        table = self.get_table(self.sync_table_name)
        query = sa.select([table.c.cid]).where(table.c.model == name)
        value = self.conn.execute(query).scalar()
        return value

    def update_sync_data(self, name: str, cid: Any, time: datetime.datetime):
        table = self.get_table(self.sync_table_name)
        query = insert(table).values(model=name, cid=cid, updated=time)
        query = query.on_conflict_do_update(
            index_elements=[table.c.model],
            set_=dict(cid=cid, updated=time)
        )
        self.conn.execute(query)

    def synchronize(self, name: str, value: Any, primary_key: str):
        table = self.get_table(name)
        hash_return = _hash_value(value)
        if hash_return is None:
            return None
        else:
            value, hashed = hash_return

        where_condition = sa.or_(table.c.key == primary_key, table.c.hash == hashed)
        select_query = sa.select([sa.func.count()]).select_from(table).where(where_condition)
        count = self.conn.execute(select_query).scalar()
        should_insert = True
        if count == 1:
            should_insert = False
            update_query = sa.update(table).values(key=primary_key, hash=hashed, value=value).where(where_condition)
            self.conn.execute(update_query)
        else:
            delete_query = sa.delete(table).where(where_condition)
            self.conn.execute(delete_query)

        if should_insert:
            query = insert(table).values(key=primary_key, hash=hashed, value=value)
            self.conn.execute(query)

    def has_synced_before(self) -> bool:
        table = self.get_table(self.sync_table_name)
        count = self.conn.execute(sa.func.count(table.c.model)).scalar()
        return count != 0


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
    rc: RawConfig = context.get('rc')
    config: Config = context.get('config')
    dsn = rc.get('keymaps', keymap.name, 'dsn', required=True)
    sync_transaction_size = rc.get('keymaps', keymap.name, 'sync_transaction_size', default=10000, cast=int)
    ensure_data_dir(config.data_path)
    dsn = dsn.format(data_dir=config.data_path)
    if dsn.startswith('sqlite:///'):
        dsn = dsn.replace('sqlite:///', 'sqlite+spinta:///')
    keymap.sync_transaction_size = sync_transaction_size
    keymap.dsn = dsn


@commands.prepare.register(Context, SqlAlchemyKeyMap)
def prepare(context: Context, keymap: SqlAlchemyKeyMap, **kwargs):
    keymap.engine = sa.create_engine(keymap.dsn)
    keymap.metadata = sa.MetaData(keymap.engine)


@commands.sync.register(Context, SqlAlchemyKeyMap)
def sync(context: Context, keymap: SqlAlchemyKeyMap, *, data: Generator[KeymapData]):
    transaction_size = keymap.sync_transaction_size
    transaction = None
    try:
        transaction = keymap.conn.begin()
        for i, row in enumerate(data):
            if transaction_size is not None and i % transaction_size == 0 and i != 0:
                if transaction.is_active:
                    transaction.commit()

                transaction = keymap.conn.begin()

            keymap.synchronize(row.key, row.value, row.identifier)
            yield row
    finally:
        if transaction is not None and transaction.is_active:
            transaction.commit()
