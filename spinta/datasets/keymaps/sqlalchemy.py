from typing import Optional, Any

import datetime
import uuid
import hashlib
import decimal
from uuid import UUID

import msgpack
import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import insert

from spinta import commands
from spinta.cli.helpers.data import ensure_data_dir
from spinta.components import Config
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.datasets.keymaps.components import KeyMap
from sqlalchemy.dialects.sqlite import insert


class SqlAlchemyKeyMap(KeyMap):
    dsn: str = None

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
            if name == '_synchronize':
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
        else:
            value, hashed = hash_return

        tmp_name = None
        if '.' in name:
            tmp_name = name.split('.')[0]

        table = self.get_table(name)

        if primary_key is not None:
            primary_key_table = self.get_table(tmp_name if tmp_name else name)
            query = sa.select([primary_key_table.c.key]).where(primary_key_table.c.key == primary_key)
        else:
            query = sa.select([table.c.key]).where(table.c.hash == hashed)
        key = self.conn.execute(query).scalar()

        if primary_key:
            stmt = insert(table).values(
                key=primary_key,
                hash=hashed,
                value=value
            )
            stmt = stmt.on_conflict_do_nothing()
            self.conn.execute(stmt)
            return primary_key
        if key is None:
            key = str(uuid.uuid4())
            self.conn.execute(table.insert(), {
                'key': key,
                'hash': hashed,
                'value': value,
            })
        return key

    def decode(self, name: str, key: str) -> object:
        table = self.get_table(name)
        query = sa.select([table.c.value]).where(table.c.key == key)
        value = self.conn.execute(query).scalar()
        value = msgpack.loads(value, raw=False)
        return value

    def get_sync_data(self, name: str) -> object:
        table = self.get_table('_synchronize')
        query = sa.select([table.c.cid]).where(table.c.model == name)
        value = self.conn.execute(query).scalar()
        return value

    def update_sync_data(self, name: str, cid: Any, time: datetime.datetime):
        table = self.get_table('_synchronize')
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

    def first_time_sync(self) -> bool:
        table = self.get_table('_synchronize')
        count = self.conn.execute(sa.func.count(table.c.model)).scalar()
        return count == 0


def _hash_value(value):
    if isinstance(value, (list, tuple)):
        value = [_encode_value(k) for k in value if k is not None]
        if len(value) == 0:
            return None
    else:
        if value is None:
            return None
        value = _encode_value(value)
    value = msgpack.dumps(value, strict_types=True)
    return value, hashlib.sha1(value).hexdigest()


def _encode_value(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, UUID):
        return str(value)
    return value


@commands.configure.register(Context, SqlAlchemyKeyMap)
def configure(context: Context, keymap: SqlAlchemyKeyMap):
    rc: RawConfig = context.get('rc')
    config: Config = context.get('config')
    dsn = rc.get('keymaps', keymap.name, 'dsn', required=True)
    ensure_data_dir(config.data_path)
    dsn = dsn.format(data_dir=config.data_path)
    keymap.dsn = dsn


@commands.prepare.register(Context, SqlAlchemyKeyMap)
def prepare(context: Context, keymap: SqlAlchemyKeyMap):
    keymap.engine = sa.create_engine(keymap.dsn)
    keymap.metadata = sa.MetaData(keymap.engine)
