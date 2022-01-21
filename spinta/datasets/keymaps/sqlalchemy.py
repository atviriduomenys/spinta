from typing import Optional

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
            table = sa.Table(
                name, self.metadata,
                sa.Column('key', sa.Text, primary_key=True),
                sa.Column('hash', sa.Text, unique=True),
                sa.Column('value', sa.LargeBinary),
            )
            table.create(checkfirst=True)
        return self.metadata.tables[name]

    def encode(self, name: str, value: object) -> Optional[str]:
        # Make value msgpack serializable.
        if isinstance(value, (list, tuple)):
            value = [_encode_value(k) for k in value if k is not None]
            if len(value) == 0:
                return None
        else:
            if value is None:
                return None
            value = _encode_value(value)

        # Get value hash.
        value = msgpack.dumps(value, strict_types=True)
        hash = hashlib.sha1(value).hexdigest()

        # Try to find key by value hash.
        table = self.get_table(name)
        query = sa.select([table.c.key]).where(table.c.hash == hash)
        key = self.conn.execute(query).scalar()
        if key is None:

            # Create new key.
            key = str(uuid.uuid4())
            self.conn.execute(table.insert(), {
                'key': key,
                'hash': hash,
                'value': value,
            })

        return key

    def decode(self, name: str, key: str) -> object:
        table = self.get_table(name)
        query = sa.select([table.c.value]).where(table.c.key == key)
        value = self.conn.execute(query).scalar()
        value = msgpack.loads(value, raw=False)
        return value


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
