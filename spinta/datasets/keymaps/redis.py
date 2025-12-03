import datetime
import json
import uuid
from collections import Counter
from typing import Optional, Any
from collections.abc import Generator

from typer import echo

from redis import Redis

from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.datasets.keymaps.components import KeyMap, KeymapSyncData
from spinta.exceptions import KeyMapGivenKeyMissmatch, KeymapDuplicateMapping


class RedisKeyMap(KeyMap):
    dsn: Optional[str] = None
    duplicate_warn_only: bool = False
    sync_transaction_size: Optional[int] = None

    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn

    def __enter__(self):
        assert self.dsn is not None
        self.redis = Redis.from_url(self.dsn, decode_responses=True)
        return self

    def __exit__(self, *exc):
        self.redis.close()

    @staticmethod
    def _get_value_table_name(model_name: str) -> str:
        return f"keymap:{model_name}:values"

    @staticmethod
    def _get_key_table_name(model_name: str) -> str:
        return f"keymap:{model_name}:keys"

    @staticmethod
    def _get_metadata_table_name(model_name: str) -> str:
        return f"keymap:{model_name}:meta"

    @staticmethod
    def _get_sync_table_name() -> str:
        return "keymap:sync"

    def encode(self, name: str, value: Any, primary_key: Optional[str] = None) -> Optional[str]:
        if value is None:
            return None

        serialized = json.dumps(value, sort_keys=True)
        value_table_name = self._get_value_table_name(name)
        key_table_name = self._get_key_table_name(name)

        current_key = self.redis.hget(value_table_name, serialized)
        if current_key:
            if primary_key is None:
                return current_key
            if current_key != primary_key:
                raise KeyMapGivenKeyMissmatch(name=name, given_key=primary_key, found_key=current_key)
            return current_key

        key = primary_key or str(uuid.uuid4())
        self.redis.hset(value_table_name, serialized, key)
        self.redis.hset(key_table_name, key, serialized)
        return key

    def decode(self, name: str, key: str) -> Optional[object]:
        serialized = self.redis.hget(self._get_key_table_name(name), key)
        return json.loads(serialized) if serialized is not None else None

    def contains(self, name: str, value: Any) -> bool:
        if value is None:
            return False
        serialized = json.dumps(value, sort_keys=True)
        return self.redis.hexists(self._get_value_table_name(name), serialized)

    def has_synced_before(self) -> bool:
        return bool(self.redis.hlen(self._get_sync_table_name()))

    def get_last_synced_id(self, name: str) -> Optional[Any]:
        if last_sync_record := self.redis.hget(self._get_sync_table_name(), name):
            return json.loads(last_sync_record).get("cid")
        return None

    def update_sync_data(self, name: str, cid: Any, time: datetime.datetime) -> None:
        data = json.dumps(
            {
                "cid": cid,
                "updated": time.isoformat(),
            }
        )
        self.redis.hset(self._get_sync_table_name(), key=name, value=data)

    def synchronize(self, data: KeymapSyncData) -> None:
        name = data.name
        key = data.identifier
        redirect = data.redirect
        value = data.value
        modified_at = data.data.get("_created")

        metadata_table_name = self._get_metadata_table_name(name)

        if value:
            value_table_name = self._get_value_table_name(name)
            key_table_name = self._get_key_table_name(name)

            serialized_value = json.dumps(value, sort_keys=True)

            self.redis.hset(value_table_name, serialized_value, key)
            self.redis.hset(key_table_name, key, serialized_value)
        existing_metadata = self.redis.hget(metadata_table_name, key)
        metadata = json.loads(existing_metadata) if existing_metadata else {}
        if modified_at:
            metadata["modified_at"] = json.dumps(modified_at)
        if redirect:
            metadata["redirect"] = json.dumps(redirect)
        if metadata:
            self.redis.hset(metadata_table_name, key, json.dumps(metadata, sort_keys=True))

    def validate_data(self, name: str) -> None:
        value_table_name = self._get_value_table_name(name)
        all_values = self.redis.hvals(value_table_name)
        counts = Counter(all_values)
        affected_key_count = sum(1 for v in counts.values() if v > 1)
        if affected_key_count:
            affected_row_count = sum(v - 1 for v in counts.values() if v > 1)
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
                raise KeymapDuplicateMapping(key=name, key_count=affected_key_count, affected_count=affected_row_count)


@commands.configure.register(Context, RedisKeyMap)
def configure(context: Context, keymap: RedisKeyMap) -> None:
    rc: RawConfig = context.get("rc")

    keymap.sync_transaction_size = rc.get("keymaps", keymap.name, "sync_transaction_size", default=10000, cast=int)
    keymap.duplicate_warn_only = rc.get("keymaps", keymap.name, "duplicate_warn_only", default=False, cast=bool)
    keymap.dsn = rc.get("keymaps", keymap.name, "dsn", required=True)


@commands.prepare.register(Context, RedisKeyMap)
def prepare(context: Context, keymap: RedisKeyMap, **kwargs) -> None:
    keymap.redis = Redis.from_url(keymap.dsn, decode_responses=True)
    try:
        keymap.redis.ping()
    except Exception as e:
        raise RuntimeError(f"Failed to connect to Redis at {keymap.dsn}: {e}")


@commands.sync.register(Context, RedisKeyMap)
def sync(context: Context, keymap: RedisKeyMap, *, data: Generator[KeymapSyncData]) -> None:
    transaction_size = keymap.sync_transaction_size
    pipeline = keymap.redis.pipeline(transaction=True)
    count = 0

    try:
        for row in data:
            keymap.synchronize(row)
            count += 1
            if transaction_size and count >= transaction_size:
                pipeline.execute()
                pipeline = keymap.redis.pipeline(transaction=True)
                count = 0
            yield row
    finally:
        if count > 0:
            pipeline.execute()
