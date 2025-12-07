import datetime
import json
from unittest import mock

import fakeredis
import pytest

from spinta.datasets.keymaps.redis import RedisKeyMap
from spinta.exceptions import KeyMapGivenKeyMissmatch, KeymapDuplicateMapping


@pytest.fixture
def redis_in_memory_keymap():
    keymap = RedisKeyMap(dsn="redis://localhost")
    keymap.redis = fakeredis.FakeRedis(decode_responses=True)
    return keymap


def test_encode_and_decode(redis_in_memory_keymap):
    name = "test"
    value = {"id": 1, "name": "Alice"}

    key = redis_in_memory_keymap.encode(name, value)
    assert key is not None

    # Should be able to decode the same key
    decoded = redis_in_memory_keymap.decode(name, key)
    assert decoded == value

    # Should return same key when encoding same value
    same_key = redis_in_memory_keymap.encode(name, value)
    assert same_key == key


def test_encode_with_primary_key(redis_in_memory_keymap):
    name = "user"
    value = {"id": 5}
    primary_key = "my-key"

    key = redis_in_memory_keymap.encode(name, value, primary_key)
    assert key == primary_key

    # Calling again with same key is fine
    assert redis_in_memory_keymap.encode(name, value, primary_key) == primary_key


def test_encode_key_mismatch_raises(redis_in_memory_keymap):
    name = "user"
    value = {"id": 99}
    redis_in_memory_keymap.encode(name, value, "key-1")

    with pytest.raises(KeyMapGivenKeyMissmatch):
        redis_in_memory_keymap.encode(name, value, "wrong-key")


def test_contains(redis_in_memory_keymap):
    name = "sample"
    value = {"id": 11}

    assert not redis_in_memory_keymap.contains(name, value)
    redis_in_memory_keymap.encode(name, value)
    assert redis_in_memory_keymap.contains(name, value)


def test_has_synced_before_and_get_last_synced_id(redis_in_memory_keymap):
    sync_table = redis_in_memory_keymap._get_sync_table_name()

    assert not redis_in_memory_keymap.has_synced_before()

    now = datetime.datetime.now().isoformat()
    redis_in_memory_keymap.redis.hset(sync_table, "model1", json.dumps({"cid": "abc", "updated": now}))
    assert redis_in_memory_keymap.has_synced_before()
    assert redis_in_memory_keymap.get_last_synced_id("model1") == "abc"
    assert redis_in_memory_keymap.get_last_synced_id("missing") is None


def test_update_sync_data(redis_in_memory_keymap):
    name = "mymodel"
    now = datetime.datetime.now()

    redis_in_memory_keymap.update_sync_data(name, cid="xyz", time=now)
    stored = redis_in_memory_keymap.redis.hget(redis_in_memory_keymap._get_sync_table_name(), name)
    data = json.loads(stored)

    assert data["cid"] == "xyz"
    assert "updated" in data


def test_synchronize_sets_value_and_metadata(redis_in_memory_keymap):
    from spinta.datasets.keymaps.components import KeymapSyncData

    data = KeymapSyncData(
        name="model",
        identifier="k1",
        redirect=None,
        value={"id": 1, "val": 10},
        data={"_created": "2025-01-01T00:00:00"},
    )

    redis_in_memory_keymap.synchronize(data)

    key_table = redis_in_memory_keymap._get_key_table_name("model")
    value_table = redis_in_memory_keymap._get_value_table_name("model")
    meta_table = redis_in_memory_keymap._get_metadata_table_name("model")

    serialized = json.dumps({"id": 1, "val": 10}, sort_keys=True)
    assert redis_in_memory_keymap.redis.hget(value_table, serialized) == "k1"
    assert json.loads(redis_in_memory_keymap.redis.hget(key_table, "k1")) == {"id": 1, "val": 10}

    metadata = json.loads(redis_in_memory_keymap.redis.hget(meta_table, "k1"))
    assert json.loads(metadata["modified_at"]) == "2025-01-01T00:00:00"


def test_synchronize_with_redirect(redis_in_memory_keymap):
    from spinta.datasets.keymaps.components import KeymapSyncData

    data = KeymapSyncData(name="redirect_test", identifier="old-id", redirect="new-id", value={"x": 5}, data={})
    redis_in_memory_keymap.synchronize(data)

    meta_table = redis_in_memory_keymap._get_metadata_table_name("redirect_test")
    key_table = redis_in_memory_keymap._get_key_table_name("redirect_test")
    value_table = redis_in_memory_keymap._get_value_table_name("redirect_test")
    metadata = json.loads(redis_in_memory_keymap.redis.hget(meta_table, "old-id"))
    assert json.loads(metadata["redirect"]) == "new-id"
    assert redis_in_memory_keymap.redis.hgetall(key_table) == {"old-id": '{"x": 5}'}
    assert redis_in_memory_keymap.redis.hgetall(value_table) == {'{"x": 5}': "old-id"}


def test_validate_data_no_duplicates(redis_in_memory_keymap):
    name = "unique"
    val_table = redis_in_memory_keymap._get_value_table_name(name)
    redis_in_memory_keymap.redis.hset(val_table, "v1", "k1")
    redis_in_memory_keymap.redis.hset(val_table, "v2", "k2")

    redis_in_memory_keymap.validate_data(name)


def test_validate_data_duplicates_raise(redis_in_memory_keymap):
    name = "dupes"
    val_table = redis_in_memory_keymap._get_value_table_name(name)
    redis_in_memory_keymap.redis.hset(val_table, "v1", "dup")
    redis_in_memory_keymap.redis.hset(val_table, "v2", "dup")

    with pytest.raises(KeymapDuplicateMapping):
        redis_in_memory_keymap.validate_data(name)


def test_validate_data_duplicates_warn_only(redis_in_memory_keymap, monkeypatch):
    name = "dupes_warn"
    val_table = redis_in_memory_keymap._get_value_table_name(name)
    redis_in_memory_keymap.redis.hset(val_table, "v1", "dup")
    redis_in_memory_keymap.redis.hset(val_table, "v2", "dup")
    redis_in_memory_keymap.duplicate_warn_only = True

    with mock.patch("spinta.datasets.keymaps.redis.echo") as mock_echo:
        redis_in_memory_keymap.validate_data(name)

        # Assert echo was called at least once
        assert mock_echo.called
        # Grab the first call's arguments
        msg, err = mock_echo.call_args[0][0], mock_echo.call_args[1].get("err", False)
        assert "WARNING: Keymap's" in msg
        assert err is True
