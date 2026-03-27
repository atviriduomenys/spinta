from pathlib import Path

import pytest

import spinta.adapters.soap_plugins as soap_plugins_module
from spinta.core.config import RawConfig
from spinta.core.ufuncs import Expr, UFuncRegistry
from spinta.adapters.soap_plugins import (
    _load_adapters_from_config,
    _make_body_resolver,
    get_deferred_prepare_names,
    register_soap_ufuncs,
)


def test_load_adapters_from_config_empty_when_no_raw_config() -> None:
    deferred, resolvers = _load_adapters_from_config(None)
    assert deferred == set()
    assert resolvers == {}


def test_load_adapters_from_config_empty_when_no_modules_list() -> None:
    raw_config = RawConfig()
    raw_config.add("empty", {})
    deferred, resolvers = _load_adapters_from_config(raw_config)
    assert deferred == set()
    assert resolvers == {}


def test_load_adapters_from_config_skips_missing_file(tmp_path: Path) -> None:
    raw_config = RawConfig()
    raw_config.add("cfg", {"soap_adapter_modules": [str(tmp_path / "nope.py")]})
    deferred, resolvers = _load_adapters_from_config(raw_config)
    assert deferred == set()
    assert resolvers == {}


def test_load_adapters_from_config_collects_deferred_and_resolvers(tmp_path: Path) -> None:
    adapter = tmp_path / "stub_adapter.py"
    adapter.write_text(
        """
def get_deferred_prepare_names():
    return ["stub_defer"]

def get_body_resolvers():
    return {"stub_defer": lambda env, expr=None: 42}
"""
    )
    raw_config = RawConfig()
    raw_config.add("cfg", {"soap_adapter_modules": [str(adapter)]})
    deferred, resolvers = _load_adapters_from_config(raw_config)

    assert "stub_defer" in deferred
    assert resolvers["stub_defer"](None, None) == 42


def test_load_adapters_accepts_single_path_string(tmp_path: Path) -> None:
    adapter = tmp_path / "one.py"
    adapter.write_text(
        """
def get_deferred_prepare_names():
    return "solo"

def get_body_resolvers():
    return {}
"""
    )
    raw_config = RawConfig()
    raw_config.add("cfg", {"soap_adapter_modules": str(adapter)})
    deferred, _ = _load_adapters_from_config(raw_config)
    assert deferred == {"solo"}


@pytest.fixture()
def restore_deferred_prepare_cache() -> None:
    """``register_soap_ufuncs`` writes ``soap_plugins._deferred_prepare_names_cache`` (process-wide).

    Snapshot before the test, restore after, so this module does not leak that global into other tests.
    """
    previous = soap_plugins_module._deferred_prepare_names_cache
    yield
    soap_plugins_module._deferred_prepare_names_cache = previous


def test_register_soap_ufuncs_sets_deferred_names_cache(
    tmp_path: Path,
    restore_deferred_prepare_cache,
) -> None:
    """``get_deferred_prepare_names()`` reads a module cache filled only by ``register_soap_ufuncs`` at config load."""
    adapter = tmp_path / "reg.py"
    adapter.write_text(
        """
def get_deferred_prepare_names():
    return ["only_name"]

def get_body_resolvers():
    return {}
"""
    )
    raw_config = RawConfig()
    raw_config.add("cfg", {"soap_adapter_modules": [str(adapter)]})

    registry = UFuncRegistry()
    soap_plugins_module._deferred_prepare_names_cache = None
    register_soap_ufuncs(registry, raw_config)

    assert get_deferred_prepare_names() == {"only_name"}


def test_make_body_resolver_two_arg_invocation() -> None:
    calls: list[tuple[object, ...]] = []

    def two_arg(env: object, expr: Expr | None) -> str:
        calls.append((env, expr))
        return "two"

    env = object()
    expr = Expr("x")
    wrapped = _make_body_resolver(two_arg)
    assert wrapped(env, expr) == "two"
    assert calls == [(env, expr)]


def test_make_body_resolver_falls_back_to_env_only_on_type_error() -> None:
    calls: list[object] = []

    def one_arg(env: object) -> str:
        calls.append(env)
        return "one"

    env = object()
    expr = Expr("x")
    wrapped = _make_body_resolver(one_arg)
    assert wrapped(env, expr) == "one"
    assert calls == [env]
