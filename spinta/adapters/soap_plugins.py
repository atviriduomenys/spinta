"""
SOAP adapter registration via **config-driven module loading**.
Adapters are loaded from local Python files specified in `config.yml`:
    soap_adapter_modules:
      - /path/to/adapter.py
Each module should provide:
    - get_deferred_prepare_names() -> list[str]
    - get_body_resolvers() -> dict[str, callable]
This keeps Spinta core generic while allowing deployments to drop in
adapter files without publishing packages or configuring entry points.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from typing import Any, Callable

from spinta.core.ufuncs import Expr
from spinta.core.config import RawConfig
from spinta.ufuncs.loadbuilder.components import LoadBuilder
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder

log = logging.getLogger(__name__)


_deferred_prepare_names_cache: set[str] | None = None


def get_deferred_prepare_names() -> set[str]:
    """Return deferred prepare names loaded at startup.
    This is used by SOAP query builders to skip resolving deferred expressions
    until the request body is populated and body resolvers run.
    If `register_soap_ufuncs()` hasn't been called yet, this returns an empty set.
    """
    return _deferred_prepare_names_cache or set()


def _load_adapter_module_from_path(file_path: str):
    """Load a Python module from a file path.
    Returns the module object, or None if loading fails.
    """
    try:
        path = Path(file_path)
        if not path.exists():
            log.warning("SOAP adapter module not found: %s", file_path)
            return None
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            log.warning("Failed to create module spec for: %s", file_path)
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        log.info("Loaded SOAP adapter module from: %s", file_path)
        return module
    except Exception as e:
        log.warning("Failed to load SOAP adapter module %s: %s", file_path, e)
        return None


def _load_adapters_from_config(raw_config: RawConfig | None) -> tuple[set[str], dict[str, Callable[..., Any]]]:
    """Load adapter modules specified in raw config.
    Config format:
        soap_adapter_modules:
          - /path/to/adapter.py
          - /path/to/another_adapter.py
    Each module should have:
    - get_deferred_prepare_names() -> list[str]
    - get_body_resolvers() -> dict[str, callable]
    """
    deferred: set[str] = set()
    body_resolvers: dict[str, Callable[..., Any]] = {}

    if raw_config is None:
        return deferred, body_resolvers

    module_paths = raw_config.get("soap_adapter_modules", default=[])
    if not module_paths:
        return deferred, body_resolvers

    if isinstance(module_paths, str):
        module_paths = [module_paths]

    for module_path in module_paths:
        module = _load_adapter_module_from_path(module_path)
        if module is None:
            continue

        if hasattr(module, "get_deferred_prepare_names"):
            try:
                result = module.get_deferred_prepare_names()
                if isinstance(result, (list, tuple)):
                    deferred.update(result)
                else:
                    deferred.add(str(result))
            except Exception as e:
                log.warning("Failed to call get_deferred_prepare_names in %s: %s", module_path, e)

        if hasattr(module, "get_body_resolvers"):
            try:
                result = module.get_body_resolvers()
                if isinstance(result, dict):
                    body_resolvers.update(result)
            except Exception as e:
                log.warning("Failed to call get_body_resolvers in %s: %s", module_path, e)

    return deferred, body_resolvers


def _deferred_prepare_resolver(env, expr: Expr) -> Expr:
    env.param.soap_body = {env.this: expr}
    return expr


def _make_body_resolver(callable_fn: Callable[..., Any]):
    def body_resolver(env, expr: Expr) -> Any:
        try:
            return callable_fn(env, expr)
        except TypeError:
            return callable_fn(env)
    return body_resolver


def register_soap_ufuncs(registry, raw_config: RawConfig | None = None) -> None:
    """Register SOAP adapter resolvers from config (`soap_adapter_modules`).
    Adapters are loaded from local Python files listed in:
        soap_adapter_modules:
          - /path/to/adapter.py
    Call this after resolver.collect().
    """
    global _deferred_prepare_names_cache

    deferred, body_resolvers = _load_adapters_from_config(raw_config)

    _deferred_prepare_names_cache = deferred

    for name in deferred:
        registry.register(name, _deferred_prepare_resolver, (LoadBuilder, Expr))

    for name, callable_fn in body_resolvers.items():
        registry.register(name, _make_body_resolver(callable_fn), (SoapQueryBuilder, Expr))
