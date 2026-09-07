from __future__ import annotations

import collections
import enum
import logging
import os
import pathlib
import sys
import typing
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

from ruamel.yaml import YAML

from spinta.core.enums import Mode
from spinta.utils.imports import importstr
from spinta.utils.path import resource_filename
from spinta.utils.schema import NA

if typing.TYPE_CHECKING:
    from spinta.manifests.components import ManifestPath

Schema = Dict[str, Any]
# A configuration key path, e.g. `("backends", "default", "dsn")` for
# `backends.default.dsn`. The root key is an empty tuple `()`.
Key = Tuple[str, ...]


class KeyNode(NamedTuple):
    type: str | None  # resolved from SCHEMA (`spinta/config.yml`), None if unknown
    source: ConfigSource | None  # last source that wrote this node, None for schema-derived nodes
    env: str | None  # env overlay name if value came from environments.<env>, else None
    value: Any  # object-type -> effective child-name list; else leaf value
    # True if `value` was explicitly set by a configuration source. False for
    # auto-derived nodes: union parent nodes (child-name lists collected from
    # more specific keys) and schema-derived nodes (object structure and
    # default values). Only explicit object-type nodes hide child keys that
    # are not in their child-name list (see `RawConfig._key_exists`).
    explicit: bool = False


yaml = YAML(typ="safe")

log = logging.getLogger(__name__)

SCHEMA = {
    "type": "object",
    "items": yaml.load(resource_filename("spinta", "config.yml").read_text()),
}


def read_config(args=None, envfile=None):
    # `config` option can be set in any source (even in cliargs), so we first
    # read all sources to discover additional config files, and only then read
    # everything in the correct order. The effective order of sources is:
    #
    #     spinta -> config files -> envfile -> envvars -> cliargs
    #
    # Each subsequent source overrides values from all previous sources.
    def _make_sources():
        return [
            Path("spinta", "spinta.config:CONFIG"),
            EnvFile("envfile", envfile or ".env"),
            EnvVars("envvars", os.environ),
            CliArgs("cliargs", args or []),
        ]

    tmp_rc = RawConfig()
    tmp_rc.read(_make_sources())
    configs = tmp_rc.get("config", cast=list, default=[])
    if configs:
        final_sources = [
            Path("spinta", "spinta.config:CONFIG"),
            *[Path(c, c) for c in configs],
            EnvFile("envfile", envfile or ".env"),
            EnvVars("envvars", os.environ),
            CliArgs("cliargs", args or []),
        ]
        rc = RawConfig()
        rc.read(final_sources)
        return rc
    else:
        return tmp_rc


class KeyFormat(str, enum.Enum):
    cfg = "cfg"
    cli = "cli"
    env = "env"


class ConfigSource:
    name: str

    def __init__(self, name=None, config=None):
        self.name = self.getname(name)
        self.config = {} if config is None else config

    def __str__(self):
        return self.name

    def __repr__(self):
        return type(self).__module__ + "." + type(self).__name__ + "(" + repr(self.name) + ")"

    def getname(self, name):
        return name or self.name or type(self).__name__

    def read(self, schema: Schema):
        config = {}
        for k, v in self.config.items():
            v = dict(_traverse(v, k))
            inner = _get_inner_keys(v, depth=len(k))
            v.update(inner)
            config.update(v)
        self.config = config

    def keys(self, env: Optional[str] = None):
        if env:
            for key in self.config:
                if key[:2] == ("environments", env):
                    yield key[2:]
        else:
            for key in self.config:
                if key[:1] != ("environments",):
                    yield key

    def get(self, key: tuple, env: Optional[str] = None):
        if env:
            return self.config.get(("environments", env) + key, NA)
        else:
            return self.config.get(key, NA)


class PyDict(ConfigSource):
    def read(self, schema: Schema):
        # Don't modify given config dict, it can be shared, e.g. a global
        # `spinta.config.CONFIG` dict can be imported by multiple `RawConfig`
        # instances.
        config = dict(self.config)
        envs = config.pop("environments", {})
        config = {tuple(k.split(".")): v for k, v in config.items()}
        for env, values in envs.items():
            for k, v in values.items():
                config[("environments", env) + tuple(k.split("."))] = v
        self.config = config
        super().read(schema)
        _check_keys(self.config, schema, self.name)


class Path(PyDict):
    def read(self, schema: Schema):
        if self.config.endswith((".yml", ".yaml")):
            path = pathlib.Path(self.config)
            self.config = yaml.load(path.read_text())
        else:
            self.config = importstr(self.config)
        super().read(schema)


class CliArgs(PyDict):
    name = "cli"

    def read(self, schema: Schema):
        config = {}
        for arg in self.config:
            key, val = arg.split("=", 1)
            if "," in val:
                val = [v.strip() for v in val.split(",")]
            config[key] = val
        self.config = _parse_object_key_values(config, schema)
        super().read(schema)


class EnvVars(ConfigSource):
    name = "env"

    def read(self, schema: Schema):
        config = {}
        for key, val in self.config.items():
            if not key.startswith("SPINTA_"):
                continue
            key = key[len("SPINTA_") :]
            key = tuple(key.lower().split("__"))
            if len(key) > 1 and key[0] not in schema["items"] and key[1] in schema["items"]:
                key = ("environments",) + key
            config[key] = val
        config = _parse_object_key_values(config, schema)
        self.config = config
        super().read(schema)


class EnvFile(EnvVars):
    def read(self, schema: Schema):
        config = {}
        path = pathlib.Path(self.config)
        if path.exists():
            with path.open() as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    line = line.strip()
                    if line == "":
                        continue
                    if "=" not in line:
                        continue
                    name, value = line.split("=", 1)
                    config[name] = value
        self.config = config
        super().read(schema)


# A mapping of a config key path (see `Key`) to a `KeyNode`. Holds the whole
# effective configuration tree: values from all sources, with
# `environments.<env>.*` overlays applied (prefix dropped) and schema
# structure and defaults merged in. All read methods (`get`, `keys`,
# `getall`, ...) read only this tree.
# Example:
#   keys: ConfigKeys = {
#       ():                      KeyNode("object", spinta, None, ["backends", "manifests", ...]),  # root
#       ("backends",):           KeyNode("object", spinta, None, ["default", "keymaps"]),
#       ("backends", "default"): KeyNode("object", envvars, None, ["type", "dsn", "name"]),
#       ...
#   }
ConfigKeys = Dict[Key, KeyNode]


def _flatten_fork_config(data: Dict[str, Any]) -> Dict[Key, Any]:
    """Flatten a nested fork dict into flat tuple keys.

    A fork often contains only a partial configuration, e.g. only
    `keymaps.default.type` without `keymaps.default.dsn`. Flattening nested
    structures into leaf keys makes such partial forks merge with lower
    priority sources instead of replacing them.

    Top-level keys are split on dots (config hierarchy, e.g.
    `"backends.three"`), while nested dict keys are kept verbatim (they may
    contain dots themselves, e.g. denorm property `"test.name"`).
    """
    flat: Dict[Key, Any] = {}
    for k, v in data.items():
        prefix = tuple(k.split("."))
        if isinstance(v, dict):
            if not v:
                # Empty dicts declare nothing, same as before.
                continue
            for subpath, subval in _traverse(v, ()):
                flat[prefix + subpath] = subval
        else:
            flat[prefix] = v
    return flat


class RawConfig:
    """A raw configuration reader component

    Reads configuration directly from supported configuration `sources`.

    Currently supported configuration sources are:

    - `PyDict` - python `dict` objects.
    - `Path` - python module path pointing to a `dict` or YAML file path.
    - `EnvVars` - environment variables with `SPINTA_` prefix.
    - `EnvFile` - `.env` files containing variables with `SPINTA_` prefix.
    - `CliArgs` - `-o` command line arguments with `name=value` values.

    Args:
        sources: List of sources to read configuration options from.

    """

    sources: List[ConfigSource]

    def __init__(self, sources: Optional[List[ConfigSource]] = None):
        self._locked = False
        self.sources = sources or []
        self._keys: ConfigKeys = {}
        self._schema = SCHEMA

    def read(self, sources: List[ConfigSource]):
        if self._locked:
            raise Exception("Configuration is locked, use `rc.fork()` if you need to change configuration.")

        for config in sources:
            log.info(f"Reading config from {config.name}.")
            config.read(self._schema)

        self.sources.extend(sources)
        self._rebuild()

    def add(self, name, params):
        self.read([PyDict(name, params)])
        return self

    def fork(self, sources=None) -> RawConfig:
        rc = RawConfig(list(self.sources))
        if sources:
            if isinstance(sources, dict):
                fork_source = ConfigSource("fork", _flatten_fork_config(sources))
                rc.read([fork_source])
            else:
                rc.read(sources)
        else:
            rc._rebuild()
        return rc

    def lock(self):
        self._locked = True

    def has(self, *key: str) -> bool:
        return self.get(*key, default=NA) is not NA

    def get(
        self,
        *key: str,
        default=NA,
        cast=None,
        required=False,
        exists=False,
        origin=False,
    ) -> Any:
        node = self._keys.get(key)
        if node is not None and self._key_exists(key):
            config = node.source
            if node.type == "object":
                # The schema declares this key as an object (e.g. `backends`
                # is a mapping of backend names to backend configs), so its
                # value is the list of child key names, and a reset value,
                # like an empty string, is returned as an empty list.
                value = list(node.value)
            else:
                value = node.value
        else:
            value, config = default, None

        if cast is not None:
            if cast is list and isinstance(value, str):
                value = value.split(",") if value else []
            elif value is not None:
                value = cast(value)
            else:
                # XXX: why []?
                value = default or []

        if required and value is None:
            name = ".".join(key)
            raise Exception(f"{name!r} is a required configuration option.")

        if exists and isinstance(value, pathlib.Path) and not value.exists():
            name = ".".join(key)
            raise Exception(f"{name} ({value}) path does not exist.")

        if origin:
            if config:
                return value, config.name
            else:
                return value, ""
        else:
            return value

    def keys(self, *key, origin=False) -> Union[List[str], Tuple[List[str], str]]:
        node = self._keys.get(key)
        keys_list: List[str] = []
        if node is not None and isinstance(node.value, list):
            if node.type == "object" or not node.explicit:
                # Object-type keys hold a list of child key names, and
                # non-explicit nodes are union parent nodes, which also hold
                # a list of child key names. An explicit non-object node
                # holding a list is a leaf with an array value (e.g.
                # `ignore: [a, b]`), not a list of child key names.
                keys_list = list(node.value)
        if origin:
            return (keys_list, node.source.name if node is not None and node.source else "")
        return keys_list

    def getall(self, *key, origin=False):
        keys = self.keys(*key)
        if keys:
            for k in keys:
                yield from self.getall(*key, k, origin=origin)
        else:
            res = self.get(*key, origin=origin)
            res = res if origin else (res,)
            yield (key,) + res

    def dump(self, *names, fmt: KeyFormat = KeyFormat.cfg, file=sys.stdout):
        table = [("Origin", "Env", "Name", "Value")]
        sizes = [len(x) for x in table[0]]
        for key, val, origin in self.getall(origin=True):
            if names:
                for name in names:
                    it = enumerate(name.split("."))
                    if all(key[i].startswith(k) for i, k in it if k):
                        break
                else:
                    continue

            node = self._keys.get(key)
            env_str = ""
            if node is not None and node.env:
                env_str = node.env

            if fmt == KeyFormat.env:
                key_str = "SPINTA_" + "__".join(key).upper()
            else:
                key_str = ".".join(key)

            if isinstance(val, list):
                for i, v in enumerate(val):
                    row = (origin, env_str, key_str + f".{i}", v)
                    table.append(row)
                    sizes = [max(x) for x in zip(sizes, map(len, map(str, row)))]
            else:
                row = (origin, env_str, key_str, val)
                table.append(row)
                sizes = [max(x) for x in zip(sizes, map(len, map(str, row)))]

        table = table[:1] + [tuple(["-" * s for s in sizes])] + table[1:]
        if file:
            for row in table:
                print("  ".join([str(x).ljust(s) for x, s in zip(row, sizes)]), file=file)
        else:
            return table

    def to_dict(self, *names: str) -> Dict[str, Any]:
        result = {}
        for key, val in self.getall(*names):
            key = ".".join(key[len(names) :])
            result[key] = val
        return result

    def _rebuild(self) -> None:
        """Rebuild the whole configuration tree in `_keys` from scratch.

        Sources are processed in priority order (low to high), and after
        each source's base keys, the source's `environments.<env>.*` overlay
        keys are processed, where `<env>` is the active environment given by
        the base value of the `env` option. Overlay keys are overlaid on top
        of the base values in `_keys`, with the `environments.<env>` prefix
        dropped, so an overlay of source N beats the base values of source
        N, but loses to the base values of source N+1.

        Finally, schema (`spinta/config.yml`) structure and default values
        are added as non-explicit nodes, which are not registered in parent
        child-name lists, so `keys()` and `getall()` don't see them, but
        `get()` can find them.

        Within each (source, base-or-overlay) group keys are processed
        parents-first (sorted by length), which makes same-source
        reset+re-add deterministic.
        """
        self._keys = {}
        env = self._get_active_env()
        for config in self.sources:
            self._process_source(config)
            if env:
                self._process_source(config, env)
        self._add_schema_nodes()

    def _get_active_env(self) -> Optional[str]:
        # The active environment is the base value of the `env` option from
        # the highest priority source that sets it.
        for config in reversed(self.sources):
            value = config.get(("env",))
            if value is not NA:
                return value
        return None

    def _process_source(self, config: ConfigSource, env: Optional[str] = None) -> None:
        keys = list(config.keys(env))
        keys.sort(key=len)
        for key in keys:
            self._process_key(config, key, env)

    def _process_key(self, config: ConfigSource, key: Key, env: Optional[str]) -> None:
        raw = config.get(key, env)
        if raw is NA:
            return
        if len(key) == 0:
            return
        # Ensure all ancestors exist (union), parents-first makes
        # same-source reset+re-add deterministic.
        self._ensure_union((), key[0], config)
        for j in range(1, len(key)):
            self._ensure_union(key[:j], key[j], config)
        full_schema = _get_key_path_schema(self._schema, key)
        if full_schema is not None and full_schema.get("type") == "object":
            if isinstance(raw, str):
                # This should never happen, all configuration sources
                # must either parse comma separated values of
                # object-type keys into lists (see
                # `_parse_object_key_values`) or raise an error (see
                # `_check_keys`).
                raise Exception(
                    f"Invalid configuration value {raw!r} for key {'.'.join(key)!r} in {config.name} config: "
                    f"expected a mapping or a list of key names, but got a scalar value, "
                    f"use a list instead, e.g. {'.'.join(key)}: ['one']."
                )
            # Explicit replace: last source's explicit list wins;
            # higher (or same-source, processed later) leaves add back.
            self._keys[key] = KeyNode(type="object", source=config, env=env, value=list(raw), explicit=True)
        else:
            node_type = full_schema.get("type") if full_schema is not None else None
            self._keys[key] = KeyNode(type=node_type, source=config, env=env, value=raw, explicit=True)

    def _ensure_union(self, prefix: Key, child: str, config: ConfigSource) -> None:
        node = self._keys.get(prefix)
        if node is None:
            if len(prefix) == 0:
                node_type: str | None = "object"
            else:
                schema = _get_key_path_schema(self._schema, prefix)
                node_type = schema.get("type") if schema is not None else None
            self._keys[prefix] = KeyNode(type=node_type, source=config, env=None, value=[child])
        elif isinstance(node.value, list):
            if child not in node.value:
                node.value.append(child)
        else:
            # A former leaf scalar becomes a parent (scalar-tail dict
            # values, e.g. property `type` given as a dict). Keep its
            # type/source/env, but track children so `keys()`/`getall()`
            # can reconstruct the subtree.
            self._keys[prefix] = KeyNode(type=node.type, source=node.source, env=node.env, value=[child])

    def _add_schema_nodes(self) -> None:
        """Add schema structure and default values to `_keys`.

        Schema-derived nodes are not registered in parent child-name lists,
        so `keys()` and `getall()` don't see them, but `get()` can find
        object-type structure and default values of unset options.
        """
        # Static schema paths (e.g. `accesslog.buffer_size`).
        self._add_schema_nodes_for(self._schema, ())
        # Dynamic schema paths (e.g. `manifests.<name>.mode`), resolved for
        # subtrees set by configuration sources.
        for key in list(self._keys):
            schema = _get_key_path_schema(self._schema, key)
            if schema is not None:
                self._add_schema_nodes_for(schema, key, recurse=False)

    def _add_schema_nodes_for(self, schema: Schema, path: Key, recurse: bool = True) -> None:
        for name, sub in _iter_static_schema_items(schema):
            key = path + (name,)
            if sub.get("type") == "object":
                if key not in self._keys:
                    self._keys[key] = KeyNode(type="object", source=None, env=None, value=[])
                    if recurse:
                        self._add_schema_nodes_for(sub, key)
            elif key not in self._keys and "default" in sub:
                self._keys[key] = KeyNode(type=sub.get("type"), source=None, env=None, value=sub["default"])

    def _key_exists(self, key: Key) -> bool:
        # Check if `key` is present in the effective key structure.
        #
        # Values set on parent keys, like `SPINTA_BACKENDS=` or
        # `SPINTA_BACKENDS=one`, control the structure of the parent key
        # subtree. Keys removed from the subtree are not available, even if
        # a lower priority source still has values set for them.
        #
        # Auto-created parents are complete unions (contain every known leaf),
        # so they never hide; only explicit (subset) object parents hide.
        for i in range(1, len(key)):
            node = self._keys.get(key[:i])
            if node is not None and node.explicit and node.type == "object":
                if key[i] not in node.value:
                    return False
        return True

    def get_source_names(self) -> List[str]:
        return [source.name for source in self.sources]


def _iter_static_schema_items(schema: Schema):
    """Iterate statically known child items of an object-type schema node.

    Static items are declared under `items` and under `case` branches. Items
    of dynamic subtrees, declared with `keys`/`values` (e.g. `backends` can
    have any number of backends with arbitrary names), are not included,
    since their child names are not known statically.
    """
    if schema.get("type") == "object":
        yield from schema.get("items", {}).items()
        for items in schema.get("case", {}).values():
            yield from items.items()


def _get_key_schema(schema: Schema, key: str):
    if schema.get("type") == "object":
        if "items" in schema:
            if key in schema["items"]:
                return schema["items"][key]
        if "case" in schema:
            for items in schema["case"].values():
                if key in items:
                    return items[key]
        if "keys" in schema and schema["keys"]["type"] == "string":
            return schema["values"]


def _get_key_path_schema(schema: Schema, key: Key) -> Optional[Schema]:
    """Resolve a schema node for a full configuration key path."""
    for k in key:
        schema = _get_key_schema(schema, k)
        if schema is None:
            break
    return schema


def _check_keys(config: Dict[tuple, Any], schema: Schema, name: str):
    """Check that keys, which declare a list of child keys, don't have scalar
    values set.

    A fork is a python `dict`, so a list of child key names can be given
    directly as a `list`, for example `{'backends': ['one']}`, or as a mapping
    declaring the whole subtree, for example `{'backends': {'one': {...}}}`.
    Setting a scalar value, e.g. `{'backends': 'one'}`, is most likely a
    mistake, so an error is raised.
    """
    for key, value in config.items():
        node = schema
        for k in key:
            node = _get_key_schema(node, k)
            if node is None:
                # Skip unknown keys, only keys known to the schema can have
                # child keys.
                break
        else:
            if node.get("type") == "object" and not isinstance(value, (dict, list)):
                raise Exception(
                    f"Invalid configuration value {value!r} for key {'.'.join(key)!r} in {name} config: "
                    f"expected a mapping or a list of key names, but got a scalar value, "
                    f"use a list instead, e.g. {'.'.join(key)}: ['one']."
                )


def _parse_object_key_values(config: dict, schema: Schema) -> dict:
    """Parse values of object-type keys given as comma separated strings.

    Keys with `type: object` schema (e.g. `backends`) hold a list of child key
    names. Environment variables and command line arguments can only have
    scalar values, so such lists are given as comma separated strings, for
    example `SPINTA_BACKENDS=one,two`. This converts those strings to lists,
    so `RawConfig` always receives object key values as declared in
    `spinta/config.yml`, that is, as lists of child key names.

    An empty string is converted to an empty list, which resets the list of
    child keys. Keys unknown to the schema and keys declared as scalars are
    left as is.

    Keys can be full key paths given as tuples, like in a flattened config, or
    dotted key names, like in command line arguments, before they are split
    into key path tuples by `PyDict.read`.
    """
    result = {}
    for key, value in config.items():
        if isinstance(value, str):
            path = key if isinstance(key, tuple) else tuple(key.split("."))
            if path[:1] == ("environments",) and len(path) > 2:
                # Environment specific keys, like `("environments", "test",
                # "backends")`, don't map to the configuration schema
                # directly, their schema is the same as the schema of the key
                # without the environment prefix.
                node = _get_key_path_schema(schema, path[2:])
            else:
                node = _get_key_path_schema(schema, path)
            if node is not None and node.get("type") == "object":
                value = [v.strip() for v in value.split(",") if v.strip()]
        result[key] = value
    return result


def _traverse(value, path=()):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _traverse(v, path + (k,))
    else:
        yield path, value


def _get_inner_keys(config: Dict[tuple, Any], depth=1):
    """Get inner keys for config.

    `config` is flattened dict, that looks like this:

        {
            ('a', 'b', 'c'): 1,
            ('a', 'b', 'd'): 2,
        }

    Nested version of this would look like this:

        {
            'a': {
                'b': {
                    'c': 1,
                    'd': 1,
                }
            }
        }

    Then, the purpose of this function is to add keys to all inner nesting
    levels. For this example, function result will be:

        {
            ('a'): ['b'],
            ('a', 'b'): ['c', 'd'],
        }

    This is needed for `RawConfig` class, in order to be able to do things like
    this:

        config.keys('a', 'b')
        ['c', 'd']

    And this functionality is needed, because of environment variables. For
    example, in order to add a new backend, first you need to add new keys, like
    this:

        SPINTA_A=b,x

    And then, you can add values to it:

        SPINTA_A_X=3

    And the end configuration will look like this:

        {
            'a': {
                'b': {
                    'c': 1,
                    'd': 1,
                },
                'x': '3'
            }
        }

    """
    inner = collections.defaultdict(list)
    for key in config.keys():
        for i in range(depth, len(key)):
            k = tuple(key[:i])
            if key[i] not in inner[k]:
                inner[k].append(key[i])
    return inner


def _get_from_prefix(config: dict, prefix: tuple):
    for k, v in config.items():
        if k[: len(prefix)] == prefix:
            yield k[len(prefix) :], v


def _get_default_dir(name, default):
    path = os.environ.get(name, default)
    return pathlib.Path(path).expanduser() / "spinta"


DEFAULT_CONFIG_PATH = _get_default_dir("XDG_CONFIG_HOME", "~/.config")
DEFAULT_DATA_PATH = _get_default_dir("XDG_DATA_HOME", "~/.local/share")


class ResourceTuple(NamedTuple):
    # Resource type from config:components.backends
    type: str
    # Resource URI, depends on type.
    external: str
    # Resource prepare formula.
    prepare: Optional[str] = None


def parse_resource_args(
    resource_type: str,
    resource_source: str,
    formula: Optional[str] = None,
) -> Optional[ResourceTuple]:
    resource = ResourceTuple(resource_type, resource_source, formula)
    if resource.type is None and resource.external is None and not resource.prepare:
        return None
    return [resource]


def parse_manifest_path(
    rc: RawConfig,
    path: Union[str, ManifestPath, ResourceTuple],
) -> ManifestPath:
    from spinta.manifests.components import ManifestPath

    if isinstance(path, ManifestPath):
        return path
    from spinta.manifests.helpers import detect_manifest_from_path

    if isinstance(path, ResourceTuple):
        path = path.external
    Manifest_ = detect_manifest_from_path(rc, path)
    return ManifestPath(type=Manifest_.type, path=path)


def check_if_manifest_valid(rc: RawConfig, manifest: str):
    names = rc.keys("components", "manifests")
    return manifest in names


def _get_resource_config(
    rc: RawConfig,
    resource: ResourceTuple,
) -> Dict[str, str]:
    if resource.external and resource.external in rc.get("backends", default={}):
        return {
            "type": resource.type,
            "backend": resource.external,
            "prepare": resource.prepare,
        }
    else:
        return {
            "type": resource.type,
            "external": resource.external,
            "prepare": resource.prepare,
        }


def configure_rc(
    rc: RawConfig,
    manifests: List[Union[str, ManifestPath]] = None,
    *,
    mode: Mode = Mode.internal,
    check_names: Optional[bool] = None,
    backend_type: str | None = None,
    backend: str | None = None,
    resources: List[ResourceTuple] = None,
    dataset: str = None,
    manifest_type: str = "inline",
    ensure_backends=True,
) -> RawConfig:
    config: Dict[str, Any] = {}

    if backend:
        # TODO: Parse backend string to detect type. Currently type is hardcoded
        #       to 'postgresql'.
        if backend_type:
            config["backends.default"] = {
                "type": backend_type,
                "dsn": backend,
            }
        elif backend == "memory":
            config["backends.default"] = {
                "type": "memory",
            }
        else:
            config["backends.default"] = {
                "type": "postgresql",
                "dsn": backend,
            }
    elif "default" not in rc.get("backends", default={}):
        config["backends.default"] = {
            "type": "memory",
        }

    if not rc.get("keymaps", "default"):
        config["keymaps.default"] = {
            "type": "sqlalchemy",
            "dsn": "sqlite:///{data_dir}/keymap.db",
        }

    if manifests or resources:
        sync = []
        inline = []
        if dataset:
            config["given_dataset_name"] = dataset

        if resources:
            inline.append(
                {
                    "type": "dataset",
                    "name": "datasets/gov/example",
                    "resources": {
                        f"resource{i}": _get_resource_config(rc, resource) for i, resource in enumerate(resources, 1)
                    },
                }
            )

        if manifest_type != "inline":
            manifest = parse_manifest_path(rc, manifests[0])
            config["manifests.default"] = {
                "type": manifest_type,
                "backend": "default",
                "keymap": "default",
                "mode": mode.value,
                "path": manifest.path,
                "file": manifest.file,
                "manifest": inline,
            }
        else:
            if manifests:
                for i, path in enumerate(manifests):
                    manifest_name = f"manifest{i}"
                    manifest = parse_manifest_path(rc, path)
                    config[f"manifests.{manifest_name}"] = {
                        "type": manifest.type,
                        "path": manifest.path,
                        "file": manifest.file,
                        "prepare": manifest.prepare,
                    }
                    sync.append(manifest_name)

            config["manifests.default"] = {
                "type": manifest_type,
                "backend": "default",
                "keymap": "default",
                "mode": mode.value,
                "sync": sync,
                "manifest": inline,
            }
        config["manifest"] = "default"

        if check_names is not None:
            config["check.names"] = check_names

    config["ensure_backends"] = ensure_backends

    if config:
        rc = rc.fork(config)

    return rc
