from __future__ import annotations

import collections
import enum
import logging
import os
import pathlib
import sys
import typing
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple, Union

from ruamel.yaml import YAML

from spinta.core.enums import Mode
from spinta.utils.imports import importstr
from spinta.utils.path import resource_filename
from spinta.utils.schema import NA

if typing.TYPE_CHECKING:
    from spinta.manifests.components import ManifestPath

Schema = Dict[str, Any]
Key = Tuple[str]


class InnerKeys(list):
    """Child key names derived from the structure of a merge source.

    In contrast to explicitly set values (e.g. a comma separated list of names
    set via `SPINTA_BACKENDS=one,two`), these child key names don't replace
    child key names from lower priority sources, but are added to them, and
    they don't make the key explicitly set (see `RawConfig._key_exists`).
    """


yaml = YAML(typ="safe")

log = logging.getLogger(__name__)

SCHEMA = {
    "type": "object",
    "items": yaml.load(resource_filename("spinta", "config.yml").read_text()),
}


def read_config(args=None, envfile=None):
    rc = RawConfig()
    rc.read(
        [
            Path("spinta", "spinta.config:CONFIG"),
            EnvFile("envfile", envfile or ".env"),
            EnvVars("envvars", os.environ),
            CliArgs("cliargs", args or []),
        ]
    )

    # Inject extension provided defaults.
    #
    # `config` option can be set in any source (even in cliargs), so we read
    # all sources first and only then load additional config files. They are
    # inserted right after `spinta`, so the effective order of sources is:
    #
    #     spinta -> config files -> envfile -> envvars -> cliargs
    #
    # Each subsequent source overrides values from all previous sources.
    configs = rc.get("config", cast=list, default=[])
    if configs:
        rc.read([Path(c, c) for c in configs], after="spinta")

    return rc


class KeyFormat(str, enum.Enum):
    cfg = "cfg"
    cli = "cli"
    env = "env"


class ConfigSource:
    name: str

    # When `merge` is True (see `ForkConfig`), child key names read from this
    # source are added to child key names from lower priority sources instead
    # of replacing them, and are not marked as explicitly set.
    merge = False

    def __init__(self, name=None, config=None):
        self.name = self.getname(name)
        self.config = config

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
            if self.merge:
                # Mark child key names derived from this source structure, so
                # they could be distinguished from explicitly set values.
                inner = {key: InnerKeys(names) for key, names in inner.items()}
            v.update(inner)
            config.update(v)
        self.config = config

    def keys(self, env: str = None):
        if env:
            for key in self.config:
                if key[:2] == ("environments", env):
                    yield key[2:]
        else:
            for key in self.config:
                if key[:1] != ("environments",):
                    yield key

    def get(self, key: tuple, env: str = None):
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


class ForkConfig(PyDict):
    """Configuration source created by `RawConfig.fork`.

    Unlike regular configuration sources, which declare the full structure of
    their subtrees and replace child key names declared by lower priority
    sources, a fork often contains only a partial configuration, e.g. only
    `keymaps.default.type` without `keymaps.default.dsn`. Child key names from
    this source are added to child key names from lower priority sources, so
    such partial forks don't shadow the rest of the configuration.
    """

    merge = True


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

    def __init__(self, sources: Optional[ConfigSource] = None):
        self._locked = False
        self.sources = sources or []
        self._keys: Dict[Tuple[str], Tuple[int, List[str]]] = {}
        self._explicit_keys: Set[Tuple[str]] = set()
        self._schema = SCHEMA

    def read(
        self,
        sources: List[ConfigSource],
        after: Optional[str] = None,
    ):
        if self._locked:
            raise Exception("Configuration is locked, use `rc.fork()` if you need to change configuration.")

        for config in sources:
            log.info(f"Reading config from {config.name}.")
            config.read(self._schema)

        if after is not None:
            pos = (i for i, s in enumerate(self.sources) if s.name == after)
            pos = next(pos, None)
            if pos is None:
                raise Exception(f"Given after value {after!r} does not exist.")
            pos += 1
            self.sources[pos:pos] = sources
        else:
            self.sources.extend(sources)

        self._keys = self._update_keys()

    def add(self, name, params):
        self.read([PyDict(name, params)])
        return self

    def fork(self, sources=None, after=None) -> RawConfig:
        rc = RawConfig(list(self.sources))
        if sources:
            if isinstance(sources, dict):
                rc.read([ForkConfig("fork", sources)], after)
            else:
                rc.read(sources, after)
        else:
            rc._keys = rc._update_keys()
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
        env, _ = self._get_config_value(("env",), default=None)
        if self._key_exists(key):
            value, config = self._get_config_value(key, default, env)
            schema = _get_key_path_schema(self._schema, key)
            if schema is not None and schema.get("type") == "object":
                # The schema declares this key as an object (e.g. `backends`
                # is a mapping of backend names to backend configs), so its
                # value is the list of child key names, and a reset value,
                # like an empty string, is returned as an empty list.
                value = self.keys(*key)
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
        config, keys = self._keys.get(key, (None, []))
        return (keys, config.name) if origin else keys

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
        table = [("Origin", "Name", "Value")]
        sizes = [len(x) for x in table[0]]
        for key, val, origin in self.getall(origin=True):
            if names:
                for name in names:
                    it = enumerate(name.split("."))
                    if all(key[i].startswith(k) for i, k in it if k):
                        break
                else:
                    continue

            if fmt == KeyFormat.env:
                key = "SPINTA_" + "__".join(key).upper()
            else:
                key = ".".join(key)

            if isinstance(val, list):
                for i, v in enumerate(val):
                    row = (origin, key + f".{i}", v)
                    table.append(row)
                    sizes = [max(x) for x in zip(sizes, map(len, map(str, row)))]
            else:
                row = (origin, key, val)
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

    def _update_keys(self) -> Dict[Key, List[str]]:
        """Update inner keys respecting already set values."""
        keys = {}
        explicit = set()
        env, _ = self._get_config_value(("env",), default=None)
        for config in self.sources:
            self._update_config_keys(keys, explicit, config, config.keys())
            if env:
                self._update_config_keys(keys, explicit, config, config.keys(env), env)
        self._explicit_keys = explicit
        return keys

    def _update_config_keys(self, keys, explicit, config, ckeys, env=None):
        # Update `keys` in place.
        if () not in keys:
            keys[()] = config, []
        for key in ckeys:
            if key and key[0] not in keys[()][1]:
                keys[()][1].append(key[0])
            n = len(key)
            schema = self._schema
            for i in range(1, n + 1):
                schema = _get_key_schema(schema, key[i - 1])
                if schema is None:
                    # Skip unknown keys, only keys known to the schema can
                    # have child keys.
                    break
                if schema["type"] != "object":
                    # The schema declares this key as a scalar, but configuration
                    # may still use it as a nested object (e.g. a property
                    # `type` given as `{"name": ..., ...}`). Allow one more level
                    # of child collection so such dict values can be reconstructed,
                    # but do not mark the key as explicitly set based on these
                    # derived children.
                    if i < n:
                        k = tuple(key[:i])
                        if k not in keys:
                            keys[k] = config, []
                        if key[i] not in keys[k][1]:
                            keys[k][1].append(key[i])
                    break
                k = tuple(key[:i])
                v = config.get(k, env)
                if v is not NA:
                    if isinstance(v, str):
                        # This should never happen, all configuration sources
                        # must either parse comma separated values of
                        # object-type keys into lists (see
                        # `_parse_object_key_values`) or raise an error (see
                        # `_check_keys`).
                        raise Exception(
                            f"Invalid configuration value {v!r} for key {'.'.join(k)!r} in {config.name} config: "
                            f"expected a mapping or a list of key names, but got a scalar value, "
                            f"use a list instead, e.g. {'.'.join(k)}: ['one']."
                        )
                    elif isinstance(v, InnerKeys):
                        # Child key names derived from the structure of a merge
                        # source (see `ForkConfig`). Add them to child key names
                        # already declared by lower priority sources instead of
                        # replacing them, so a partial forked subtree would not
                        # shadow the rest of the configuration.
                        if k in keys:
                            existing = keys[k][1]
                            keys[k] = config, existing + [c for c in v if c not in existing]
                        else:
                            keys[k] = config, list(v)
                    else:
                        # Source has explicit value set. Empty values reset the
                        # current key list, but nested keys are still added back.
                        keys[k] = config, list(v)
                        explicit.add(k)
                if i < n:
                    # Collect all parent keys.
                    if k not in keys:
                        keys[k] = config, []
                    if key[i] not in keys[k][1]:
                        keys[k][1].append(key[i])

    def _get_config_value(self, key: Key, default: Any = NA, env: str = None):
        assert isinstance(key, tuple)
        for config in reversed(self.sources):
            val = NA
            if env:
                val = config.get(key, env)
            if val is NA:
                val = config.get(key)
            if val is not NA:
                return val, config
        if default is NA:
            schema = _get_key_path_schema(self._schema, key)
            if schema is not None:
                default = schema.get("default", NA)
        return default, None

    def _key_exists(self, key: Key) -> bool:
        # Check if `key` is present in the effective key structure.
        #
        # Values set on parent keys, like `SPINTA_BACKENDS=` or
        # `SPINTA_BACKENDS=one`, control the structure of the parent key
        # subtree. Keys removed from the subtree are not available, even if
        # a lower priority source still has values set for them.
        for i in range(1, len(key)):
            prefix = key[:i]
            if prefix not in self._explicit_keys:
                continue
            node = self._keys.get(prefix)
            if node is not None and key[i] not in node[1]:
                return False
        return True

    def get_source_names(self) -> List[str]:
        return [source.name for source in self.sources]


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
