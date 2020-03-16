from typing import Any, Dict, List, Union, Optional

import collections
import logging
import os
import pathlib

from ruamel.yaml import YAML

from spinta.utils.imports import importstr
from spinta.utils.schema import NA

yaml = YAML(typ='safe')

log = logging.getLogger(__name__)


def read_config(args=None):
    rc = RawConfig()
    rc.read([
        Path('spinta', 'spinta.config:CONFIG'),
        EnvFile('envfile', '.env'),
        EnvVars('envvars', os.environ),
        CliArgs('cliargs', args or []),
    ])

    # Inject extenstion provided defaults
    configs = rc.get('config', cast=list, default=[])
    if configs:
        rc.read([Path(c, c) for c in configs], after='spinta')

    return rc


class ConfigSource:

    def __init__(self, name=None, config=None):
        self.name = self.getname(name)
        self.config = config

    def __str__(self):
        return self._name

    def getname(self, name):
        return name or self.name or type(self).__name__

    def read(self):
        pass


class PyDict(ConfigSource):

    def read(self):
        config = {}
        for k, v in self.config.items():
            k = tuple(k.split('.'))
            v = dict(_traverse(v, k))
            v.update(_get_inner_keys(v, depth=len(k)))
            config.update(v)
        self.config = config

    def keys(self):
        yield from self.config.keys()

    def get(self, key: tuple):
        return self.config.get(key, NA)


class Path(PyDict):

    def read(self):
        if self.config.endswith(('.yml', '.yaml')):
            path = pathlib.Path(self.config)
            self.config = yaml.load(path.read_text())
        else:
            self.config = importstr(self.config)
        super().read()


class CliArgs(PyDict):
    name = 'cli'

    def read(self):
        config = {}
        for arg in self.config:
            key, val = arg.split('=', 1)
            if ',' in val:
                val = [v.strip() for v in val.split(',')]
            config[key] = val
        self.config = config
        super().read()


class EnvVars(ConfigSource):
    name = 'env'

    def keys(self):
        return []

    def get(self, key: Union[tuple, str]):
        if len(key) > 1 and key[0] == 'environments':
            key = key[1:]
        key = 'SPINTA_' + '_'.join(key).upper()
        val = self.config.get(key, NA)
        if isinstance(val, str) and ',' in val:
            return [v.strip() for v in val.split(',')]
        return val


class EnvFile(EnvVars):

    def read(self):
        config = {}
        path = pathlib.Path(self.config)
        if path.exists():
            with path.open() as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    line = line.strip()
                    if line == '':
                        continue
                    if '=' not in line:
                        continue
                    name, value = line.split('=', 1)
                    config[name] = value
        self.config = config


class RawConfig:

    def __init__(self, sources=None):
        self._locked = False
        self._sources = sources or []

    def read(
        self,
        sources: List[ConfigSource],
        after: Optional[int] = None,
    ):
        if self._locked:
            raise Exception(
                "Configuration is locked, use `rc.fork()` if you need to "
                "change configuration."
            )

        for config in sources:
            log.info(f"Reading config from {config.name}.")
            config.read()

        if after is not None:
            pos = (i for i, s in enumerate(self._sources) if s.name == after)
            pos = next(pos, None)
            if pos is None:
                raise Exception(f"Given after value {after!r} does not exist.")
            pos += 1
            self._sources[pos:pos] = sources
        else:
            self._sources.extend(sources)

    def add(self, name, params):
        self.read([PyDict(name, params)])
        return self

    def fork(self, sources=None, after=None):
        rc = RawConfig(list(self._sources))
        if sources:
            rc.read(sources, after)
        return rc

    def lock(self):
        self._locked = True

    def get(self, *key, default=None, cast=None, env=True, required=False, exists=False, origin=False):
        envname, _ = self._get_config_value(('env',), None, True)
        value, config = self._get_config_value(key, default, env, env=envname)

        if cast is not None:
            if cast is list and isinstance(value, str):
                value = value.split(',') if value else []
            elif value is not None:
                value = cast(value)

        if required and not value:
            name = '.'.join(key)
            raise Exception(f"{name!r} is a required configuration option.")

        if exists and isinstance(value, pathlib.Path) and not value.exists():
            name = '.'.join(key)
            raise Exception(f"{name} ({value}) path does not exist.")

        if origin:
            return value, config.name
        else:
            return value

    def keys(self, *key):
        if key == ():
            return [key[0] for key in self._all_keys() if len(key) == 1]
        else:
            return self.get(*key, cast=list, default=[])

    def getall(self, origin=False):
        for key in sorted(self._all_keys()):
            if origin:
                value, origin = self.get(*key, origin=True)
                yield key, value, origin
            else:
                yield key, self.get(*key)

    def dump(self, names: List[str] = None):
        table = [('Origin', 'Name', 'Value')]
        sizes = [len(x) for x in table[0]]
        for key, value, origin in self.getall(origin=True):
            if names:
                for name in names:
                    if '.'.join(key).startswith(name):
                        break
                else:
                    continue
            *pkey, key = key
            key = len(pkey) * '  ' + key
            if isinstance(value, list):
                value = ' | '.join(value)

            row = (origin, key, value)
            table.append(row)
            sizes = [max(x) for x in zip(sizes, map(len, map(str, row)))]

        table = (
            table[:1] +
            [['-' * s for s in sizes]] +
            table[1:]
        )
        for row in table:
            print('  '.join([str(x).ljust(s) for x, s in zip(row, sizes)]))

    def _all_keys(self):
        seen = set()
        for config in self._sources:
            for key in config.keys():
                if key not in seen:
                    seen.add(key)
                    yield key

    def _get_config_value(self, key: tuple, default, envvar, env=None):
        assert isinstance(key, tuple)

        for config in reversed(self._sources):
            if env:
                value = config.get(('environments', env) + key)
                if value is not NA:
                    return value, config
            value = config.get(key)
            if value is not NA:
                return value, config

        return default, None


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
        if k[:len(prefix)] == prefix:
            yield k[len(prefix):], v
