from typing import Any, Dict, List, Union

import collections
import logging
import pathlib

from ruamel.yaml import YAML

from spinta.utils.imports import importstr
from spinta.utils.schema import NA

yaml = YAML(typ='safe')

log = logging.getLogger(__name__)


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
            config.update(_traverse(v, k))
            config.update(_get_inner_keys(v, k))
        self.config = config

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
        self.config = dict(arg.split('=', 1) for arg in self.config)


class EnvVars(ConfigSource):
    name = 'env'

    def get(self, key: Union[tuple, str]):
        key = 'SPINTA_' + key
        return self.config.get(key, NA)


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

    def __init__(self):
        self._dirty = False
        self._backup = None
        self._sources = []

    def read(self, sources: List[ConfigSource]):
        for config in sources:
            log.info(f"Reading config from {config.name}.")
            config.read()
            self._sources.append(config)

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

    def keys(self, *key, **kwargs):
        kwargs.setdefault('default', [])
        return self.get(*key, cast=list, **kwargs)

    def getall(self, origin=False):
        keys = []
        for config in self.sources:
            for key in config.keys():
                if key not in keys:
                    keys.append()
        for key in sorted(keys):
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

    def _get_config_value(self, key: tuple, default, envvar, env=None):
        assert isinstance(key, tuple)

        for config in reversed(self.sources):
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


def _get_inner_keys(config: Dict[tuple, Any], path=()):
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
        for i in range(1, len(key)):
            k = path + tuple(key[:i])
            if key[i] not in inner[k]:
                inner[k].append(key[i])
    return inner


def _get_from_prefix(config: dict, prefix: tuple):
    for k, v in config.items():
        if k[:len(prefix)] == prefix:
            yield k[len(prefix):], v
