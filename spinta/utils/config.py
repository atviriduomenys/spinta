import pathlib
from typing import Union

import cachetools
from ruamel.yaml import YAML

from spinta.components import Config
from spinta.core.config import RawConfig, DEFAULT_CONFIG_PATH


def asbool(s: Union[str, bool]) -> bool:
    if isinstance(s, bool):
        return s
    s = s.lower()
    if s in ("true", "1", "on", "yes"):
        return True
    if s in ("false", "0", "off", "no", ""):
        return False
    raise ValueError(f"Expected a boolean value, got {s!r}.")


# Returns config path from rc or DEFAULT_CONFIG_PATH
def get_config_path(rc: RawConfig) -> pathlib.Path:
    if rc is None:
        return pathlib.Path(DEFAULT_CONFIG_PATH)

    return pathlib.Path(rc.get("config_path") or DEFAULT_CONFIG_PATH)


# Returns id path (used for storing client files)
def get_id_path(path: pathlib.Path) -> pathlib.Path:
    return path / "id"


# Returns helpers path (used for storing auth helper files)
def get_helpers_path(path: pathlib.Path) -> pathlib.Path:
    return path / "helpers"


# Returns keymap.yml path, stored in helpers folder
def get_keymap_path(path: pathlib.Path):
    return get_helpers_path(path) / "keymap.yml"


# Returns clients folder, used for auth functionality.
def get_clients_path(path: Union[Config, pathlib.Path]):
    if isinstance(path, pathlib.Path):
        return path / "clients"
    return path.config_path / "clients"


def get_limit_path(config: Config) -> pathlib.Path:
    return config.config_path / "limits.yml"


@cachetools.cached(cachetools.TTLCache(maxsize=1, ttl=30))
def get_limit_config_data(config: Config) -> dict:
    path = get_limit_path(config)
    if not path.exists():
        return {}

    yml = YAML()
    data = yml.load(path)
    return data
