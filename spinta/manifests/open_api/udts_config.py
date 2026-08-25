"""Static information for an UDTS data service OpenAPI specification.

Manifest does not hold environment hosts, service level `info` or the
authorization server address, so these are given in a separate YAML file::

    spinta udts oas manifest.csv -o at280.json \\
        --path datasets/gov/rc/jadis/at280/1 \\
        --udts-cfg vartai.yml

An example file is shipped as `udts_cfg.example.yml` next to this module.
"""

from __future__ import annotations

import pathlib
import warnings
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from spinta.exceptions import InvalidUdtsConfig

yaml = YAML(typ="safe")

KNOWN_KEYS = frozenset(["info", "servers", "auth", "externalDocs"])

#: Path of the token endpoint, as routed by the API gateway inside a data
#: service. See `UTILITY_PATHS` in `openapi_generator`.
TOKEN_PATH = "/:token"


@dataclass
class UdtsConfig:
    """Parsed `--udts-cfg` file."""

    info: dict[str, Any] = field(default_factory=dict)
    servers: list[dict[str, Any]] = field(default_factory=list)
    auth: dict[str, Any] = field(default_factory=dict)
    external_docs: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_path(cls, path: pathlib.Path | str) -> UdtsConfig:
        path = pathlib.Path(path)
        try:
            data = yaml.load(path.read_text(encoding="utf-8"))
        except OSError as error:
            raise InvalidUdtsConfig(path=str(path), error=f"can not be read, {error.strerror or error}.")
        except YAMLError as error:
            raise InvalidUdtsConfig(path=str(path), error=f"is not a valid YAML file, {error}.")

        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise InvalidUdtsConfig(path=str(path), error="configuration must be a mapping.")

        for key in sorted(set(data) - KNOWN_KEYS):
            warnings.warn(f"{path}: unknown UDTS configuration key {key!r}, ignoring it.", UserWarning)

        for key in ("info", "auth", "externalDocs"):
            if key in data and data[key] is not None and not isinstance(data[key], dict):
                raise InvalidUdtsConfig(path=str(path), error=f"`{key}` must be a mapping.")

        token_url = (data.get("auth") or {}).get("token_url")
        if token_url is not None:
            _check_url(token_url, path, "`auth.token_url`")

        servers = data.get("servers")
        if servers is None:
            servers = []
        if not isinstance(servers, list):
            raise InvalidUdtsConfig(path=str(path), error="`servers` must be a list.")
        for server in servers:
            _check_server(server, path)

        return cls(
            info=data.get("info") or {},
            servers=servers,
            auth=data.get("auth") or {},
            external_docs=data.get("externalDocs") or {},
        )

    def resolve_servers(self, service_path: str) -> list[dict[str, Any]]:
        """Build `servers` for a given data service.

        API gateway derives the API context path from the path part of the first
        server URL, so every server URL must end with the data service path and
        must not end with a slash. A URL given without a path gets the data
        service path appended, which keeps one configuration file usable for all
        data services of one agent.
        """
        if not self.servers:
            return [{"url": f"/{service_path}"}]

        servers = []
        for server in self.servers:
            server = dict(server)
            server["url"] = _resolve_server_url(server.get("url", ""), service_path)
            servers.append(server)
        return servers

    def resolve_token_url(self, servers: list[dict[str, Any]]) -> str:
        """Return the authorization server token endpoint.

        Defaults to the token endpoint of the first server, which is where an
        API gateway routes it inside a data service.
        """
        token_url = self.auth.get("token_url")
        if token_url:
            return token_url

        base = servers[0].get("url", "") if servers else ""
        return f"{base}{TOKEN_PATH}"


def _check_server(server: Any, path: pathlib.Path) -> None:
    if not isinstance(server, dict):
        raise InvalidUdtsConfig(
            path=str(path),
            error=f"every `servers` entry must be a mapping with an `url`, got {server!r}.",
        )

    url = server.get("url")
    if not isinstance(url, str) or not url:
        raise InvalidUdtsConfig(path=str(path), error=f"`servers` entry {server!r} has no `url`.")

    _check_url(url, path, "server URL")


def _check_url(url: Any, path: pathlib.Path, what: str) -> None:
    if not isinstance(url, str) or not url:
        raise InvalidUdtsConfig(path=str(path), error=f"{what} must be a non empty string, got {url!r}.")

    # OpenAPI allows a relative URL, but it has to start with a slash. Anything
    # else has to carry both a scheme and a host, otherwise `urlsplit` reads the
    # host as a path (`localhost:8080` is read as scheme `localhost`) and the
    # data service path is silently dropped.
    if not url.startswith("/"):
        parts = urlsplit(url)
        if not parts.scheme or not parts.netloc:
            raise InvalidUdtsConfig(
                path=str(path),
                error=(
                    f"{what} {url!r} has no scheme and host, "
                    "use `https://host.example.com` or a path starting with `/`."
                ),
            )


def _resolve_server_url(url: str, service_path: str) -> str:
    parts = urlsplit(url.rstrip("/"))
    path = parts.path

    if not path:
        return urlunsplit(parts._replace(path=f"/{service_path}"))

    if path.lstrip("/") != service_path:
        warnings.warn(
            f"Server URL {url!r} path does not match data service path {service_path!r}. "
            "Leaving it as given, API gateway will derive a different context path.",
            UserWarning,
        )

    return urlunsplit(parts)
