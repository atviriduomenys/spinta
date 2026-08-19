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
        data = yaml.load(path.read_text()) or {}

        if not isinstance(data, dict):
            raise ValueError(f"{path}: UDTS configuration must be a mapping.")

        for key in sorted(set(data) - KNOWN_KEYS):
            warnings.warn(f"{path}: unknown UDTS configuration key {key!r}, ignoring it.", UserWarning)

        return cls(
            info=data.get("info") or {},
            servers=data.get("servers") or [],
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
