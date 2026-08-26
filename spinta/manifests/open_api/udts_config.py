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
import re
import warnings
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from spinta.exceptions import InvalidUdtsConfig

yaml = YAML(typ="safe")

KNOWN_KEYS = frozenset(["info", "servers", "auth", "externalDocs"])

#: Fields of the OpenAPI objects the configuration is copied into, plus the
#: fields of `auth`, which is ours. Everything else, apart from `x-` extensions,
#: is a typo or a field this configuration does not support and is left out.
#: Server variables are left out as well, because an environment is described by
#: an URL of its own, not by a template.
INFO_KEYS = frozenset(["title", "summary", "description", "termsOfService", "contact", "license", "version"])
CONTACT_KEYS = frozenset(["name", "url", "email"])
LICENSE_KEYS = frozenset(["name", "identifier", "url"])
SERVER_KEYS = frozenset(["url", "description"])
EXTERNAL_DOCS_KEYS = frozenset(["description", "url"])
AUTH_KEYS = frozenset(["token_url"])

#: A percent sign not starting an escape of two hexadecimal digits, RFC 3986.
malformed_escape_re = re.compile("%(?![0-9A-Fa-f]{2})")

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
        except UnicodeDecodeError as error:
            raise InvalidUdtsConfig(path=str(path), error=f"is not an UTF-8 file, {error}.")
        except YAMLError as error:
            raise InvalidUdtsConfig(path=str(path), error=f"is not a valid YAML file, {error}.")

        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise InvalidUdtsConfig(path=str(path), error="configuration must be a mapping.")

        for key in sorted(set(data) - KNOWN_KEYS, key=str):
            warnings.warn(f"{path}: unknown UDTS configuration key {key!r}, ignoring it.", UserWarning)

        for key in ("info", "auth", "externalDocs"):
            if key in data and data[key] is not None and not isinstance(data[key], dict):
                raise InvalidUdtsConfig(path=str(path), error=f"`{key}` must be a mapping.")

        _check_info(data.get("info") or {}, path)
        _check_external_docs(data.get("externalDocs") or {}, path)

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

        _warn_on_relative_token_url(data, servers, path)

        return cls(
            info=_clean_info(data.get("info") or {}, path),
            servers=[_keep_known(server, SERVER_KEYS, path, "`servers` entry") for server in servers],
            auth=_keep_known(data.get("auth") or {}, AUTH_KEYS, path, "`auth`"),
            external_docs=_keep_known(data.get("externalDocs") or {}, EXTERNAL_DOCS_KEYS, path, "`externalDocs`"),
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

        # A server URL may carry a query or a fragment, so the token path is
        # added to its path, not to the end of the whole URL.
        base = urlsplit(servers[0].get("url", "") if servers else "")
        return urlunsplit(base._replace(path=f"{base.path}{TOKEN_PATH}"))


def _check_server(server: Any, path: pathlib.Path) -> None:
    if not isinstance(server, dict):
        raise InvalidUdtsConfig(
            path=str(path),
            error=f"every `servers` entry must be a mapping with an `url`, got {server!r}.",
        )

    url = server.get("url")
    if not isinstance(url, str) or not url:
        raise InvalidUdtsConfig(path=str(path), error=f"`servers` entry {server!r} has no `url`.")

    if "{" in url or "}" in url:
        raise InvalidUdtsConfig(
            path=str(path),
            error=(
                f"server URL {url!r} is a template, which is not supported, give each environment an URL of its own."
            ),
        )

    _check_url(url, path, "server URL", relative=True)
    _check_optional_string(server.get("description"), path, f"`description` of server {url!r}")


def _warn_on_relative_token_url(data: dict, servers: list, path: pathlib.Path) -> None:
    """Warn when the token URL can only be derived as a relative one.

    It is derived from the first server, while the OpenAPI schema types it as
    an absolute `uri`.
    """
    if (data.get("auth") or {}).get("token_url") or not servers:
        return

    if not urlsplit(servers[0].get("url", "")).netloc:
        warnings.warn(
            f"{path}: no server with a host and no `auth.token_url`, so the token endpoint of "
            "`components.securitySchemes` is left relative, while OpenAPI expects an absolute URL.",
            UserWarning,
        )


def _keep_known(mapping: dict, known: frozenset[str], path: pathlib.Path, what: str) -> dict:
    """Leave out fields OpenAPI does not define, keeping `x-` extensions.

    Such a field is usually a typo, which would both make the document invalid
    and silently leave the intended field unset.
    """
    kept = {}
    for key, value in mapping.items():
        if isinstance(key, str) and (key in known or key.startswith("x-")):
            kept[key] = value
        else:
            warnings.warn(f"{path}: {what} key {key!r} is not supported, leaving it out.", UserWarning)
    return kept


def _clean_info(info: dict, path: pathlib.Path) -> dict:
    info = _keep_known(info, INFO_KEYS, path, "`info`")

    if "contact" in info:
        info["contact"] = _keep_known(info["contact"], CONTACT_KEYS, path, "`info.contact`")
    if "license" in info:
        info["license"] = _keep_known(info["license"], LICENSE_KEYS, path, "`info.license`")

    return info


def _check_string(value: Any, path: pathlib.Path, what: str) -> None:
    if not isinstance(value, str) or not value:
        raise InvalidUdtsConfig(path=str(path), error=f"{what} must be a non empty string, got {value!r}.")


def _check_optional_string(value: Any, path: pathlib.Path, what: str) -> None:
    if value is not None and not isinstance(value, str):
        raise InvalidUdtsConfig(path=str(path), error=f"{what} must be a string, got {value!r}.")


def _check_info(info: dict, path: pathlib.Path) -> None:
    """`info` is emitted as given, so its values have to be of OpenAPI types."""
    for key in ("title", "version", "summary", "description"):
        _check_optional_string(info.get(key), path, f"`info.{key}`")

    # OpenAPI Info Object requires it to be an URL.
    if info.get("termsOfService") is not None:
        _check_url(info["termsOfService"], path, "`info.termsOfService`")

    for key in ("contact", "license"):
        value = info.get(key)
        if value is not None and not isinstance(value, dict):
            raise InvalidUdtsConfig(path=str(path), error=f"`info.{key}` must be a mapping, got {value!r}.")

    contact = info.get("contact") or {}
    for key in ("name", "email"):
        _check_optional_string(contact.get(key), path, f"`info.contact.{key}`")
    if contact.get("url") is not None:
        _check_url(contact["url"], path, "`info.contact.url`")

    license_ = info.get("license")
    if license_ is not None:
        # OpenAPI License Object requires a name.
        _check_string(license_.get("name"), path, "`info.license.name`")
        _check_optional_string(license_.get("identifier"), path, "`info.license.identifier`")
        if license_.get("identifier") is not None and license_.get("url") is not None:
            raise InvalidUdtsConfig(
                path=str(path),
                error="`info.license` can have either an `identifier` or an `url`, not both.",
            )
        if license_.get("url") is not None:
            _check_url(license_["url"], path, "`info.license.url`")


def _check_external_docs(external_docs: dict, path: pathlib.Path) -> None:
    if not external_docs:
        return

    _check_url(external_docs.get("url"), path, "`externalDocs.url`")

    _check_optional_string(external_docs.get("description"), path, "`externalDocs.description`")


def _check_url(url: Any, path: pathlib.Path, what: str, *, relative: bool = False) -> None:
    _check_string(url, path, what)

    # `urlsplit` parses an URL, it does not validate one.
    if any(character.isspace() for character in url):
        raise InvalidUdtsConfig(path=str(path), error=f"{what} {url!r} is not a valid URL, it holds whitespace.")

    if malformed_escape_re.search(url):
        raise InvalidUdtsConfig(
            path=str(path),
            error=f"{what} {url!r} is not a valid URL, a percent sign has to start an escape of two hex digits.",
        )

    try:
        parts = urlsplit(url)
        parts.port  # noqa: B018  Port is parsed only when it is read.
    except ValueError as error:
        raise InvalidUdtsConfig(path=str(path), error=f"{what} {url!r} is not a valid URL, {error}.")

    if parts.scheme and parts.netloc:
        return

    # A server URL is a `uri-reference` in the OpenAPI schema, so it can be
    # relative, but it has to start with a slash. Everything else is an `uri`
    # there and has to be absolute. Without a scheme `urlsplit` reads the host
    # as a path (`localhost:8080` is read as scheme `localhost`), which would
    # silently drop the data service path.
    if relative and url.startswith("/"):
        return

    hint = "use `https://host.example.com`"
    if relative:
        hint += " or a path starting with `/`"
    raise InvalidUdtsConfig(path=str(path), error=f"{what} {url!r} has no scheme and host, {hint}.")


def _resolve_server_url(url: str, service_path: str) -> str:
    # A trailing slash is removed from the path, not from the whole URL, which
    # can end with a query string or a fragment. An API gateway takes the API
    # context path from this path, and falls back to the API title when it is
    # left empty by a trailing slash.
    parts = urlsplit(url)
    path = parts.path.rstrip("/")

    if not path:
        return urlunsplit(parts._replace(path=f"/{service_path}"))

    if path != f"/{service_path}":
        warnings.warn(
            f"Server URL {url!r} path does not match data service path {service_path!r}. "
            "Leaving it as given, API gateway will derive a different context path.",
            UserWarning,
        )

    return urlunsplit(parts._replace(path=path))
