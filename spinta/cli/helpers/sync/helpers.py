from __future__ import annotations
from pathlib import Path
from io import StringIO
from typing import Optional, Union

from typer import echo, Context as TyperContext

from spinta.auth import DEFAULT_CREDENTIALS_SECTION
from spinta.cli.helpers.sync import ContentType
from spinta.cli.helpers.store import load_manifest
from spinta.cli.manifest import _read_and_return_manifest
from spinta.client import get_client_credentials, RemoteClientCredentials
from spinta.core.config import RawConfig
from spinta.core.context import configure_context
from spinta.core.enums import Mode, Access
from spinta.datasets.inspect.helpers import create_manifest_from_inspect, coalesce
from spinta.exceptions import ManifestFileInvalidPath, ManifestFilePathNotGiven
from spinta.manifests.components import Manifest, ManifestPath
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.components import Config, MetaData, Context
from spinta.exceptions import (
    NotImplementedFeature,
    InvalidCredentialsConfigurationException,
)
from spinta.formats.csv.commands import _render_manifest_csv
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.yaml.components import InlineManifest


EXCLUDED_RESOURCE_FIELDS = frozenset({"dataset", "models", "given", "lang"})


def prepare_context(context_object: Context) -> Context:
    context = configure_context(context_object, None, mode=Mode.external, backend=None)
    load_manifest(context, ensure_config_dir=True, full_load=True)
    return context


def load_configuration_values(context: Context) -> tuple[str, str]:
    raw_config: RawConfig = context.get("rc")

    data_source_name = raw_config.get("backends", "default", "dsn")
    manifest_path = raw_config.get("manifests", "default", "path")

    return data_source_name, manifest_path


def get_context_and_manifest(
    ctx: TyperContext,
    manifest: ManifestPath,
    resource: Optional[tuple[str, str]],
    formula: str,
    backend: Optional[str],
    auth: Optional[str],
    priority: str,
) -> tuple[Context, Manifest]:
    """Create a context and manifest for synchronization.

    This function inspects and builds a manifest from provided resources,
    formula, backend, and authentication settings. The `priority` determines
    whether manifest or external data should take precedence.
    """
    if priority not in ["manifest", "external"]:
        echo(
            f"Priority '{priority}' does not exist, there can only be 'manifest' or 'external', it will be set to default 'manifest'."
        )
        priority = "manifest"

    context, manifest = create_manifest_from_inspect(
        context=ctx.obj,
        manifest=manifest,
        resources=resource,
        formula=formula,
        backend=backend,
        auth=auth,
        priority=priority,
    )

    return context, manifest


def prepare_local_manifest_file(manifest_path: str) -> None:
    if not manifest_path:
        raise ManifestFilePathNotGiven

    manifest_file = Path(manifest_path)

    try:
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        if not manifest_file.exists():
            manifest_file.touch()
    except (OSError, ValueError, PermissionError):
        raise ManifestFileInvalidPath(manifest_path=manifest_path)


def get_configuration_credentials(context: Context) -> RemoteClientCredentials:
    """Retrieve remote client credentials from configuration."""
    config: Config = context.get("config")
    credentials: RemoteClientCredentials = get_client_credentials(config.credentials_file, DEFAULT_CREDENTIALS_SECTION)
    return credentials


def validate_credentials(credentials: RemoteClientCredentials) -> None:
    """Validates the credentials required for calls to the Catalog."""
    required = {
        "resource_server": credentials.resource_server,
        "server": credentials.server,
        "client": credentials.client,
        "organization_type": credentials.organization_type,
        "organization": credentials.organization,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise InvalidCredentialsConfigurationException(missing_credentials=", ".join(missing))


def get_agent_name(credentials: RemoteClientCredentials) -> str:
    """Retrieves the name of the agent from the client name set in the configuration file."""
    return credentials.client.rsplit("_", 1)[0]


def render_content_from_manifest(context: Context, manifest: InlineManifest, content_type: ContentType) -> bytes | str:
    rows = datasets_to_tabular(context, manifest)
    rows = ({c: row[c] for c in DATASET} for row in rows)

    rendered_rows = "".join(_render_manifest_csv(rows))
    if content_type == ContentType.CSV:
        return rendered_rows
    elif content_type == ContentType.BYTES:
        return rendered_rows.encode("utf-8")
    else:
        raise NotImplementedFeature()


def read_and_get_manifest(
    context: Context,
    manifest_type: str = "tabular",
    path: str | None = None,
    contents: Union[str, list[str]] | None = None,
):
    manifest_paths = []

    if path:
        manifest_paths.append(ManifestPath(type=manifest_type, path=path, file=None))
    if contents:
        if isinstance(contents, str):
            contents = [contents]
        manifest_paths.extend(
            ManifestPath(type=manifest_type, path=None, file=StringIO(content)) for content in contents
        )

    if not manifest_paths:
        raise ManifestFilePathNotGiven

    return _read_and_return_manifest(
        context,
        manifest_paths,
        external=True,
        access=Access.private,
        format_names=False,
        order_by=None,
        rename_duplicates=False,
        verbose=False,
    )


def merge_manifest_attributes(target: MetaData, source: MetaData, fields: set[str]) -> None:
    for field in fields:
        setattr(target, field, coalesce(getattr(source, field, None), getattr(target, field, None)))


def get_nested_attribute(entity: MetaData, attribute_path: str) -> str | None:
    """Retrieve an attribute from an object, supporting dotted paths.

    This function safely traverses nested attributes using a dot-separated path.
    For example,
      - "id" returns `entity.id`
      - "external.name" returns `entity.external.name` if both exist
      - Returns None if any intermediate attribute is missing or None.
    """
    if not attribute_path:
        return entity

    current = entity
    for part in attribute_path.split("."):
        if current is None:
            return None
        current = getattr(current, part, None)
    return current


def find_existing_entity(
    entity: MetaData, candidates: dict[str, MetaData], keys: tuple[str, str, str] = ("id", "name", "source")
) -> Union[tuple[MetaData, str], tuple[None, None]]:
    for candidate_name, candidate in candidates.items():
        for key in keys:
            entity_value = get_nested_attribute(entity, key)
            candidate_value = get_nested_attribute(candidate, key)
            if entity_value and entity_value == candidate_value:
                return candidate, candidate_name
    return None, None
