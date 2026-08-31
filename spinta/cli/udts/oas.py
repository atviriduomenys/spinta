from __future__ import annotations

import pathlib
from typing import List, Optional

from typer import Argument, Option, echo
from typer import Context as TyperContext

from spinta import commands
from spinta.cli.helpers.manifest import convert_str_to_manifest_path
from spinta.cli.helpers.message import cli_error, cli_message
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context
from spinta.core.context import configure_context
from spinta.exceptions import InvalidUdtsConfig
from spinta.manifests.open_api.helpers import create_openapi_manifest, write_openapi_manifest
from spinta.manifests.open_api.service import datasets_under_service, find_services, is_service_level_path
from spinta.manifests.open_api.udts_config import UdtsConfig


def oas(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help="Manifest files to load"),
    output: Optional[str] = Option(
        None, "-o", "--output", help="Write specification to a given file, `.json` or `.yaml`"
    ),
    path: Optional[str] = Option(None, "--path", help="Data service path, for example `datasets/gov/rc/jadis/at280/1`"),
    udts_cfg: Optional[pathlib.Path] = Option(
        None, "--udts-cfg", help="YAML file with environments, `info` and authorization server"
    ),
    api_version: Optional[str] = Option(None, "--api-version", help="Value of `info.version`"),
    list_services: bool = Option(False, "--list", help="List data services found in a manifest and exit"),
):
    """Export OpenAPI specification of one UDTS data service

    Specification covers all datasets of the given data service and is meant to
    be used both for importing endpoints into an API gateway and for validating
    requests and responses against it.
    """
    context: Context = ctx.obj
    manifests = convert_str_to_manifest_path(manifests)
    # No data is read, so external resource backends do not have to be usable.
    context = configure_context(context, manifests, ensure_backends=False)
    store = load_manifest(context, load_internal=False, full_load=True, check_config=False, verbose=False)
    manifest = store.manifest

    dataset_names = list(commands.get_datasets(context, manifest))
    services = find_services(dataset_names)

    if list_services:
        _echo_services(services)
        return

    service_path = _resolve_service_path(path, services, dataset_names)

    try:
        config = UdtsConfig.from_path(udts_cfg) if udts_cfg else UdtsConfig()
    except InvalidUdtsConfig as error:
        cli_error(str(error))
    if udts_cfg and not config.auth.get("token_url"):
        cli_message(f"{udts_cfg}: no `auth.token_url` given, deriving it from the first server URL.")

    spinta_config = context.get("config")

    def scope_name(node, action):
        # Runtime authorization passes the UDTS flag positionally. Do the same
        # here so configured formatters may rename it or make it positional-only.
        return spinta_config.scope_formatter(context, node, action, True)

    spec = create_openapi_manifest(
        manifest,
        service_path=service_path,
        config=config,
        api_version=api_version,
        # Scopes are built by the formatter Spinta authorizes with, see
        # `spinta.auth.authorized`, so a deployment replacing it is followed.
        scope_name=scope_name,
    )
    write_openapi_manifest(spec, output)


def _echo_services(services: dict[str, list[str]]) -> None:
    if not services:
        cli_error("Manifest has no datasets belonging to a data service.")

    for service, dataset_names in services.items():
        echo(service)
        for name in dataset_names:
            echo(f"  {name}")


def _resolve_service_path(path: str | None, services: dict[str, list[str]], dataset_names: list[str]) -> str:
    if path is None:
        if not services:
            cli_error("Manifest has no datasets belonging to a data service, give one with `--path`.")
        if len(services) > 1:
            cli_error(
                "Manifest contains more than one data service, choose one with `--path`:\n"
                + "".join(f"  {service}\n" for service in services)
            )
        return next(iter(services))

    path = path.strip("/")

    # A path of another shape is not an error, it just is not what UDTS
    # describes, so datasets under it are still exported.
    if not is_service_level_path(path):
        cli_message(
            f"Given path {path!r} is not an UDTS data service path "
            "(`datasets/{form}/{org}/{is}/{service}/{version}`)."
        )

    if not datasets_under_service(dataset_names, path):
        cli_error(
            f"Path {path!r} has no datasets in manifest. Data services found in it:\n"
            + ("".join(f"  {service}\n" for service in services) or "  none\n")
        )

    return path
