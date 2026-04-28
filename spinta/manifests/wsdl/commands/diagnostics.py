from __future__ import annotations

from spinta import commands
from spinta.cli.helpers.diagnostics import get_diagnostics_storage
from spinta.components import Context
from spinta.manifests.wsdl.components import WsdlManifest
from spinta.manifests.wsdl.diagnostics import WSDL_SCHEMA_DIAGNOSTICS_KEY


@commands.store_schema_diagnostics.register(Context, WsdlManifest, str, list)
def store_schema_diagnostics(
    context: Context,
    manifest: WsdlManifest,
    path: str,
    diagnostics: list,
    **kwargs,
) -> None:
    from spinta.manifests.wsdl.helpers import normalize_wsdl_path

    normalized_path = normalize_wsdl_path(path)

    manifest.schema_diagnostics = list(diagnostics)

    if not diagnostics:
        return

    stored = get_diagnostics_storage(context, WSDL_SCHEMA_DIAGNOSTICS_KEY)
    for diagnostic in diagnostics:
        stored.append((normalized_path, str(diagnostic)))
