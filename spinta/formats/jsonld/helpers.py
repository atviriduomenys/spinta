from spinta.components import Context
from spinta.formats.components import Format
from spinta.formats.json.components import Json
from spinta.formats.jsonld.components import JsonLd


def jsonld_enabled(context: Context, fmt: Format) -> bool:
    """Tell whether a response in format `fmt` is enriched with JSON-LD.

    The format is already negotiated from the `Accept` header and the
    `/:format/...` path (`get_response_type`), so the request is not needed.

    - `JsonLd` (`Accept: application/ld+json`, `/:format/jsonld`): always.
    - `Json` (`Accept: application/json`, `/:format/json`): only when the
      `jsonld` config switch is on.
    - Any other format: never.
    """
    if isinstance(fmt, JsonLd):
        return True
    if isinstance(fmt, Json):
        return bool(context.get("config").jsonld)
    return False
