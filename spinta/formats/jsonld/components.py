from spinta.formats.json.components import Json


class JsonLd(Json):
    """JSON-LD response format, served for `Accept: application/ld+json` and `/:format/jsonld`.

    Unlike `Json`, it is always enriched with JSON-LD, whatever the `jsonld`
    config switch says (see `spinta.formats.jsonld.helpers.jsonld_enabled`).
    """

    content_type = "application/ld+json"
    accept_types = {
        "application/ld+json",
    }
