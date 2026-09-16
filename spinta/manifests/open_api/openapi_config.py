from spinta.core.enums import Action

#: Property types Spinta serves under `/{model}/{id}/{property}`. Anything else
#: raises `UnavailableSubresource`, see `spinta.commands.read.getone`.
PROPERTY_TYPES_IN_PATHS = {"file", "image"}
OBJECT_PROPERTY_TYPE = "object"

VERSION = "3.0.3"
INFO = {
    "version": "1.0.0",
    "description": "Data service of an UDTS agent. Every model of the service is served as a listing and as single objects, described below.",
    "title": "Universal application programming interface",
    "contact": {"name": "VSSA", "url": "https://vssa.lrv.lt/", "email": "info@vssa.lt"},
    "license": {"name": "CC-BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0/"},
}

EXTERNAL_DOCS = {"url": "https://ivpk.github.io/uapi"}

BASE_TAGS = [{"name": "utility", "description": "Utility operations performed on the API itself"}]

PROPERTY_EXAMPLE = {
    "string": "Example string",
    "integer": 42,
    "number": 123.45,
    "boolean": True,
    "datetime": "2025-09-23T11:44:11.753Z",
    "date": "2025-09-23",
    "time": "11:44:11",
    "text": "Example text content",
    "binary": "base64encodeddata==",
    "file": {"_id": "example.pdf", "_content_type": "application/pdf"},
    "image": {"_id": "example.png", "_content_type": "image/png"},
    "object": {},
    "geometry": "POINT (6088198 505579)",
    "money": 99.99,
}

#: A revision is a UUID, as is an identifier, see `spinta.backends`. This is the
#: canonical spelling, which is what a response carries.
_UUID_CANONICAL = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}"
UUID_PATTERN = f"^{_UUID_CANONICAL}$"

#: The same identifier as a request may spell it. `is_object_id` reads the value
#: with `uuid.UUID`, which drops an `urn:` and an `uuid:` prefix and surrounding
#: braces and takes the hexadecimal with or without the hyphens, so a document
#: accepting the canonical spelling alone would have a gateway refuse a request
#: Spinta serves.
#:
#: `uuid.UUID` is looser still: it drops those prefixes wherever they sit and
#: every hyphen wherever it sits, so `<id>urn:` is read as well. What is written
#: here are the spellings a client writes, while keeping the version and the
#: variant of the value asserted; following the parser all the way would mean
#: giving up on asserting those, which buys a gateway less than it costs.
_UUID_COMPACT = "[0-9a-fA-F]{12}4[0-9a-fA-F]{3}[89abAB][0-9a-fA-F]{15}"

#: A value of a property declared `uuid`, an `_id` of a model among them. It is
#: read by `UUID.load` through `is_str_uuid`, which builds the value again and
#: compares it with what was given, so only the canonical lower case spelling of
#: a version 4 UUID passes; the looser reading above is of `is_object_id`, which
#: an identifier Spinta itself gives goes through.
UUID_VALUE_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
UUID_REQUEST_PATTERN = f"^(?:urn:)?(?:uuid:)?\\{{?(?:{_UUID_CANONICAL}|{_UUID_COMPACT})\\}}?$"

STANDARD_OBJECT_PROPERTIES = {
    "_type": {"type": "string", "description": "Name of the model this object belongs to."},
    "_id": {
        "type": "string",
        # `uuid` is the format the registry gives; `uuidv4` is not one, so no
        # tool recognised it. The version is said by the pattern below.
        "format": "uuid",
        "pattern": UUID_PATTERN,
        "description": "Identifier of the object.",
    },
    "_revision": {
        "type": "string",
        "nullable": True,
        "pattern": UUID_PATTERN,
        "description": "Revision of the object, which changes with every change of it.",
    },
}

#: A page token, as `spinta.utils.encoding.encode_page_values` writes one: URL
#: safe Base64 with the padding kept, so every group of four is whole and only
#: the last one may carry `=`.
PAGE_TOKEN_PATTERN = "^(?:[A-Za-z0-9_-]{4})*(?:[A-Za-z0-9_-]{4}|[A-Za-z0-9_-]{3}=|[A-Za-z0-9_-]{2}==)$"

#: A value of a property declared `base32`, as `cast_backend_to_python` builds
#: one: the RFC 4648 alphabet with the padding dropped, which leaves a length
#: `base64.b32decode` can pad back and rules out one, three and six characters
#: over a multiple of eight, see `decode_id_value`.
#:
#: No pattern of the document looks around or repeats more than a thousand
#: times, see `HEADER_VALUE_PATTERN`, so a value is not empty because its last group
#: is not optional.
_BASE32_LENGTH = "(?:[A-Z2-7]{8})*(?:[A-Z2-7]{8}|[A-Z2-7]{2}|[A-Z2-7]{4}|[A-Z2-7]{5}|[A-Z2-7]{7})"
BASE32_VALUE_PATTERN = f"^{_BASE32_LENGTH}$"

#: The same value as a request gives it, behind the equals sign it is reached
#: by and bounded, because a path segment of a request is. The bound is
#: `BASE32_ID_MAX_LENGTH`, the equals sign included.
BASE32_ID_PATTERN = f"^={_BASE32_LENGTH}$"
BASE32_ID_MAX_LENGTH = 513

PROPERTY_MAPPING = {
    "string": {"type": "string"},
    "uuid": {"type": "string", "format": "uuid", "pattern": UUID_VALUE_PATTERN},
    # A `base32` value is built by Spinta, not read from the data, so its shape
    # is known, see `spinta.backends.cast_backend_to_python`.
    "base32": {"type": "string", "pattern": BASE32_VALUE_PATTERN},
    "integer": {"type": "integer"},
    "number": {"type": "number"},
    "boolean": {"type": "boolean"},
    "datetime": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
    "time": {"type": "string", "format": "time"},
    "text": {"type": "string"},
    "binary": {"type": "string", "format": "binary"},
    # In a model response a file is an object, its content is served by the
    # property endpoint.
    "file": {"$ref": "#/components/schemas/file"},
    "image": {"$ref": "#/components/schemas/image"},
    "geometry": {"type": "string", "description": "Geometry data in WKT format"},
    "money": {"type": "number"},
    "object": {"type": "object"},
}

#: A header value is printable ASCII, RFC 9110 section 5.5. The grammar of each
#: header is not repeated here; what is stated is the character set and a bound,
#: so a request carrying anything else is refused before it reaches the service.
#:
#: The bound is `maxLength` rather than a repetition of the pattern. Patterns of
#: the document are written in what regular expression dialects share: OpenAPI
#: 3.0 names ECMA 262, while the linter an API gateway is reviewed with reads
#: them as RE2 does, which has no lookaround and repeats a group a thousand
#: times at most. What a pattern can not say then, a `maxLength` or a `not`
#: beside it says.
HEADER_VALUE_PATTERN = "^[\\x20-\\x7E]+$"
HEADER_VALUE_MAX_LENGTH = 1024

#: The same characters without a bound, for a value of a response, which no
#: policy of an API gateway applies to.
HEADER_CHARACTER_PATTERN = "^[\\x20-\\x7E]+$"

#: `traceparent` of W3C Trace Context: version, trace id, parent id and flags,
#: hexadecimal throughout. Version `ff` is invalid and so is an identifier of
#: nothing but zeroes. Version `00` is those four fields and nothing else, while
#: a version above it may carry fields of its own after the flags, which a
#: parser has to tolerate rather than refuse, see the versioning section of the
#: specification.
#:
#: An identifier of zeroes is refused by `TRACEPARENT_OF_ZEROES_PATTERN` given
#: under `not`, since a pattern refusing it would have to look ahead.
_TRACE_ID_PARENT_ID_FLAGS = "[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}"
_LATER_TRACE_VERSION = "(?:0[1-9a-f]|[1-9a-e][0-9a-f]|f[0-9a-e])"
TRACEPARENT_PATTERN = (
    f"^(?:00-{_TRACE_ID_PARENT_ID_FLAGS}|{_LATER_TRACE_VERSION}-{_TRACE_ID_PARENT_ID_FLAGS}(?:-[!-~]{{1,512}})?)$"
)
TRACEPARENT_OF_ZEROES_PATTERN = "^[0-9a-f]{2}-(?:0{32}-[0-9a-f]{16}|[0-9a-f]{32}-0{16})-"

#: Scopes separated by spaces, each of them a `scope-token` of RFC 6749 section
#: 3.3: a printable character other than a space, a quotation mark or a
#: backslash. Narrower than that would refuse a scope a configured
#: `scope_formatter` builds, and it is free to build what it likes.
SCOPE_TOKEN = "[\\x21\\x23-\\x5B\\x5D-\\x7E]+"
SCOPE_PATTERN = f"^({SCOPE_TOKEN}( {SCOPE_TOKEN})*)?$"

#: An identifier a model declares itself holds the key of the data, of a shape
#: only that data knows, see `spinta.backends.is_object_id`. What can be said is
#: that it is one path segment, so it carries no slash, and that it is bounded.
DECLARED_ID_PATTERN = "^[^/]{1,512}$"

#: The same, where the value is reached by an equals sign, which
#: `spinta.backends.helpers.is_accessible_by_equals_sign` asks for: a `base32`
#: identifier, and a `string` one of a model keyed by a single property.
EQUALS_ID_PATTERN = "^=[^/]{1,512}$"

#: A `base32` identifier, of the alphabet RFC 4648 gives, behind an equals sign.
COMMON_RESPONSE_HEADERS = ["ETag", "Content-Type", "Content-Length"]

#: `304` answers before a body is built, see
#: `spinta.utils.response.validate_cache_control_request`, so it carries the
#: validators of the cache and none of the headers of an entity body.
NOT_MODIFIED_HEADERS = ["ETag", "Cache-Control"]

PATHS_CONFIG = {
    "/:version": {
        "parameters": ["traceparent", "tracestate"],
        "get": {
            "tags": ["utility"],
            "security": [{}],
            "summary": "Get API version",
            "description": "Get the version of the API that is being called\n",
            "operationId": "apiVersion",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {"application/json": {"schema": "version"}},
                },
                "400": {"$ref": "error400"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/:health": {
        "parameters": ["traceparent", "tracestate"],
        "get": {
            "tags": ["utility"],
            "security": [{}],
            "summary": "Check whether the service is operational",
            "description": (
                "Report whether the service is operational: it answered, and the disk and the memory "
                "of the machine it runs on are within the limits it was given. Backends holding the "
                "data are not probed, so a healthy service can still be answering off a backend that "
                "is not, see `spinta.api.health`.\n\n"
                "An unhealthy service is reported in the body, not in the status code: the answer is "
                "`200` with `healthy` set to `false`, because `503` says the service did not answer at "
                "all. A probe has to read `healthy` rather than the status code.\n"
            ),
            "operationId": "apiHealth",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": [*COMMON_RESPONSE_HEADERS, "Cache-Control"],
                    "content": {"application/json": {"schema": "health"}},
                },
                "400": {"$ref": "error400"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/:token": {
        "parameters": ["traceparent", "tracestate"],
        "post": {
            "tags": ["utility"],
            "security": [{"UAPI_client": []}],
            "summary": "Get an access token",
            "description": "Get an OAuth 2.0 access token using the `client_credentials` grant.\n\nClient credentials are given in the `Authorization` header using HTTP Basic authentication scheme.\n\n\nRFC 6749 section 2.3.1 asks for TLS here: the credentials are sent in plain, base64 of them being plain. The transport of an environment is not stated by this document — a server URL may be relative, and a deployment reached over `http` is described the same way — so it is ensured where the service is deployed.\n",
            "operationId": "apiToken",
            "requestBody": {
                "required": True,
                "description": "Credentials of the client, given as an OAuth 2.0 `client_credentials` request, see RFC 6749 section 4.4.2.",
                "content": {
                    "application/x-www-form-urlencoded": {
                        "schema": {
                            "type": "object",
                            "required": ["grant_type"],
                            "properties": {
                                "grant_type": {
                                    "type": "string",
                                    "enum": ["client_credentials"],
                                    "description": "The only grant this endpoint serves.",
                                    "example": "client_credentials",
                                },
                                # Example is filled in with a scope of this
                                # data service, see `_add_security_schemes`.
                                "scope": {
                                    "type": "string",
                                    "pattern": SCOPE_PATTERN,
                                    "description": "Space separated list of requested scopes.",
                                },
                            },
                        }
                    }
                },
            },
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {"application/json": {"schema": "token"}},
                },
                "400": {"$ref": "tokenError400"},
                "401": {"$ref": "tokenError401"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/version": {
        # Served by the agent itself, so it takes a server of its own.
        "servers": "agent",
        "parameters": ["traceparent", "tracestate"],
        "get": {
            "tags": ["utility"],
            "security": [{}],
            "summary": "Get API version, from the agent itself",
            "description": "Get the version of the API that is being called.\n\nThis is the endpoint of the agent, called at its own address. An API gateway serves the same endpoint inside a data service, as `/:version`.\n",
            "operationId": "apiVersionOfAgent",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {"application/json": {"schema": "version"}},
                },
                "400": {"$ref": "error400"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/health": {
        # Served by the agent itself, so it takes a server of its own.
        "servers": "agent",
        "parameters": ["traceparent", "tracestate"],
        "get": {
            "tags": ["utility"],
            "security": [{}],
            "summary": "Check whether the service is operational, from the agent itself",
            "description": (
                "Report whether the service is operational: it answered, and the disk and the memory "
                "of the machine it runs on are within the limits it was given. Backends holding the "
                "data are not probed, so a healthy service can still be answering off a backend that "
                "is not, see `spinta.api.health`.\n\n"
                "An unhealthy service is reported in the body, not in the status code: the answer is "
                "`200` with `healthy` set to `false`, because `503` says the service did not answer at "
                "all. A probe has to read `healthy` rather than the status code.\n\n"
                "This is the endpoint of the agent, called at its own address. An API gateway serves the "
                "same endpoint inside a data service, as `/:health`.\n"
            ),
            "operationId": "apiHealthOfAgent",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": [*COMMON_RESPONSE_HEADERS, "Cache-Control"],
                    "content": {"application/json": {"schema": "health"}},
                },
                "400": {"$ref": "error400"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/auth/token": {
        # Served by the agent itself, so it takes a server of its own: every
        # environment of the document, with the path of the data service off.
        "servers": "agent",
        "parameters": ["traceparent", "tracestate"],
        "post": {
            "tags": ["utility"],
            "security": [{"UAPI_client": []}],
            "summary": "Get an access token, from the agent itself",
            "description": "Get an OAuth 2.0 access token using the `client_credentials` grant.\n\nClient credentials are given in the `Authorization` header using HTTP Basic authentication scheme.\n\nThis is the endpoint of the agent, called at its own address. An API gateway serves the same endpoint inside a data service, as `/:token`.\n\n\nRFC 6749 section 2.3.1 asks for TLS here: the credentials are sent in plain, base64 of them being plain. The transport of an environment is not stated by this document — a server URL may be relative, and a deployment reached over `http` is described the same way — so it is ensured where the service is deployed.\n",
            "operationId": "apiTokenOfAgent",
            "requestBody": {
                "required": True,
                "description": "Credentials of the client, given as an OAuth 2.0 `client_credentials` request, see RFC 6749 section 4.4.2.",
                "content": {
                    "application/x-www-form-urlencoded": {
                        "schema": {
                            "type": "object",
                            "required": ["grant_type"],
                            "properties": {
                                "grant_type": {
                                    "type": "string",
                                    "enum": ["client_credentials"],
                                    "description": "The only grant this endpoint serves.",
                                    "example": "client_credentials",
                                },
                                # Example is filled in with a scope of this
                                # data service, see `_add_security_schemes`.
                                "scope": {
                                    "type": "string",
                                    "pattern": SCOPE_PATTERN,
                                    "description": "Space separated list of requested scopes.",
                                },
                            },
                        }
                    }
                },
            },
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {"application/json": {"schema": "token"}},
                },
                "400": {"$ref": "tokenError400"},
                "401": {"$ref": "tokenError401"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}": {
        "parameters": ["traceparent", "tracestate", "Cache-Control", "If-None-Match", "Accept-Language"],
        "head": {
            # Spinta authorizes `HEAD` against the same actions as `GET`.
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "modelHead",
            # `HEAD` is narrowed down by the same query as `GET`, see
            # `spinta.urlparams.get_action`, which is why it takes the `search`
            # scope as well.
            "parameters": ["select", "limit", "sort", "page"],
            "responses": {
                "200": {"description": "OK"},
                "304": {"description": "Not Modified", "headers": NOT_MODIFIED_HEADERS},
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
        "get": {
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Get multiple objects.",
            "description": "Return list of objects for a given model.\n\nThe listing is narrowed down by the query parameters listed below, and by a filter on the properties of the model, `?code='LT'` or `?name.startswith('V')` for one, which is not a parameter of a fixed name and is not listed. Two more forms are accepted and are not listed, because neither is a `name=value` pair: `count()`, written as `?count()` or as `?_count` without a value, answers with the number of objects instead of the objects; and `limit(...)`, `select(...)`, `sort(...)` and `page(...)`, written as calls. A parameter left empty, `?_select=` for one, is refused.\n\nA request narrowed down with query parameters is authorized with the `:search` scope, an unnarrowed one with `:getall`. The two are listed as alternative security requirements, because OpenAPI can not make a requirement depend on query parameters, so they are not interchangeable: a token needs the scope of the request it makes.\n",
            "operationId": "getAll",
            "parameters": ["select", "limit", "sort", "page"],
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {
                        "application/json": {"schema": "objects"},
                    },
                },
                "304": {
                    "description": "Not Modified",
                    "headers": NOT_MODIFIED_HEADERS,
                },
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}/{id}": {
        "parameters": ["id", "traceparent", "tracestate", "If-None-Match", "Accept-Language"],
        "head": {
            # Spinta authorizes `HEAD` against the same actions as `GET`.
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "headOne",
            "responses": {
                "200": {"description": "OK"},
                "301": {
                    "description": "Moved Permanently. The identifier was moved to another one, which `Location` gives.",
                    "headers": ["Location"],
                },
                "304": {"description": "Not Modified", "headers": NOT_MODIFIED_HEADERS},
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
        "get": {
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Get a single object by given {id}.",
            "description": "Retrieve a single specific object based on its unique object identifier {id}\n",
            "operationId": "getOne",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {
                        "application/json": {"schema": "object"},
                    },
                },
                "301": {
                    "description": "Moved Permanently. The identifier was moved to another one, which `Location` gives.",
                    "headers": ["Location"],
                },
                "304": {
                    "description": "Not Modified",
                    "headers": NOT_MODIFIED_HEADERS,
                },
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}/{id}/{field}": {
        # Property name is part of the generated path, so it is not a parameter.
        "parameters": ["id", "traceparent", "tracestate", "If-None-Match", "Accept-Language", "Range"],
        "head": {
            # Spinta authorizes `HEAD` against the same actions as `GET`.
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "headProperty",
            # `Range` is a parameter of the path, so a `HEAD` is ranged as well,
            # and `starlette.responses.FileResponse` answers it the same way.
            "responses": {
                "200": {"description": "OK"},
                "206": {"description": "Partial Content", "headers": COMMON_RESPONSE_HEADERS},
                "304": {"description": "Not Modified", "headers": NOT_MODIFIED_HEADERS},
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "416": {"description": "Range Not Satisfiable"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
        "get": {
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "For a given specific object by {id}, retrieve one property of it (subresource).",
            "description": "Retrieve a specific property from an object structure.\n\nBy default when retrieving object you receive all data items from its structure, using this service you retrieve a specific property from its structure. The property is named by the path itself, one path per property, so there is no parameter to fill in for it.\n\nWhere that property is a file instead of data, the file is served as a binary stream.\n",
            "operationId": "getProperty",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {
                        # Property endpoints are generated for file and image
                        # properties, which serve the file content with the
                        # media type it was stored with.
                        "*/*": {"schema": {"type": "string", "format": "binary"}}
                    },
                },
                "206": {
                    "description": "Partial Content",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {"*/*": {"schema": {"type": "string", "format": "binary"}}},
                },
                "304": {
                    "description": "Not Modified",
                    "headers": NOT_MODIFIED_HEADERS,
                },
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "416": {"description": "Range Not Satisfiable"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}/{id}/{field}:ref": {
        # Property name is part of the generated path, so it is not a parameter.
        "parameters": ["id", "traceparent", "tracestate", "If-None-Match", "Accept-Language"],
        "head": {
            # Spinta authorizes `HEAD` against the same actions as `GET`.
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "headPropertyRef",
            "responses": {
                "200": {"description": "OK"},
                "304": {"description": "Not Modified", "headers": NOT_MODIFIED_HEADERS},
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
        "get": {
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Get the metadata of a file property",
            "description": "Return what is known about the file a property holds, its name and its media type, instead of the file itself. The file itself is served by the same path without the `:ref` action.\n",
            "operationId": "getPropertyRef",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {"application/json": {"schema": "fileRef"}},
                },
                "304": {"description": "Not Modified", "headers": NOT_MODIFIED_HEADERS},
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}/{id}/{object_field}": {
        # Property name is part of the generated path, so it is not a parameter.
        "parameters": ["id", "traceparent", "tracestate", "If-None-Match", "Accept-Language"],
        "head": {
            # Spinta authorizes `HEAD` against the same actions as `GET`.
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "headObjectProperty",
            "responses": {
                "200": {"description": "OK"},
                "304": {"description": "Not Modified", "headers": NOT_MODIFIED_HEADERS},
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
        "get": {
            "security": [{"UAPI_auth": []}],  # Scopes are filled in per model and action.
            "summary": "Get one object property of an object",
            "description": "Return a single object property of a given object, instead of the whole object. The answer holds what the property itself holds, together with `_type` and `_revision` of the object it belongs to.\n",
            "operationId": "getObjectProperty",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    # Schema is the object the property holds, per property.
                    "content": {"application/json": {"schema": None}},
                },
                "304": {"description": "Not Modified", "headers": NOT_MODIFIED_HEADERS},
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "429": {"$ref": "error429"},
                "503": {"$ref": "error503"},
            },
        },
    },
}

#: Fields of an error object, see `spinta.exceptions.error_response`. An error
#: that is not Spinta's own, one `authlib` raises for one, is answered with the
#: `code` and the `message` alone, see `spinta.api.error_response`.
ERROR_CONTEXT = {
    "type": "object",
    "description": "What the error happened on: the model, the property, the manifest and whatever else the template of this error names. Which keys it holds depends on the error.",
    "example": {"component": "spinta.components.Model", "manifest": "default"},
}
ERROR_MESSAGE = {
    "type": "string",
    "description": "The template of this error, filled in with its context.",
    # A named error replaces this with its own template.
    "example": "Model 'datasets/gov/rc/jadis/at280/1/at280_israsas/Israsas' not found.",
}


def _error_schema(name: str, template: str | None = None) -> dict:
    """Schema of one error object.

    `code` is the name of the class that raised it and `template` the template
    of that class, both of them fixed, so both are given as constants, taken
    from the class rather than copied beside it, where they drifted apart
    before. `type` is decided per error, out of what it happened on.
    """
    schema = {
        "type": "object",
        "description": f"Error object of `{name}`.",
        # `error_response` writes all of them, every time, so an object holding
        # fewer is not an error of Spinta.
        "required": ["type", "code", "template", "context", "message"],
        "properties": {
            "type": {
                "type": "string",
                "description": "What the error happened on, `system` when it is nothing in particular.",
                "example": "system",
            },
            "code": {"type": "string", "enum": [name], "example": name},
            "message": dict(ERROR_MESSAGE),
            "context": dict(ERROR_CONTEXT),
        },
        "additionalProperties": False,
    }
    if template is not None:
        schema["properties"]["template"] = {"type": "string", "enum": [template], "example": template}
        schema["properties"]["message"]["example"] = _example_message(template)
    return schema


class _ExampleValue(str):
    """A stand-in for whatever an error template names.

    Templates read attributes of the values they are given, so the same
    stand-in answers for those as well.
    """

    def __getattr__(self, name: str) -> "_ExampleValue":
        return self


class _ExampleContext(dict):
    def __missing__(self, key: str) -> _ExampleValue:
        return _ExampleValue("example")


def _example_message(template: str) -> str:
    """The message of an error, as `error_response` sends it: filled in.

    A template is what an error is built from, not what it answers with, so an
    example holding `{model!r}` shows an object the service never sends.
    """
    try:
        return template.format_map(_ExampleContext())
    except (IndexError, KeyError, ValueError):
        return template


def _example_error_template() -> str:
    """Template of the error the generic example stands for.

    `ERROR_MESSAGE` shows that error filled in, so the two describe one object,
    and the example is then an error Spinta really answers with rather than a
    part of one.
    """
    from spinta.exceptions import ModelNotFound

    return ModelNotFound.template


def _errors_of(status_code: int) -> dict[str, dict]:
    """Schemas of the errors Spinta answers with under a status code."""
    import inspect

    from spinta import exceptions

    return {
        name: _error_schema(name, obj.template)
        for name, obj in sorted(vars(exceptions).items())
        if inspect.isclass(obj)
        and issubclass(obj, exceptions.BaseError)
        and obj is not exceptions.BaseError
        and getattr(obj, "status_code", None) == status_code
    }


#: Errors named one by one, where a status code has few enough of them to be
#: worth naming. `400` and `500` have over a hundred each, so they are answered
#: for by `Error` alone.
NAMED_ERRORS = {status: _errors_of(status) for status in (401, 403, 404, 409, 415)}

#: Any error object at all, which is what a response accepts beside the ones it
#: names: Spinta answers with more error codes than a document can list, and an
#: error that is not its own carries the `code` and the `message` alone.
GENERIC_ERROR = {
    "Error": {
        "type": "object",
        "description": "Any error object. Every error carries a `code` and a `message`; an error of Spinta itself carries the `template` it was built from, the `context` it happened in, and what that context is.",
        # Two of them are written by every error, of Spinta or not, see
        # `spinta.api.error_response`; the rest only by an error of Spinta.
        "required": ["code", "message"],
        "properties": {
            "type": {"type": "string", "description": "What the error happened on.", "example": "system"},
            "code": {"type": "string", "description": "Name of the error.", "example": "ModelNotFound"},
            "template": {
                "type": "string",
                "description": "Template the message was built from.",
                "example": _example_error_template(),
            },
            "message": dict(ERROR_MESSAGE),
            "context": dict(ERROR_CONTEXT),
        },
        "additionalProperties": False,
    },
    # `authlib` answers a missing or insufficient scope with this, and it is
    # not an error of Spinta, so it carries no template and no context.
    "InsufficientScopeError": {
        "type": "object",
        "description": "Error object of an access token that does not carry a scope the operation needs.",
        "required": ["code", "message"],
        "properties": {
            "code": {"type": "string", "enum": ["InsufficientScopeError"], "example": "InsufficientScopeError"},
            "message": {
                "type": "string",
                "description": "Which scopes would have been enough.",
                "example": "insufficient_scope: Missing one of required scopes: uapi:/datasets/gov/rc/jadis/at280/1/:getall",
            },
        },
        "additionalProperties": False,
    },
    "InvalidScopes": {
        "type": "object",
        "description": "Error object of a token request naming a scope that does not exist.",
        "required": ["type", "code", "template", "context", "message"],
        "properties": {
            "type": {"type": "string", "example": "system"},
            "code": {"type": "string", "enum": ["InvalidScopes"], "example": "InvalidScopes"},
            "template": {
                "type": "string",
                "enum": ["Request contains invalid, unknown or malformed scopes: {scopes}."],
                "example": "Request contains invalid, unknown or malformed scopes: {scopes}.",
            },
            # The message of this error, not of whichever one `ERROR_MESSAGE`
            # stands for; `error_response` sends it filled in.
            "message": {
                **ERROR_MESSAGE,
                "example": _example_message("Request contains invalid, unknown or malformed scopes: {scopes}."),
            },
            "context": dict(ERROR_CONTEXT),
        },
        "additionalProperties": False,
    },
}


def _named_errors(status_code: int, *also: str) -> list[str]:
    """Errors a response names, the one accepting any error object last."""
    return [*sorted(NAMED_ERRORS.get(status_code, {})), *also, "Error"]


RESPONSE_COMPONENTS = {
    # Rate limiting is applied by an API gateway or by whatever else stands in
    # front of the service, a WAF or a reverse proxy, not by Spinta itself, so
    # what the body holds is decided there and is not described here.
    "error429": {
        "description": "Too Many Requests",
        # A rate limit is applied in front of the service, by an API gateway,
        # and never by Spinta. The gateway answers with an object, whose fields
        # are its own, see `RateLimited`.
        "content": {"*/*": {"schema": "RateLimited"}},
    },
    # Token endpoint answers with an OAuth 2.0 error, see RFC 6749 section 5.2,
    # not with a Spinta one.
    "tokenError400": {
        "description": "Bad Request",
        "headers": [],
        "content": {
            "application/json": {
                "schema": {"anyOf": ["tokenError", {"errors": ["InvalidScopes", "Error"]}]},
                # An alternative of two, so neither schema example answers for it.
                "example": {"error": "invalid_client", "error_description": "Client authentication failed."},
            }
        },
    },
    "tokenError401": {
        "description": "Unauthorized",
        "headers": [],
        "content": {"application/json": {"schema": "tokenError"}},
    },
    # An error response names the errors of its status code and accepts any
    # other: `400` alone has over a hundred of them, and an error that is not
    # Spinta's own carries the `code` and the `message` alone.
    "error400": {
        "description": "Bad Request",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(400)}}},
    },
    "error401": {
        "description": "Unauthorized",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(401)}}},
    },
    "error403": {
        "description": "Forbidden",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(403, "InsufficientScopeError")}}},
    },
    "error404": {
        "description": "Not Found",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(404)}}},
    },
    "error409": {
        "description": "Conflict",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(409)}}},
    },
    "error415": {
        "description": "Unsupported Media Type",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(415)}}},
    },
    "error500": {
        "description": "Internal Server Error",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(500)}}},
    },
    "error503": {
        "description": "Service Unavailable",
        "headers": [],
        "content": {"application/json": {"schema": {"errors": _named_errors(503)}}},
    },
}

HEADER_COMPONENTS = {
    "Content-Type": {
        "description": "The `Content-Type` header indicates the media type of the resource or data. For responses, it tells the client what the content type of the returned content actually is.",
        "required": True,
        "schema": {"type": "string", "example": "application/json"},
    },
    "Content-Length": {
        "description": "The `Content-Length` header indicates the size of the response body, in bytes, sent to the recipient.",
        "required": False,
        "schema": {
            "type": "integer",
            "format": "int64",
            "minimum": 0,
            "maximum": 9223372036854775807,
            "example": 1024,
        },
    },
    "ETag": {
        "description": "`ETag` header is an entity tag that uniquely represents the requested resource. It is a revision number for this item.",
        "required": False,
        "schema": {
            "type": "string",
            # A revision a model declares itself is of no stated length, and a
            # response is not bounded by a policy of the gateway, so only the
            # characters a header may hold are stated here.
            "pattern": HEADER_CHARACTER_PATTERN,
            "example": "16dabe62-61e9-4549-a6bd-07cecfbc3508",
        },
    },
    "Cache-Control": {
        "description": "The `Cache-Control` header tells caches what they may do with the response. A probe answers `no-store`, because a cached answer would report a state the service no longer is in.",
        "required": False,
        "schema": {"type": "string", "example": "no-store"},
    },
    "Location": {
        "description": "Where the object lives now. Answered with `301` when the identifier asked for was moved to another one, see `spinta.commands.read.getone`.",
        "required": True,
        "schema": {
            "type": "string",
            "format": "uri-reference",
            "example": "/datasets/gov/rc/jadis/at280/1/at280_israsas/Israsas/abdd1245-bbf9-4085-9366-f11c0f737c1d",
        },
    },
}


PARAMETER_COMPONENTS = {
    "traceparent": {
        "name": "traceparent",
        "in": "header",
        "description": "The `traceparent` request header represents the incoming request in a tracing system in a common format, understood by all vendors. For more context check [***trace-context***](https://w3c.github.io/trace-context/) documentation.",
        "required": False,
        "schema": {
            "type": "string",
            "pattern": TRACEPARENT_PATTERN,
            "not": {"type": "string", "pattern": TRACEPARENT_OF_ZEROES_PATTERN},
            "description": "Consists of `version` `trace-id` `parent-id` `trace-flags` separated by `-`. \n\n`trace-id` recommended to be in UUIDv4",
            "example": "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01",
        },
    },
    "tracestate": {
        "name": "tracestate",
        "in": "header",
        "required": False,
        "description": "The main purpose of the `tracestate` HTTP header is to provide additional vendor-specific trace identification information across different distributed tracing systems and is a companion header for the `traceparent` field. It also conveys information about the request's position in multiple distributed tracing graphs.\nFor more context check [***trace-context***](https://w3c.github.io/trace-context/) documentation.",
        "schema": {
            "type": "string",
            "pattern": HEADER_VALUE_PATTERN,
            "maxLength": HEADER_VALUE_MAX_LENGTH,
            "description": "Consists of a `list` of `list-members` separated by commas (`,`)",
            "example": "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE",
        },
    },
    "Cache-Control": {
        "name": "Cache-Control",
        "in": "header",
        "required": False,
        "description": "`Cache-Control` header should be used if service supports caching. It allows the user to provide directives from their side. `no-cache` can be used to request revalidation of data with the origin server before reuse. `no-store` can be used to request to not store the data in caches.\n\nMultiple directives can be used separated by `, `. If they are conflicting, most restrictive directive should be honored.",
        "schema": {
            "type": "string",
            "pattern": HEADER_VALUE_PATTERN,
            "maxLength": HEADER_VALUE_MAX_LENGTH,
            "example": "no-cache",
        },
    },
    "Range": {
        "name": "Range",
        "in": "header",
        "required": False,
        "description": "Part of a file to return, see [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110#field.range). A file kept in a file system is served with `Accept-Ranges: bytes`; one kept in a database is returned whole, with `200`.",
        "schema": {
            "type": "string",
            "pattern": HEADER_VALUE_PATTERN,
            "maxLength": HEADER_VALUE_MAX_LENGTH,
            "example": "bytes=0-1023",
        },
    },
    "If-None-Match": {
        "name": "If-None-Match",
        "in": "header",
        "required": False,
        "description": "Using `If-None-Match` client can provide a revision number of an object to server to check if modification to the object has occurred, if not, server will return `304 - Not Modified`.",
        # It carries a revision, whose shape a model can declare itself.
        "schema": {
            "type": "string",
            "pattern": HEADER_VALUE_PATTERN,
            "maxLength": HEADER_VALUE_MAX_LENGTH,
            "example": "16dabe62-61e9-4549-a6bd-07cecfbc3508",
        },
    },
    "Accept-Language": {
        "name": "Accept-Language",
        "in": "header",
        "required": False,
        "description": '`Accept-Language` header is used to indicate the language preference of the user. It\'s a list of values with quality factors (e.g., `"de, en"`).',
        "schema": {
            "type": "string",
            "pattern": HEADER_VALUE_PATTERN,
            "maxLength": HEADER_VALUE_MAX_LENGTH,
            "example": "lt",
        },
    },
    # What narrows a listing down is given as parameters of its own, each a
    # `name=value` pair a gateway validating requests checks one by one. The
    # ones naming properties are built for every model, see
    # `PathGenerator._select_parameter`, and `_limit` for every document.
    "select": {
        "name": "_select",
        "in": "query",
        "required": False,
        "description": "Comma separated list of properties to include in the result. Written as `select(...)` as well.",
        "schema": {
            "type": "string",
            # Names, dotted paths and function calls, which is what the query
            # language holds here, plus the `*` of `_select=*`, which asks for
            # everything, see `spinta.spyna`. The characters are bounded so a
            # gateway validating requests refuses anything else.
            "pattern": "^[A-Za-z0-9_.,@()* +-]{1,1000}$",
            "example": "name,country.name,country.continent.name",
        },
    },
    "sort": {
        "name": "_sort",
        "in": "query",
        "required": False,
        "description": "Comma separated list of properties, optionally prefixed with `+` or `-` operators to control sort direction. Written as `sort(...)` as well.",
        "schema": {
            "type": "string",
            # The same, with `+` or `-` for the direction and no calls.
            "pattern": "^[A-Za-z0-9_.,@ +-]{1,1000}$",
            "example": "-code,country.name",
        },
    },
    "limit": {
        "name": "_limit",
        "in": "query",
        "required": False,
        "description": "Limit result to given number of objects. A larger listing is answered a page at a time, see `_page`. Written as `limit(...)` as well.",
        "schema": {
            "type": "integer",
            # A limit below one is refused. Spinta holds to no upper bound, so
            # `maximum` is the one an API gateway applies in front of it, set
            # per document, see `PathGenerator._limit_parameter`.
            "minimum": 1,
            "example": 10,
        },
    },
    "page": {
        "name": "_page",
        "in": "query",
        "required": False,
        "description": "Continues a listing where the previous answer ended, taking the token that answer gave in `_page.next`. The token is given as it is, `=` padding included, or percent encoded. Written as `page('<token>')` as well.",
        "schema": {
            "type": "string",
            # The token `spinta.utils.encoding.encode_page_values` writes.
            "pattern": PAGE_TOKEN_PATTERN,
            "example": "WyIyMDI2LTA4LTMxIl0=",
        },
    },
    "id": {
        "name": "id",
        "in": "path",
        "required": True,
        # A model can declare `_id` of its own, and then the parameter is built
        # for that model, see `PathGenerator._id_parameter`. This one describes
        # the identifier Spinta gives, which `is_object_id` accepts only as a
        # UUID version 4, in any of the spellings `uuid.UUID` reads.
        "description": "Public global object identifier.\n\nAn identifier is an UUID version 4. It is read with `uuid.UUID`, so it may be given hyphenated or not, in braces, or behind an `urn:`, `uuid:` or `urn:uuid:` prefix; a response always carries the canonical hyphenated spelling, which is the one to send.\n\nOnce object is assigned a global identifier, it should never change.",
        "schema": {
            "type": "string",
            # No `format` here, although a response carries one: a validator
            # asserting `format: uuid` knows the canonical spelling alone and
            # would refuse the ones the pattern is here to accept.
            "pattern": UUID_REQUEST_PATTERN,
            "example": "abdd1245-bbf9-4085-9366-f11c0f737c1d",
        },
    },
    "property": {
        "name": "property",
        "in": "path",
        "required": True,
        "description": "Subresource of an object.\n\nAll lower case, words separated with `_` symbol.",
        "schema": {"type": "string", "example": "cities"},
    },
}


#: Schema of a reference to a model whose metadata is not published, see
#: `_published`. It names nothing of that model, neither its name nor its
#: properties, so it says no more than that an object is there.
UNPUBLISHED_REFERENCE = "UnpublishedReference"

COMMON_SCHEMAS = {
    # Error objects, built from the classes that raise them.
    **GENERIC_ERROR,
    UNPUBLISHED_REFERENCE: {
        "type": "object",
        "description": "A reference to an object of a model whose metadata is not published, so what the reference carries is not described.",
        "properties": {},
    },
    "RateLimited": {
        # An object, as the API gateway answers with one, but an open one: which
        # fields it holds is decided by the gateway, not by Spinta, so none is
        # required or refused.
        "type": "object",
        "description": "Answer of a rate limit reached. A limit is applied by an API gateway in front of the service, and never by Spinta, so which fields the object holds is decided there. None of them is asserted here, otherwise validating a response would refuse the very answer that says the limit was reached.",
        "properties": {},
        "example": {"message": "Rate limit exceeded", "http_status_code": 429},
    },
    **{name: schema for errors in NAMED_ERRORS.values() for name, schema in errors.items()},
    "absent": {
        "type": "object",
        "description": "For objects that have been deleted during change, `type` value is changed to `absent`",
        "properties": {"type": {"type": "string", "enum": ["absent"]}},
    },
    "binary": {
        "type": "string",
        "description": "Binary string of data. A single set should not exceed 1G",
        "pattern": "^[0-1]+$",
    },
    "text": {"type": "string", "description": "Natural language text."},
    "datetime": {
        "type": "string",
        "description": "Date and time provided in a standard format based on [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html)\n\nMinimum value: `0001-01-01T00:00:00`\nMaximum value: `999-12-31T23:59:59.999999`\n\nBased on maturity level data can be:\n- maturity level 1 - provided in different formats or free text\n- maturity level 2 - not according to standard but all in the same format. Or different parts of data are available in different fields (eg. year in one field and a month in another)\n- maturity level >=3 - data provided according to `ISO 8601` standard",
    },
    "date": {
        "type": "string",
        "description": "Date provided in a standard format based on [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html)\n\nMinimum value: `0001-01-01`\nMaximum value: `999-12-31`\n\nIf the resolution of data is lower than a day or a month 01 can be used instead.\n\nBased on maturity level data can be:\n- maturity level 1 - provided in different formats or free text\n- maturity level 2 - not according to standard but all in the same format. Or different parts of data are available in different fields (eg. year in one field and a month in another)\n- maturity level >=3 - data provided according to `ISO 8601` standard",
    },
    "time": {
        "type": "string",
        "description": "Time provided in a standard format based on [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html)\n\nMinimum value: `00:00:00`\nMaximum value: `23:59:59.999999`\n\nIf the resolution of data is lower than a second 00 can be used instead.\n\nBased on maturity level data can be:\n- maturity level 1 - provided in different formats or free text\n- maturity level 2 - not according to standard but all in the same format. Or different parts of data are available in different fields (eg. year in one field and a month in another)\n- maturity level >=3 - data provided according to `ISO 8601` standard",
    },
    "temporal": {"type": "string", "description": "Temporal definition in time. Same format as `datetime`"},
    "geometry": {
        "type": "object",
        "description": "Geometry data. Data provided in [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) format, using [EPSG](https://epsg.org/home.html) database parameters, for different projections.",
        "properties": {
            "form": {
                "type": "string",
                "description": "Geometry form can have these types - `point`, `linestring`, `polygon`, `multipoint`, `multilinestring`, `multipolygon`. Each of these types can have `z` (height), `m` (selected measurement like time, distance, width, etc.) or `zm` (height and a selected measurement) designations after the type.",
                "enum": [
                    "point",
                    "point z",
                    "point m",
                    "point zm",
                    "linestring",
                    "linestring z",
                    "linestring m",
                    "linestring zm",
                    "polygon",
                    "polygon z",
                    "polygon m",
                    "polygon zm",
                    "multipoint",
                    "multipoint z",
                    "multipoint m",
                    "multipoint zm",
                    "multilinestring",
                    "multilinestring z",
                    "multilinestring m",
                    "multilinestring zm",
                    "multipolygon",
                    "multipolygon z",
                    "multipolygon m",
                    "multipolygon zm",
                ],
            },
            "crs": {
                "type": "integer",
                "description": "A [SRID](https://en.wikipedia.org/wiki/Spatial_reference_system#Identifier) number, which is an identification number of a coordinate system in [EPSG](https://epsg.org/home.html) database. If the number is not provided, it is assumed that data corresponds to `4326` ( [WGS84](https://epsg.io/4326) )",
            },
        },
    },
    "money": {
        "type": "number",
        "description": "The amount of a certain currency. Currency code is provided in `property.ref` in accordance to [ISO 4217](https://en.wikipedia.org/wiki/ISO_4217).",
    },
    "file": {
        "type": "object",
        "description": "What is known about a file a property holds.",
        # Spinta names the file `_id`, see `spinta.types.file.components.FileData`,
        # and leaves the values null when the file is deleted.
        "properties": {
            "_id": {"type": "string", "nullable": True, "description": "File name"},
            "_content_type": {
                "type": "string",
                "nullable": True,
                "description": "A [Media type](https://en.wikipedia.org/wiki/Media_type) of the file.",
            },
        },
    },
    "url": {"type": "string", "description": "Uniform Resource Locator. Used to provide links to external sources."},
    "uri": {
        "type": "string",
        "description": "Uniform Resource Identifier. Used to provide an identifier of an external resource, in an RDF data model it is subject identifier.",
    },
    "backref": {
        "type": "string",
        "description": "Backwards link showing that another model has a link to this one. This item does not hold any data",
    },
    "health": {
        "type": "object",
        "description": "Whether the service is operational, together with what it checked to say so.",
        "required": ["healthy", "dependencies"],
        "properties": {
            "healthy": {
                "type": "boolean",
                "description": "Whether every dependency below is healthy.",
                "example": True,
            },
            "dependencies": {
                "type": "array",
                "example": [
                    {"name": "spinta", "healthy": True},
                    {"name": "disk", "healthy": True},
                    {"name": "memory", "healthy": True},
                ],
                "description": "What the service checked, one entry per dependency. Which ones are reported is up to the service and can change between versions, so read the entries rather than expect a given set.",
                "items": {
                    "type": "object",
                    # Which dependencies are reported can change, what is
                    # reported about one can not, see `spinta.api.health`.
                    "required": ["name", "healthy"],
                    "properties": {
                        "name": {"type": "string", "example": "spinta"},
                        "healthy": {"type": "boolean"},
                    },
                },
            },
        },
    },
    "fileRef": {
        "type": "object",
        "description": "What is known about a file a property holds. `_id` and `_content_type` are null while the property holds no file.",
        # The envelope of the object the property belongs to is written whatever
        # the request selects, see `prepare_data_for_response` of a `File`.
        "required": ["_type", "_revision"],
        "properties": {
            "_type": {"type": "string", "example": "datasets/gov/rc/jadis/at280/1/ds/Israsas.byla"},
            "_revision": {"type": "string", "nullable": True},
            "_id": {"type": "string", "nullable": True, "description": "File name"},
            "_content_type": {
                "type": "string",
                "nullable": True,
                "description": "A [Media type](https://en.wikipedia.org/wiki/Media_type) of the file.",
            },
        },
    },
    "page": {
        "type": "object",
        "description": "Where the next page of a listing starts. Given back in `_page.next` and asked for as `page('<token>')` of the next request; the token carries `=` padding, which the query syntax reads only in quotes.",
        # `_page` is written only when there is a token to write, see
        # `spinta.formats.json`, so an object without one is not an answer.
        "required": ["next"],
        "properties": {
            "next": {
                "type": "string",
                # URL safe Base64 as `encode_page_values` writes it, padding
                # and all, which leaves a length of whole quads.
                "pattern": PAGE_TOKEN_PATTERN,
                "description": "Token of the next page.",
                "example": "WyIyMDI2LTA4LTMxIl0=",
            }
        },
    },
    "version": {
        "type": "object",
        "description": "Versions of the API, of its implementation and of the specifications it follows.",
        "properties": {
            "api": {"type": "object", "properties": {"version": {"type": "string", "example": "0.0.1"}}},
            "implementation": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "example": "Spinta"},
                    # A version is a string, and `0.1` was a number.
                    "version": {"type": "string", "example": "1.2.0"},
                },
            },
            "dsa": {"type": "object", "properties": {"version": {"type": "string", "example": "0.1.0"}}},
            "uapi": {"type": "object", "properties": {"version": {"type": "string", "example": "0.1.0"}}},
            "build": {"type": "object", "properties": {"version": {"type": "string", "example": "0.0.1"}}},
        },
    },
    "tokenError": {
        "type": "object",
        "description": "An OAuth 2.0 error of the token endpoint, see RFC 6749 section 5.2.",
        "required": ["error"],
        "properties": {
            "error": {
                "type": "string",
                "enum": [
                    "invalid_request",
                    "invalid_client",
                    "invalid_grant",
                    "unauthorized_client",
                    "unsupported_grant_type",
                    "invalid_scope",
                ],
                "description": "Error code of the token endpoint, see RFC 6749 section 5.2.",
                "example": "invalid_client",
            },
            "error_description": {
                "type": "string",
                "description": "What went wrong, for a person reading it.",
                "example": "Client authentication failed.",
            },
            "error_uri": {
                "type": "string",
                "description": "Address of a page describing the error.",
                "format": "uri",
                "example": "https://ivpk.github.io/uapi",
            },
        },
    },
    "token": {
        "type": "object",
        "description": "An OAuth 2.0 access token, see RFC 6749 section 5.1.",
        # `access_token` and `token_type` are required by RFC 6749, so a
        # response without them is not a successful token response.
        "required": ["access_token", "token_type"],
        "properties": {
            "access_token": {
                "type": "string",
                "description": "Access token to be used as a `Bearer` token.",
                "example": "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2dldC5kYXRhLmdvdi5sdCJ9.-",
            },
            "token_type": {"type": "string", "enum": ["Bearer"], "example": "Bearer"},
            "expires_in": {
                "type": "integer",
                "format": "int64",
                "minimum": 0,
                "maximum": 9223372036854775807,
                "description": "Token lifetime in seconds.",
                "example": 864000,
            },
            "scope": {
                "type": "string",
                "description": "Space separated list of granted scopes.",
                "example": "uapi:/datasets/gov/rc/jadis/at280/1/:getall",
            },
        },
    },
    "image": {
        "type": "object",
        "description": "What is known about an image a property holds.",
        "properties": {
            "_id": {"type": "string", "nullable": True, "description": "Image file name"},
            "_content_type": {
                "type": "string",
                "nullable": True,
                "description": "A [Media type](https://en.wikipedia.org/wiki/Media_type) of the image.",
            },
        },
    },
}

#: Prefix of UDTS format scopes, `scope_prefix_udts` in Spinta configuration.
SCOPE_PREFIX = "uapi:/"

#: Scope templates of UDTS format, see `spinta.auth.get_scope_name`.
SCOPE_TEMPLATE = "{prefix}{name}/:{action}"
ROOT_SCOPE_TEMPLATE = "{prefix}:{action}"

#: Action each read operation authorizes against, see `spinta.urlparams.get_action`.
#: A collection is read with `getall`, or with `search` when the request narrows
#: it down with query parameters. Both are emitted as alternative security
#: requirements, because OpenAPI can not make a requirement depend on query
#: parameters. Requiring both instead would deny a token Spinta accepts, while
#: this way a request Spinta denies is denied by Spinta, so do not "fix" it into
#: one requirement holding both scopes.
PATH_TYPE_ACTIONS = {
    "collection": (Action.GETALL, Action.SEARCH),
    "single": (Action.GETONE,),
    "property": (Action.GETONE,),
    "propertyRef": (Action.GETONE,),
    "objectProperty": (Action.GETONE,),
}

#: Path types of one property, which authorize against that property.
PROPERTY_PATH_TYPES = frozenset(["property", "propertyRef", "objectProperty"])

SCOPE_DESCRIPTION = "Access to the data of this data service."

#: `security` in path configs references `UAPI_auth`, which has to be declared
#: in `components.securitySchemes`. Token URL comes from `--udts-cfg`, scopes
#: are collected from the operations that request them.
SECURITY_SCHEMES = {
    "UAPI_auth": {
        "type": "oauth2",
        "description": "OAuth 2.0 `client_credentials` grant. Scopes are named after the data being accessed. Access tokens are JWTs, handled as RFC8725 requires: signed with one of RS256, RS384, RS512, ES256, ES384 or ES512, never accepted unsigned, and checked against the issuer and the audience.",
        "flows": {
            "clientCredentials": {
                "tokenUrl": "",
                "scopes": {},
            }
        },
    },
    "UAPI_client": {
        "type": "http",
        "scheme": "basic",
        "description": "Client identifier and secret, used to get an access token. Basic authentication is what RFC 6749 section 2.3.1 defines for the token endpoint, and it is used there alone. That section asks for TLS, which is ensured where the service is deployed rather than stated by this document.",
    },
}
