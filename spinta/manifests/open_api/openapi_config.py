PROPERTY_TYPES_IN_PATHS = {"file", "image"}

VERSION = "3.1.0"
INFO = {
    "version": "1.0.0",
    "title": "Universal application programming interface",
    "contact": {"name": "VSSA", "url": "https://vssa.lrv.lt/", "email": "info@vssa.lt"},
    "license": {"name": "CC-BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0/"},
    "summary": "Universal API specification, provided as OpenAPI JSON file for Lithuanian\ngovernment institutions as a template for implementing API's for data\nexchange in a standardized and interoperable manner.\n",
    "description": "",
}

EXTERNAL_DOCS = {"url": "https://ivpk.github.io/uapi"}

BASE_TAGS = [{"name": "utility", "description": "Utility operations performed on the API itself"}]

SERVERS = [
    {"url": "get.data.gov.lt", "description": "Data access server"},
]

CHANGE_SCHEMA_EXAMPLE = {
    "_cid": {"type": "integer", "example": 11},
    "_id": {"type": "string", "format": "uuidv4", "example": "abdd1245-bbf9-4085-9366-f11c0f737c1d"},
    "_rev": {"type": "string", "format": "uuidv4", "example": "16dabe62-61e9-4549-a6bd-07cecfbc3508"},
    "_txn": {"type": "string", "example": "792a5029-63c9-4c07-995c-cbc063aaac2c"},
    "_created": {"type": "string", "format": "date-time", "example": "2021-07-30T14:03:14.645198"},
    "_op": {"type": "string", "enum": ["insert", "patch", "delete"]},
    "_objectType": {"type": "string", "example": "ExampleModel"},
}

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
    "file": {"_name": "example.pdf", "_content_type": "application/pdf", "_size": 1024},
    "geometry": "POINT (6088198 505579)",
    "money": 99.99,
}

STANDARD_OBJECT_PROPERTIES = {
    "_type": {"type": "string"},
    "_id": {"type": "string", "format": "uuidv4"},
    "_revision": {"type": "string"},
}

PROPERTY_MAPPING = {
    "string": {"type": "string"},
    "integer": {"type": "integer"},
    "number": {"type": "number"},
    "boolean": {"type": "boolean"},
    "datetime": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
    "time": {"type": "string", "format": "time"},
    "text": {"type": "string"},
    "binary": {"type": "string", "format": "binary"},
    "file": {"type": "string", "format": "binary"},
    "geometry": {"type": "string", "description": "Geometry data in WKT format"},
    "money": {"type": "number"},
}

COMMON_RESPONSE_HEADERS = ["ETag", "Content-Type", "Content-Length"]

PATHS_CONFIG = {
    "/version": {
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
                "503": {"$ref": "error503"},
            },
        },
    },
    "/health": {
        "parameters": ["traceparent", "tracestate"],
        "get": {
            "tags": ["utility"],
            "security": [{}],
            "summary": "Perform the API health check",
            "description": "Performs API helth check with a check of the underlying system health\n",
            "operationId": "apiHealth",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {"application/json": {"schema": "health"}},
                },
                "400": {"$ref": "error400"},
                "500": {"$ref": "error500"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}": {
        "parameters": ["traceparent", "tracestate", "Cache-Control", "If-None-Match", "Accept-Language"],
        "head": {
            "security": [{}],
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "modelHead",
            "responses": {
                "200": {"description": "OK"},
                "400": {"$ref": "error400"},
                "500": {"$ref": "error500"},
                "503": {"$ref": "error503"},
            },
        },
        "get": {
            "security": [{"UAPI_auth": ["uapi:/"]}],
            "summary": "Get multiple objects.",
            "description": "Return list of objects for a given model.\n",
            "operationId": "getAll",
            "parameters": ["query"],
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
                    "headers": COMMON_RESPONSE_HEADERS,
                },
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}/{id}": {
        "parameters": ["id", "traceparent", "tracestate", "If-None-Match", "Accept-Language"],
        "head": {
            "security": [{}],
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "headOne",
            "responses": {"200": {"description": "OK"}, "400": {"$ref": "error400"}},
        },
        "get": {
            "security": [{"UAPI_auth": ["uapi:/"]}],
            "summary": "Get a single object by given {id}.",
            "description": "Retrieve a single specific object based on it's unique object identifier {id}\n",
            "operationId": "getOne",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {
                        "application/json": {"schema": "object"},
                    },
                },
                "304": {
                    "description": "Not Modified",
                    "headers": COMMON_RESPONSE_HEADERS,
                },
                "400": {"$ref": "error400"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}/{id}/{field}": {
        "parameters": ["id", "traceparent", "tracestate", "If-None-Match", "Accept-Language", "property"],
        "head": {
            "security": [{}],
            "summary": "Return only headers for the API.",
            "description": "`HEAD` method requests the headers that would be returned if the HEAD request's URL was instead requested with the `GET` method.\n",
            "operationId": "headProperty",
            "responses": {
                "200": {"description": "OK"},
                "400": {"$ref": "error400"},
                "500": {"$ref": "error500"},
                "503": {"$ref": "error503"},
            },
        },
        "get": {
            "security": [{"UAPI_auth": ["uapi:/"]}],
            "summary": "For a given specific object by {id}, retrieve a {property} from it's structure (subresource).",
            "description": "Retrieve a specific property from an object structure.\n\nBy default when retrieving object you recive all data items from it's structure, using this service you retrieve a specific property from it's structure.\n\nIf provided {property} is a file instead of getting the data, file is provided instead as binary bit stream.\n",
            "operationId": "getProperty",
            "responses": {
                "200": {
                    "description": "OK",
                    "headers": COMMON_RESPONSE_HEADERS,
                    "content": {
                        "application/json": {
                            "schema": "oneOf[absent,boolean,integer,number,binary,string,text,datetime,date,time,temporal,geometry,money,file,ref,backref,array,url,uri,object]"
                        }
                    },
                },
                "304": {
                    "description": "Not Modified",
                    "headers": COMMON_RESPONSE_HEADERS,
                },
                "400": {"$ref": "error400"},
                "401": {"$ref": "error401"},
                "403": {"$ref": "error403"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "503": {"$ref": "error503"},
            },
        },
    },
    "/{model_name}/:changes/{cid}": {
        "parameters": ["cid", "traceparent", "tracestate", "If-None-Match", "Accept-Language"],
        "get": {
            "security": [{}],
            "summary": "Get all object changes since given {cid} (change id).",
            "description": "Get latest changes to a table.\n\nIf {cid} is not given, return changes, since very first available\nchange.\n\nIf {cid} is gven, return only changes, since given change id, including\nchange id itself.\n\nThis API can return changes, that were returned previously, client\nshould be responsible for checking if a change was received previously\nor not.\n\nLast change id is included in the request, in order for clients to check\nif last change id matches change received by client. If last change\ndoes not match, then client should do a full synce, because if last\nchange id does not match, that means, that a data migration or some\nother alterations to data were made, which requires to do a full sync.\n",
            "operationId": "getChanges",
            "responses": {
                "200": {"description": "OK", "content": {"application/json": {"schema": "changes"}}},
                "400": {"$ref": "error400"},
                "404": {"$ref": "error404"},
                "500": {"$ref": "error500"},
                "503": {"$ref": "error503"},
            },
        },
    },
}

RESPONSE_COMPONENTS = {
    "error400": {
        "description": "Bad Request",
        "headers": [],
        "content": {
            "application/json": {"schema": {"oneOf": ["UniqueConstraint", "NoItemRevision", "InvalidOperandValue"]}}
        },
    },
    "error401": {
        "description": "Bad Request",
        "headers": [],
        "content": {"application/json": {"schema": {"oneOf": ["AuthorizedClientsOnly", "InvalidToken"]}}},
    },
    "error403": {
        "description": "Forbidden",
        "headers": [],
        "content": {"application/json": {"schema": "Forbidden"}},
    },
    "error404": {
        "description": "Not Found",
        "headers": [],
        "content": {"application/json": {"schema": "ItemDoesNotExist"}},
    },
    "error409": {
        "description": "Bad Request",
        "headers": [],
        "content": {"application/json": {"schema": "ConflictingValue"}},
    },
    "error500": {
        "description": "Internal Server Error",
        "headers": [],
        "content": {"application/json": {"schema": {"oneOf": ["UnhandledException", "MultipleRowsFound"]}}},
    },
    "error503": {
        "description": "Service Unavailable",
        "headers": [],
        "content": {"application/json": {"schema": "ServiceNotAvailable"}},
    },
}

HEADER_COMPONENTS = {
    "Content-Type": {
        "description": "The `Content-Type` header indicates the media type of the resource or data. For responses, it tells the client what the content type of the returned content actually is.",
        "required": True,
        "schema": {"type": "string", "examples": ["application/json", "text/csv", "application/xml"]},
    },
    "Content-Length": {
        "description": "The `Content-Length` header indicates the size of the response body, in bytes, sent to the recipient.",
        "required": False,
        "schema": {"type": "integer", "minimum": 0, "examples": [1024, 8021]},
    },
    "ETag": {
        "description": "`ETag` header is an entity tag that uniquely represents the requested resource. It is a revision number for this item.",
        "required": False,
        "schema": {"type": "string", "examples": ["16dabe62-61e9-4549-a6bd-07cecfbc3508"]},
    },
}

PARAMETER_COMPONENTS = {
    "traceparent": {
        "name": "traceparent",
        "in": "header",
        "description": "The `traceparent` request header represents the incoming request in a tracing system in a common format, understood by all vendors. For more context check [***trace-context***](https://w3c.github.io/trace-context/) documentation.",
        "required": True,
        "schema": {
            "type": "string",
            "pattern": "^[0-9]{2}-[a-f0-9]{32}-[a-f0-9]{16}-[0-9]{2}",
            "description": "Consists of `version` `trace-id` `parent-id` `trace-flags` separated by `-`. \n\n`trace-id` recommended to be in UUIDv4",
            "examples": ["00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01"],
        },
    },
    "tracestate": {
        "name": "tracestate",
        "in": "header",
        "required": True,
        "description": "The main purpose of the `tracestate` HTTP header is to provide additional vendor-specific trace identification information across different distributed tracing systems and is a companion header for the `traceparent` field. It also conveys information about the request's position in multiple distributed tracing graphs.\nFor more context check [***trace-context***](https://w3c.github.io/trace-context/) documentation.",
        "schema": {
            "type": "string",
            "description": "Consists of a `list` of `list-members` separated by commas (`,`)",
            "examples": ["rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"],
        },
    },
    "Cache-Control": {
        "name": "Cache-Control",
        "in": "header",
        "required": False,
        "description": "`Cache-Control` header should be used if service supports caching. It allows the user to provide directives from their side. `no-cache` can be used to request revalidation of data with the origin server before reuse. `no-store` can be used to request to not store the data in caches.\n\nMultiple directives can be used separated by `, `. If they are conflicting, most restrictive directive should be honored.",
        "schema": {"type": "string", "examples": ["no-cache"]},
    },
    "If-None-Match": {
        "name": "If-None-Match",
        "in": "header",
        "required": False,
        "description": "Using `If-None-Match` client can provide a revision number of an object to server to check if modification to the object has occured, if not, server will return `304 - Not Modified`.",
        "schema": {"type": "string", "examples": ["16dabe62-61e9-4549-a6bd-07cecfbc3508"]},
    },
    "Accept-Language": {
        "name": "Accept-Language",
        "in": "header",
        "required": False,
        "description": '`Accept-Language` header is used to indicate the language preference of the user. It\'s a list of values with quality factors (e.g., `"de, en"`).',
        "schema": {"type": "string", "examples": ["lt"]},
    },
    "query": {
        "name": "query",
        "in": "query",
        "required": False,
        "description": "Object filter. This filter and the pattern used to form a querie conforms to [***URI syntax standard***](https://datatracker.ietf.org/doc/html/rfc3986).\n\nOther implementations of this specification can use more complex queries depending on filtering rules. They should comply to [***AST***](https://en.wikipedia.org/wiki/Abstract_syntax_tree) formatting and logic.",
        "schema": {
            "type": "object",
            "properties": {
                "_select": {
                    "type": "string",
                    "examples": ["name,country.name,country.continent.name"],
                    "description": "Comma separated list of properties to include in the result.",
                },
                "_limit": {
                    "type": "integer",
                    "examples": [10],
                    "description": "Limit result to given number of objects.",
                },
                "_sort": {
                    "type": "string",
                    "examples": ["-code,country.name"],
                    "description": "Comma separated list of properties, optionally prefixed with `+` or `-` operators to control sort direction.",
                },
                "_count": {"type": "string", "description": "Return number of objects matching a given filter."},
                "_page": {
                    "type": "string",
                    "description": "If _limit is set and results in multiple pages of results to be available, _page value is returned.",
                },
            },
        },
    },
    "id": {
        "name": "id",
        "in": "path",
        "required": True,
        "description": "Public global object identifier.\n\nIdentifiers should be UUID v4.\n\nOnce object is assigned a global identifier, it should never change.",
        "schema": {
            "type": "string",
            "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
            "examples": ["abdd1245-bbf9-4085-9366-f11c0f737c1d"],
        },
    },
    "property": {
        "name": "property",
        "in": "path",
        "required": True,
        "description": "Subresource of an object.\n\nAll lower case, words separated with `_` symbol.",
        "schema": {"type": "string", "examples": ["cities"]},
    },
    "cid": {
        "name": "cid",
        "in": "path",
        "required": True,
        "description": "Change id.\n\nUsed for incremental changes API, to get next changes after given change id.",
        "schema": {"type": "string"},
    },
}

COMMON_SCHEMAS = {
    "absent": {
        "type": "object",
        "description": "For objects that have been deleted during change, `type` value is changed to `absent`",
        "properties": {"type": {"type": "string", "enum": ["absent"]}},
    },
    "boolean": {
        "oneOf": [
            {"type": "string", "description": "Maturity level < 3", "examples": [True, 1, "taip"]},
            {"type": "boolean", "description": "Maturity level >= 3"},
        ],
        "description": "Logical value of true or false, depending on maturity level this value can be expressed in a non standard true/false values if maturity level is lower than 3",
    },
    "integer": {
        "type": "integer",
        "description": "A value of a whole number",
        "minimum": -2147483648,
        "maximum": 2147483647,
    },
    "number": {
        "type": "number",
        "description": "A value of a real number, based on Floating-Point Arithmetic (IEEE 754), with a decimal point marked with `.`. Whole number can be up to 6 characters in length.",
    },
    "binary": {
        "type": "string",
        "description": "Binary string of data. A single set should not exceed 1G",
        "pattern": "^[0-1]+$",
    },
    "string": {
        "type": "string",
        "description": "Non natural language strings of characters. Should be provided based on UTF-8 encoding and should not exceed 1G",
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
        "properties": {
            "_name": {"type": "string", "description": "File name"},
            "_content_type": {
                "type": "string",
                "description": "A [Media type](https://en.wikipedia.org/wiki/Media_type) of the file.",
            },
            "_size": {"type": "integer", "description": "File size in bytes."},
        },
    },
    "url": {"type": "string", "description": "Uniform Resource Locator. Used to provide links to external sources."},
    "uri": {
        "type": "string",
        "description": "Uniform Resource Identifier. Used to provide an identifier of an external resource, in an RDF data model it is subject identifier.",
    },
    "array": {
        "type": "array",
        "description": "Array of data. It is recommended to avoid this type and instead use `backref`",
    },
    "backref": {
        "type": "string",
        "description": "Backwards link showing that another model has a link to this one. This item does not hold any data",
    },
    "version": {
        "type": "object",
        "properties": {
            "api": {"type": "object", "properties": {"version": {"type": "string", "examples": ["0.0.1"]}}},
            "implementation": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "examples": ["Spinta"]},
                    "version": {"type": "string", "examples": [0.1]},
                },
            },
            "build": {"type": "object", "properties": {"version": {"type": "string", "examples": ["0.0.1"]}}},
        },
    },
    "health": {
        "type": "object",
        "properties": {
            "healthy": {"type": "boolean"},
            "dependencies": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"name": {"type": "string", "examples": ["Spinta"]}, "healthy": {"type": "boolean"}},
                },
            },
        },
    },
    "UniqueConstraint": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "UniqueConstraint"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Given value already exists."},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "NoItemRevision": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "NoItemRevision"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "_revision must be given on rewrite operation."},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "InvalidOperandValue": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "InvalidOperandValue"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Invalid operand value for {operator!r} operator."},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "AuthorizedClientsOnly": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "AuthorizedClientsOnly"},
            "type": {"type": "string", "description": "system"},
            "template": {
                "type": "string",
                "description": "This resource can only be accessed by an authorized client.",
            },
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "InvalidToken": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "InvalidToken"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Invalid token"},
            "headers": {"type": "string", "description": "{'WWW-Authenticate': 'Bearer error'='invalid_token'}"},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "Forbidden": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "Forbidden"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Access is forbidden"},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "ItemDoesNotExist": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "ItemDoesNotExist"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Resource {id!r} not found."},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "ConflictingValue": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "ConflictingValue"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Conflicting Value"},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "UnhandledException": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "UnhandledException"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Unhandled exception {exception}: {error}."},
            "context": {
                "type": "object",
                "properties": {"exception": {"type": "string", "description": "error.__class__.__name__"}},
            },
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "MultipleRowsFound": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "MultipleRowsFound"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Multiple rows were found."},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
    "ServiceNotAvailable": {
        "type": "object",
        "properties": {
            "code": {"type": "string", "description": "Service Not Available"},
            "type": {"type": "string", "description": "system"},
            "template": {"type": "string", "description": "Service is currently not available."},
            "message": {
                "type": "string",
                "description": "Message within the error object contains a more detailed description of the server errors.\nThese should include more detailed overview of the internal, business logic or request processing errors that have occurred.",
            },
        },
        "additionalProperties": True,
    },
}
