"""`implied` of the elements of a `simple` document (4.6): the keys Spinta derives for each element.

The expected values are the `implied` of the examples of 5.1 and 5.2 in `.tmp/specs/exchange.md`, except the
`access` of an enum item: 4.6 says `access` is the effective one (given, else inherited), so an enum item with
no access of its own takes that of its owner. The golden of 5.2 has `null` there, and the enum tests below
check the rule.
"""

import copy

import pytest

from spinta.lib import Spinta
from tests.manifests.simple.test_read import CSV_TABLE

# The table of 5.2, as an ASCII table (the format of `spinta copy` on a `.txt` manifest).
TABLE_52 = """\
id  | d                    | r         | b    | m          | property       | type            | ref           | source       | prepare           | count | level | status  | visibility | access  | uri                                  | title           | description
    |                      |           |      |            |                | prefix          | schema        |              |                   |       |       |         |            |         | https://schema.org/                  | Schema.org      |
    |                      |           |      |            |                |                 | xsd           |              |                   |       |       |         |            |         | http://www.w3.org/2001/XMLSchema#    |                 |
    |                      |           |      |            |                | enum            | lt_status     | A            | "A"               |       |       |         |            | open    |                                      | Active          |
    |                      |           |      |            |                |                 |               | I            | "I"               |       |       |         |            |         |                                      | Inactive        |
    |                      | lt_db     |      |            |                | sql             |               | sqlite://    |                   |       |       |         |            |         |                                      | Shared database |
    | datasets/gov         |           |      |            |                | ns              |               |              |                   |       |       |         |            |         |                                      | Gov             |
ds1 | datasets/gov/example |           |      |            |                |                 |               |              |                   | 2     | 4     |         |            | open    |                                      | Example         |
    |                      |           |      |            |                | lang            | lt            |              |                   |       |       |         |            |         |                                      | Pavyzdys        |
    |                      |           |      |            |                | prefix          | dct           |              |                   |       |       |         |            |         | http://purl.org/dc/terms/            |                 |
    |                      |           |      |            |                |                 | skos          |              |                   |       |       |         |            |         | http://www.w3.org/2004/02/skos/core# |                 |
    |                      | resource1 |      |            |                | sql             |               | sqlite://    |                   |       |       |         |            |         |                                      | DB              |
    |                      |           |      |            |                | param           | api_key       | X-Key        | header("X-Key")   |       |       |         |            |         |                                      | API key         |
    |                      |           |      |            |                |                 |               | X-Other      | header("X-Other") |       |       |         |            |         |                                      |                 |
c1  |                      |           |      |            |                | comment         |               | jonas        |                   |       |       |         |            | private |                                      | 2024-05-01      | Check the resource
m1  |                      |           |      | Country    |                |                 | id, code      | COUNTRY      | status="A"        | 3     | 3     | develop | public     | open    | schema:Country                       | Country         |
    |                      |           |      |            |                | unique          | code          |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            |                | scope           | read          |              | eq(code, "LT")    |       |       |         |            | open    |                                      |                 |
p1  |                      |           |      |            | id             | integer         |               | ID           |                   |       | 3     |         |            | open    |                                      |                 |
    |                      |           |      |            | code           | string required |               | CODE         |                   |       |       |         |            |         | schema:identifier                    |                 |
    |                      |           |      |            | name           | text            |               | NAME         |                   |       | 3     |         |            |         | schema:name                          |                 |
    |                      |           |      |            | name@lt        | string          |               | NAME_LT      |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | name@en        | string          |               | NAME_EN      |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | status         | string          | lt_status     | STATUS       |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | kind           | string          |               | KIND         |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            |                | enum            |               | s            | "s"               |       |       |         |            |         |                                      | Small           |
    |                      |           |      |            |                |                 |               | l            | "l"               |       |       |         |            |         |                                      | Large           |
    |                      |           |      |            | height         | integer         | m             | HEIGHT       |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | tags[]         | string          |               | TAGS         | split(",")        |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | address        | object          |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | address.street | string          |               | STREET       |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | geo            | geometry(3346)  |               | GEO          |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | cities         | backref         | City[country] |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | title          | string          |               | TITLE        | swap("a", "b")    |       |       |         |            |         |                                      | Title           |
    |                      |           |      |            |                |                 |               |              | swap("c", "d")    |       |       |         |            |         |                                      |                 |
    |                      |           |      |            |                | comment         |               |              |                   |       |       |         |            | open    |                                      |                 | Prop comment
    |                      |           |      | City       |                |                 | id            | CITY         |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | id             | integer         |               | ID           |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | name           | string          |               | NAME         |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | label@lt       | string          |               | LABEL_LT     |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | country        | ref             | Country       | COUNTRY_ID   |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | country.name   |                 |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | country2       | ref             | Country[code] | COUNTRY_CODE |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | countries[]    | ref             | Country       |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           | City |            |                |                 | id            |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      | Capital    |                |                 | id            |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | id             |                 |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | since          | date            |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           | /    |            |                |                 |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      | Plain      |                |                 |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | n              | integer         |               |              |                   |       |       |         |            |         |                                      |                 |
    | /                    |           |      |            |                |                 |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      | Standalone |                |                 |               |              |                   |       |       |         |            |         |                                      |                 |
    |                      |           |      |            | n              | string          |               |              |                   |       |       |         |            |         |                                      |                 |
"""

# 4.6: the keys of each type, in the order the spec lists them.
KEYS = {
    "ns": ["name"],
    "backend": ["name", "dsn"],
    "prefix": ["name"],
    "enum": ["name", "enum", "source", "prepare", "level", "access", "status", "visibility", "count"],
    "dataset": ["name", "level", "access", "count"],
    "resource": ["name", "backend", "prepare", "level", "access", "source_params"],
    "base": ["name", "model", "ref", "level"],
    "model": [
        "name",
        "resource",
        "base",
        "level",
        "access",
        "status",
        "visibility",
        "count",
        "uri",
        "title",
        "description",
        "eli",
        "source",
        "prepare",
        "ref",
        "unique",
    ],
    "property": [
        "name",
        "model",
        "base",
        "type",
        "type_args",
        "required",
        "unique",
        "level",
        "access",
        "status",
        "visibility",
        "uri",
        "title",
        "description",
        "eli",
        "source",
        "prepare",
        "ref_model",
        "ref",
        "enum",
        "unit",
    ],
    "param": ["name", "sources", "formulas"],
    "append": ["name", "source", "prepare"],
    "unique": ["name", "ref"],
    "scope": ["name", "prepare", "level", "access", "status", "visibility", "count", "uri"],
    "lang": ["name"],
    "comment": ["name", "author", "created", "comment", "reply_to", "access", "level"],
}

# The `implied` of the examples of 5.1 (every element) and of 5.2 (the elements that show each kind).
EXPECTED_51 = {
    ("prefix", "datasets/gov/rc/ar/uapi/addr/dct"): {"name": "dct"},
    ("prefix", "datasets/gov/rc/ar/uapi/addr/dcat"): {"name": "dcat"},
    ("prefix", "datasets/gov/rc/ar/uapi/addr/foaf"): {"name": "foaf"},
    ("prefix", "datasets/gov/rc/ar/uapi/addr/rdfs"): {"name": "rdfs"},
    ("prefix", "datasets/gov/rc/ar/uapi/addr/skos"): {"name": "skos"},
    ("prefix", "datasets/gov/rc/ar/uapi/addr/cv"): {"name": "cv"},
    ("dataset", "datasets/gov/rc/ar/uapi/addr"): {
        "name": "datasets/gov/rc/ar/uapi/addr",
        "level": None,
        "access": "private",
        "count": None,
    },
    ("model", "datasets/gov/rc/ar/uapi/addr/Country"): {
        "name": "Country",
        "resource": None,
        "base": None,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "count": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref": ["datasets/gov/rc/ar/uapi/addr/Country/id"],
        "unique": [["datasets/gov/rc/ar/uapi/addr/Country/id"]],
        "title": None,
        "description": None,
        "eli": None,
    },
    ("model", "datasets/gov/rc/ar/uapi/addr/City"): {
        "name": "City",
        "resource": None,
        "base": None,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "count": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref": ["datasets/gov/rc/ar/uapi/addr/City/id"],
        "unique": [["datasets/gov/rc/ar/uapi/addr/City/id"]],
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/Country/id"): {
        "name": "id",
        "model": "datasets/gov/rc/ar/uapi/addr/Country",
        "base": None,
        "type": "integer",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/Country/code"): {
        "name": "code",
        "model": "datasets/gov/rc/ar/uapi/addr/Country",
        "base": None,
        "type": "string",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/Country/name"): {
        "name": "name",
        "model": "datasets/gov/rc/ar/uapi/addr/Country",
        "base": None,
        "type": "text",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/Country/name@lt"): {
        "name": "name@lt",
        "model": "datasets/gov/rc/ar/uapi/addr/Country",
        "base": None,
        "type": "string",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/City/id"): {
        "name": "id",
        "model": "datasets/gov/rc/ar/uapi/addr/City",
        "base": None,
        "type": "integer",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/City/name"): {
        "name": "name",
        "model": "datasets/gov/rc/ar/uapi/addr/City",
        "base": None,
        "type": "text",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/City/name@lt"): {
        "name": "name@lt",
        "model": "datasets/gov/rc/ar/uapi/addr/City",
        "base": None,
        "type": "string",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/rc/ar/uapi/addr/City/country"): {
        "name": "country",
        "model": "datasets/gov/rc/ar/uapi/addr/City",
        "base": None,
        "type": "ref",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": "datasets/gov/rc/ar/uapi/addr/Country",
        "ref": ["datasets/gov/rc/ar/uapi/addr/Country/id"],
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
}

EXPECTED_52 = {
    ("ns", "datasets/gov"): {"name": "datasets/gov"},
    ("backend", "lt_db"): {"name": "lt_db", "dsn": "sqlite://"},
    ("prefix", "schema"): {"name": "schema"},
    ("prefix", "datasets/gov/example/dct"): {"name": "dct"},
    ("dataset", "datasets/gov/example"): {"name": "datasets/gov/example", "level": 4, "access": "open", "count": 2},
    ("lang", "datasets/gov/example/lt"): {"name": "lt"},
    ("resource", "datasets/gov/example/resource1"): {
        "name": "resource1",
        "backend": None,
        "prepare": None,
        "level": None,
        "access": "open",
        "source_params": [],
    },
    ("param", "datasets/gov/example/resource1/api_key"): {
        "name": "api_key",
        "sources": ["X-Key", "X-Other"],
        "formulas": [{"name": "header", "args": ["X-Key"]}, {"name": "header", "args": ["X-Other"]}],
    },
    ("append", "datasets/gov/example/resource1/api_key#2"): {
        "name": None,
        "source": "X-Other",
        "prepare": {"name": "header", "args": ["X-Other"]},
    },
    ("append", "datasets/gov/example/Country/title#2"): {
        "name": None,
        "source": None,
        "prepare": {"name": "swap", "args": ["c", "d"]},
    },
    ("base", "datasets/gov/example/City"): {
        "name": "City",
        "model": "datasets/gov/example/City",
        "ref": ["datasets/gov/example/City/id"],
        "level": None,
    },
    ("model", "datasets/gov/example/Country"): {
        "name": "Country",
        "resource": "datasets/gov/example/resource1",
        "base": None,
        "level": 3,
        "access": "open",
        "status": "develop",
        "visibility": "public",
        "count": 3,
        "uri": "https://schema.org/Country",
        "source": "COUNTRY",
        "prepare": {"type": "expression", "name": "eq", "args": [{"name": "bind", "args": ["status"]}, "A"]},
        "ref": ["datasets/gov/example/Country/id", "datasets/gov/example/Country/code"],
        "unique": [
            ["datasets/gov/example/Country/id", "datasets/gov/example/Country/code"],
            ["datasets/gov/example/Country/code"],
        ],
        "title": "Country",
        "description": None,
        "eli": None,
    },
    ("model", "datasets/gov/example/Capital"): {
        "name": "Capital",
        "resource": "datasets/gov/example/resource1",
        "base": "datasets/gov/example/City",
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "count": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref": ["datasets/gov/example/Capital/id"],
        "unique": [["datasets/gov/example/Capital/id"]],
        "title": None,
        "description": None,
        "eli": None,
    },
    ("model", "Standalone"): {
        "name": "Standalone",
        "resource": None,
        "base": None,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "count": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref": [],
        "unique": [],
        "title": None,
        "description": None,
        "eli": None,
    },
    ("unique", "datasets/gov/example/Country/code"): {"name": "code", "ref": ["datasets/gov/example/Country/code"]},
    ("scope", "datasets/gov/example/Country/read"): {
        "name": "read",
        "prepare": {"name": "eq", "args": [{"name": "bind", "args": ["code"]}, "LT"]},
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "count": None,
        "uri": None,
    },
    ("comment", "datasets/gov/example/resource1/#1"): {
        "name": None,
        "author": "jonas",
        "created": "2024-05-01",
        "comment": "Check the resource",
        "reply_to": None,
        "access": "private",
        "level": None,
    },
    ("comment", "datasets/gov/example/Country/title/#1"): {
        "name": None,
        "author": None,
        "created": None,
        "comment": "Prop comment",
        "reply_to": None,
        "access": "open",
        "level": None,
    },
    ("property", "datasets/gov/example/Country/id"): {
        "name": "id",
        "model": "datasets/gov/example/Country",
        "base": None,
        "type": "integer",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": 3,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": "ID",
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Country/name"): {
        "name": "name",
        "model": "datasets/gov/example/Country",
        "base": None,
        "type": "text",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": 3,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": "https://schema.org/name",
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Country/name@C"): {
        "name": "name@C",
        "model": "datasets/gov/example/Country",
        "base": None,
        "type": "string",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": "NAME",
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Country/tags[]"): {
        "name": "tags[]",
        "model": "datasets/gov/example/Country",
        "base": None,
        "type": "string",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": "TAGS",
        "prepare": {"name": "split", "args": [","]},
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Country/geo"): {
        "name": "geo",
        "model": "datasets/gov/example/Country",
        "base": None,
        "type": "geometry",
        "type_args": ["3346"],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": "GEO",
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Country/cities"): {
        "name": "cities",
        "model": "datasets/gov/example/Country",
        "base": None,
        "type": "backref",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": "datasets/gov/example/City",
        "ref": ["datasets/gov/example/City/country"],
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Country/title"): {
        "name": "title",
        "model": "datasets/gov/example/Country",
        "base": None,
        "type": "string",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": "TITLE",
        "prepare": {"type": "method", "name": "swap", "args": [{"name": "swap", "args": ["a", "b"]}, "c", "d"]},
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": "Title",
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/City/country"): {
        "name": "country",
        "model": "datasets/gov/example/City",
        "base": None,
        "type": "ref",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": "COUNTRY_ID",
        "prepare": None,
        "ref_model": "datasets/gov/example/Country",
        "ref": ["datasets/gov/example/Country/id", "datasets/gov/example/Country/code"],
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/City/country.name"): {
        "name": "country.name",
        "model": "datasets/gov/example/City",
        "base": None,
        "type": "denorm",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": "datasets/gov/example/Country",
        "ref": ["datasets/gov/example/Country/name"],
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/City/countries[]"): {
        "name": "countries[]",
        "model": "datasets/gov/example/City",
        "base": None,
        "type": "ref",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": "datasets/gov/example/Country",
        "ref": ["datasets/gov/example/Country/id", "datasets/gov/example/Country/code"],
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Capital/id"): {
        "name": "id",
        "model": "datasets/gov/example/Capital",
        "base": "datasets/gov/example/City",
        "type": "inherit",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": "datasets/gov/example/City",
        "ref": ["datasets/gov/example/City/id"],
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "datasets/gov/example/Capital/since"): {
        "name": "since",
        "model": "datasets/gov/example/Capital",
        "base": "datasets/gov/example/City",
        "type": "date",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "open",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
    ("property", "Standalone/n"): {
        "name": "n",
        "model": "Standalone",
        "base": None,
        "type": "string",
        "type_args": [],
        "required": False,
        "unique": False,
        "level": None,
        "access": "private",
        "status": None,
        "visibility": None,
        "uri": None,
        "source": None,
        "prepare": None,
        "ref_model": None,
        "ref": None,
        "enum": None,
        "unit": None,
        "title": None,
        "description": None,
        "eli": None,
    },
}


def _implied(doc, type_, path):
    return doc[type_][path]["implied"]


@pytest.fixture(scope="module")
def spinta():
    return Spinta()


@pytest.fixture(scope="module")
def doc_51(spinta):
    return spinta.copy(CSV_TABLE.encode("utf-8"), "csv")


@pytest.fixture(scope="module")
def doc_52(spinta):
    return spinta.copy(TABLE_52.encode("utf-8"), "ascii")


@pytest.mark.parametrize("key", sorted(EXPECTED_51))
def test_implied_of_5_1_matches_the_spec(doc_51, key):
    type_, path = key
    assert _implied(doc_51, type_, path) == EXPECTED_51[key]


@pytest.mark.parametrize("key", sorted(EXPECTED_52))
def test_implied_of_5_2_matches_the_spec(doc_52, key):
    type_, path = key
    assert _implied(doc_52, type_, path) == EXPECTED_52[key]


def test_every_element_has_every_key_of_its_type(doc_52):
    for type_, elements in doc_52.items():
        if type_ in ("spinta", "manifest"):
            continue
        for path, element in elements.items():
            if element["implied"] is None:
                continue
            assert list(element["implied"]) == KEYS[type_], (type_, path)


def test_manifest_has_no_implied(doc_52):
    assert all(e["implied"] is None for e in doc_52["manifest"].values())


def test_downgraded_reference_has_implied_as_an_object(spinta):
    # The case of a `ref` to a model that is not in the document: copy downgrades it (3.4).
    text = (
        "id,dataset,resource,base,model,property,type,ref,access\n"
        "1,datasets/gov/rc/gr/at605_isplestinis,,,,,,,\n"
        "2,,,,Document,,,id,\n"
        "3,,,,,id,integer,,\n"
        "4,,,,,dokumentai,ref,/datasets/gov/rc/gr/at588_pagrindinis_asmens/Asmuo,protected\n"
    )
    doc = spinta.copy(text.encode("utf-8"), "csv")
    prop = doc["property"]["datasets/gov/rc/gr/at605_isplestinis/Document/dokumentai"]
    assert [e["code"] for e in prop["errors"]] == ["UnresolvedReference"]
    assert prop["errors"][0]["severity"] == "warning"
    implied = prop["implied"]
    assert implied is not None
    assert implied["type"] == "object"
    assert implied["ref_model"] is None
    assert implied["ref"] is None
    assert implied["enum"] is None
    assert implied["access"] == "protected"
    # `given` keeps the reference as the user wrote it, so the element is rebuilt from it on read (4.7).
    assert prop["given"]["ref"] == "/datasets/gov/rc/gr/at588_pagrindinis_asmens/Asmuo"


def test_strict_check_of_unresolved_reference_has_no_target(spinta):
    # `check` is strict by default: the reference stays a `ref`, and its target is unknown (4.6).
    checked = spinta._check_document(_doc_csv(spinta, UNRESOLVED))
    implied = _implied(checked, "property", "datasets/gov/cities/City/location")
    assert implied["type"] == "ref"
    assert implied["ref_model"] is None
    assert implied["ref"] is None


def test_resolved_reference_has_its_target(doc_51):
    implied = _implied(doc_51, "property", "datasets/gov/rc/ar/uapi/addr/City/country")
    assert implied["ref_model"] == "datasets/gov/rc/ar/uapi/addr/Country"
    assert implied["ref"] == ["datasets/gov/rc/ar/uapi/addr/Country/id"]


UNRESOLVED = (
    "id,dataset,resource,base,model,property,type,ref,uri\n"
    "1,datasets/gov/cities,,,,,,,\n"
    "2,,,,City,,,id,\n"
    "3,,,,,id,integer,,\n"
    "4,,,,,location,ref,/datasets/gov/locn/Location,\n"
)


def _doc_csv(spinta, text):
    import csv
    import io

    from spinta.manifests.simple.from_tabular import tabular_to_simple

    rows = ((str(i), r) for i, r in enumerate(csv.reader(io.StringIO(text)), 1))
    return tabular_to_simple(rows, type="csv", version=spinta.version)


def test_copy_recomputes_implied_and_is_a_fixed_point(spinta, doc_51):
    # A stale `implied` in the input is replaced, and copying the output again gives the same output.
    stale = copy.deepcopy(doc_51)
    for elements in stale.values():
        if isinstance(elements, dict):
            for element in elements.values():
                if isinstance(element, dict) and element.get("implied") is not None:
                    element["implied"] = {"name": "stale"}
    again = spinta.copy(stale)
    assert again == doc_51
    assert spinta.copy(again) == doc_51


def test_name_at_c_has_the_source_moved_from_its_text_property(doc_52):
    implied_c = _implied(doc_52, "property", "datasets/gov/example/Country/name@C")
    assert implied_c["source"] == "NAME"
    assert implied_c["type"] == "string"
    element = doc_52["property"]["datasets/gov/example/Country/name@C"]
    assert element["index"] is None
    assert element["given"] is None
    assert element["parent"] == {"type": "property", "path": "datasets/gov/example/Country/name"}
    assert _implied(doc_52, "property", "datasets/gov/example/Country/name")["source"] is None


def test_comment_under_an_append_belongs_to_the_property(doc_52):
    comment = doc_52["comment"]["datasets/gov/example/Country/title/#1"]
    assert comment["parent"] == {"type": "property", "path": "datasets/gov/example/Country/title"}
    assert comment["index"] == 35


def test_enum_item_takes_the_access_of_its_owner(doc_52):
    # `kind/s` has no access of its own. Its property `kind` is open, so the item is open (4.6).
    assert _implied(doc_52, "enum", "datasets/gov/example/Country/kind/s")["access"] == "open"
    # `lt_status/I` has no access and no owner (a manifest enum), so it is private, the default.
    assert _implied(doc_52, "enum", "lt_status/I")["access"] == "private"
    # `lt_status/A` has its own access.
    assert _implied(doc_52, "enum", "lt_status/A")["access"] == "open"


def test_element_without_a_node_has_no_implied(spinta, doc_51):
    # An element that the nodes do not have is not derived: it keeps `null`, and is rebuilt from `given` on
    # read (4.7 step 2). Here the property is not in its model.
    doc = copy.deepcopy(doc_51)
    doc["property"]["datasets/gov/rc/ar/uapi/addr/Country/nowhere"] = {
        "index": 99,
        "parent": {"type": "model", "path": "datasets/gov/rc/ar/uapi/addr/Country"},
        "given": {"type": "string"},
        "implied": None,
        "errors": [],
    }
    out = spinta.copy(doc)
    assert out["property"]["datasets/gov/rc/ar/uapi/addr/Country/nowhere"]["implied"] is None
    assert _implied(out, "property", "datasets/gov/rc/ar/uapi/addr/Country/id") is not None


# An array of backrefs with the properties of its items, as the user's manifests have them: the flat properties
# of the model are `children.born_on`, and their paths are `children[].born_on` (4.4).
ARRAY_OF_BACKREFS = """\
id,dataset,resource,base,model,property,type,ref,source,prepare,access
1,ds1,,,,,,,,,open
2,,,,Person,,,id,PERSON,,
3,,,,,id,integer,,ID,,
4,,,,,mother,ref,Person,MOTHER,,
5,,,,,children,array,,CHILDREN,split(';'),
6,,,,,children[],backref,Person[mother],,self(),
7,,,,,children[].born_on,date,,BORN,,
8,,,,,children[].name,text,,,,
9,,,,,children[].name@lt,string,,NAME_LT,,
"""


def test_property_under_an_array_of_backrefs_has_implied(spinta):
    doc = spinta.copy(ARRAY_OF_BACKREFS.encode("utf-8"), "csv")
    born_on = _implied(doc, "property", "ds1/Person/children[].born_on")
    assert born_on["name"] == "children[].born_on"
    assert born_on["type"] == "date"
    assert born_on["source"] == "BORN"
    assert born_on["access"] == "open"
    backref = _implied(doc, "property", "ds1/Person/children[]")
    assert backref["type"] == "backref"
    assert backref["ref_model"] == "ds1/Person"
    assert backref["ref"] == ["ds1/Person/mother"]
    assert all(element["implied"] is not None for element in doc["property"].values())
