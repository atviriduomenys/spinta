import textwrap
import uuid

import sqlalchemy as sa
import sqlparse
from sqlalchemy.sql import Select

from spinta import commands
from spinta import spyna
from spinta.auth import AdminToken
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.utils import create_empty_backend
from spinta.ufuncs.loadbuilder.helpers import page_contains_unsupported_keys
from spinta.ufuncs.querybuilder.helpers import add_page_expr


def _qry(qry: Select, indent: int = 4) -> str:
    sql = str(qry) % qry.compile().params
    sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    ln = "\n"
    indent_ = " " * indent
    return ln + textwrap.indent(sql, indent_) + ln + indent_


def _build(
    rc: RawConfig,
    manifest: str,
    model_name: str,
    *,
    query: str = "",
    page_mapping: dict = None,
) -> str:
    context, manifest = load_manifest_and_context(rc, manifest)
    context.set("auth.token", AdminToken())
    backend = create_empty_backend(context, "postgresql", "default")
    backend.schema = sa.MetaData()
    backend.tables = {}
    commands.prepare(context, backend, manifest)
    model = commands.get_model(context, manifest, model_name)
    query = asttoexpr(spyna.parse(query))
    if page_mapping:
        page = commands.create_page(model.page)
        if page.enabled:
            for key, value in page_mapping.items():
                cleaned = key[1:] if key.startswith("-") else key
                page.update_value(key, model.properties.get(cleaned), value)
            if page_contains_unsupported_keys(page):
                page.enabled = False
        query = add_page_expr(query, page)
    builder = backend.query_builder_class(context)
    builder.update(model=model)
    env = builder.init(backend, backend.get_table(model))
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)
    return _qry(qry)


def test_select_id(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
    """,
            "example/City",
            query="select(_id)",
        )
        == """
    SELECT example."City"._id,
           example."City"._revision
    FROM example."City"
    """
    )


def test_filter_by_ref_id(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    """,
            "example/City",
            query='country._id="ba1f89f1-066c-4a8b-bfb4-1b65627e79bb"',
        )
        == """
    SELECT example."City".name,
           example."City"."country._id",
           example."City"._id,
           example."City"._revision
    FROM example."City"
    WHERE example."City"."country._id" = :country._id_1
    """
    )


def test_join(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    """,
            "example/City",
            query="select(name, country.name)",
        )
        == """
    SELECT example."City".name,
           "Country_1".name AS "country.name",
           example."City"._id,
           example."City"._revision
    FROM example."City"
    LEFT OUTER JOIN example."Country" AS "Country_1" ON example."City"."country._id" = "Country_1"._id
    """
    )


def test_join_and_id(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    """,
            "example/City",
            query="select(_id, country.name)",
        )
        == """
    SELECT example."City"._id,
           "Country_1".name AS "country.name",
           example."City"._revision
    FROM example."City"
    LEFT OUTER JOIN example."Country" AS "Country_1" ON example."City"."country._id" = "Country_1"._id
    """
    )


def test_join_two_refs(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Planet         |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | planet1    | ref     | Planet  | open
      |   |   |   | planet2    | ref     | Planet  | open
      |   |   | City           |         |         |
      |   |   |   | country    | ref     | Country | open
    """,
            "example/City",
            query="select(_id, country.name)",
        )
        == """
    SELECT example."City"._id,
           "Country_1".name AS "country.name",
           example."City"._revision
    FROM example."City"
    LEFT OUTER JOIN example."Country" AS "Country_1" ON example."City"."country._id" = "Country_1"._id
    """
    )


def test_join_two_refs_same_model(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Planet         |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | planet1    | ref     | Planet  | open
      |   |   |   | planet2    | ref     | Planet  | open
    """,
            "example/Country",
            query="select(planet1.name, planet2.name)",
        )
        == """
    SELECT "Planet_1".name AS "planet1.name",
           "Planet_2".name AS "planet2.name",
           example."Country"._id,
           example."Country"._revision
    FROM example."Country"
    LEFT OUTER JOIN example."Planet" AS "Planet_1" ON example."Country"."planet1._id" = "Planet_1"._id
    LEFT OUTER JOIN example."Planet" AS "Planet_2" ON example."Country"."planet2._id" = "Planet_2"._id
    """
    )


def test_paginate_all_none_values(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        """,
            "example/Planet",
            page_mapping={"name": None},
        )
        == """
    SELECT example."Planet".name,
           example."Planet"._id,
           example."Planet".code,
           example."Planet"._revision
    FROM example."Planet"
    ORDER BY example."Planet".name ASC,
             example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_half_none_values(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        """,
            "example/Planet",
            page_mapping={"name": None, "code": 0},
        )
        == """
    SELECT example."Planet".name,
           example."Planet"._id,
           example."Planet".code,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name IS NULL
      AND example."Planet"._id IS NULL
      AND (example."Planet".code > :code_1
           OR example."Planet".code IS NULL)
    ORDER BY example."Planet".name ASC,
             example."Planet"._id ASC,
             example."Planet".code ASC
    LIMIT :param_1
    """
    )


def test_paginate_half_none_values_desc(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        """,
            "example/Planet",
            page_mapping={"-name": None, "-code": 0},
        )
        == """
    SELECT example."Planet".name,
           example."Planet"._id,
           example."Planet".code,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name IS NOT NULL
      OR example."Planet".name IS NULL
      AND example."Planet"._id IS NULL
      AND example."Planet".code < :code_1
    ORDER BY example."Planet".name DESC,
             example."Planet"._id ASC,
             example."Planet".code DESC
    LIMIT :param_1
    """
    )


def test_paginate_given_values_page_and_ref_not_given(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         |         |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        """,
            "example/Planet",
            page_mapping={"_id": uuid.uuid4()},
        )
        == """
    SELECT example."Planet"._id,
           example."Planet".name,
           example."Planet".code,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet"._id > :id_1
      OR example."Planet"._id IS NULL
    ORDER BY example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_given_values_page_not_given(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        """,
            "example/Planet",
            page_mapping={"name": "test", "_id": uuid.uuid4()},
        )
        == """
    SELECT example."Planet".name,
           example."Planet"._id,
           example."Planet".code,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name > :name_1
      OR example."Planet".name IS NULL
      OR example."Planet".name = :name_2
      AND (example."Planet"._id > :id_1
           OR example."Planet"._id IS NULL)
    ORDER BY example."Planet".name ASC,
             example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_given_values_size_given(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, size: 2)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        """,
            "example/Planet",
            page_mapping={"name": "test", "_id": uuid.uuid4()},
        )
        == """
    SELECT example."Planet".name,
           example."Planet"._id,
           example."Planet".code,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name > :name_1
      OR example."Planet".name IS NULL
      OR example."Planet".name = :name_2
      AND (example."Planet"._id > :id_1
           OR example."Planet"._id IS NULL)
    ORDER BY example."Planet".name ASC,
             example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_given_values_private(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access  | prepare
        example                    |         |         |         |
          |   |   | Planet         |         | name    |         | page(name, code)
          |   |   |   | name       | string  |         | open    |
          |   |   |   | code       | integer |         | private |
        """,
            "example/Planet",
            page_mapping={"name": "test", "code": 5, "_id": uuid.uuid4()},
        )
        == """
    SELECT example."Planet".name,
           example."Planet".code,
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name > :name_1
      OR example."Planet".name IS NULL
      OR example."Planet".name = :name_2
      AND (example."Planet".code > :code_1
           OR example."Planet".code IS NULL)
      OR example."Planet".name = :name_3
      AND example."Planet".code = :code_2
      AND (example."Planet"._id > :id_1
           OR example."Planet"._id IS NULL)
    ORDER BY example."Planet".name ASC,
             example."Planet".code ASC,
             example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_given_values_two_keys(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, code)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        """,
            "example/Planet",
            page_mapping={"name": "test", "code": 5, "_id": uuid.uuid4()},
        )
        == """
    SELECT example."Planet".name,
           example."Planet".code,
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name > :name_1
      OR example."Planet".name IS NULL
      OR example."Planet".name = :name_2
      AND (example."Planet".code > :code_1
           OR example."Planet".code IS NULL)
      OR example."Planet".name = :name_3
      AND example."Planet".code = :code_2
      AND (example."Planet"._id > :id_1
           OR example."Planet"._id IS NULL)
    ORDER BY example."Planet".name ASC,
             example."Planet".code ASC,
             example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_given_values_five_keys(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, code, float, user, pass)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
          |   |   |   | float      | number  |         | open   |
          |   |   |   | user       | string  |         | open   |
          |   |   |   | pass       | string  |         | open   |
        """,
            "example/Planet",
            page_mapping={"name": "test", "code": 5, "float": 1.5, "user": "test", "pass": "test", "_id": uuid.uuid4()},
        )
        == """
    SELECT example."Planet".name,
           example."Planet".code,
           example."Planet".float,
           example."Planet"."user",
           example."Planet".pass,
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name > :name_1
      OR example."Planet".name IS NULL
      OR example."Planet".name = :name_2
      AND (example."Planet".code > :code_1
           OR example."Planet".code IS NULL)
      OR example."Planet".name = :name_3
      AND example."Planet".code = :code_2
      AND (example."Planet".float > :float_1
           OR example."Planet".float IS NULL)
      OR example."Planet".name = :name_4
      AND example."Planet".code = :code_3
      AND example."Planet".float = :float_2
      AND (example."Planet"."user" > :user_1
           OR example."Planet"."user" IS NULL)
      OR example."Planet".name = :name_5
      AND example."Planet".code = :code_4
      AND example."Planet".float = :float_3
      AND example."Planet"."user" = :user_2
      AND (example."Planet".pass > :pass_1
           OR example."Planet".pass IS NULL)
      OR example."Planet".name = :name_6
      AND example."Planet".code = :code_5
      AND example."Planet".float = :float_4
      AND example."Planet"."user" = :user_3
      AND example."Planet".pass = :pass_2
      AND (example."Planet"._id > :id_1
           OR example."Planet"._id IS NULL)
    ORDER BY example."Planet".name ASC,
             example."Planet".code ASC,
             example."Planet".float ASC,
             example."Planet"."user" ASC,
             example."Planet".pass ASC,
             example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_desc(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, -code)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        """,
            "example/Planet",
            page_mapping={"name": "test", "-code": 5, "_id": uuid.uuid4()},
        )
        == """
    SELECT example."Planet".name,
           example."Planet".code,
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    WHERE example."Planet".name > :name_1
      OR example."Planet".name IS NULL
      OR example."Planet".name = :name_2
      AND example."Planet".code < :code_1
      OR example."Planet".name = :name_3
      AND example."Planet".code = :code_2
      AND (example."Planet"._id > :id_1
           OR example."Planet"._id IS NULL)
    ORDER BY example."Planet".name ASC,
             example."Planet".code DESC,
             example."Planet"._id ASC
    LIMIT :param_1
    """
    )


def test_paginate_disabled(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page()
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        """,
            "example/Planet",
            page_mapping={"name": "test"},
        )
        == """
    SELECT example."Planet".name,
           example."Planet".code,
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    """
    )


# This is prone to change, when more supported types are added
# list of valid types are declared in: get_allowed_page_property_types
# Currently valid types are: Integer, Number, String, Date, DateTime, Time, PrimaryKey
def test_paginate_invalid_types(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | access | prepare
    example                    |         |         |        |
      |   |   | Planet         |         |         |        |
      |   |   |   | name       | string  |         | open   | 
      |   |   | Country        |         |         |        | page(planet)
      |   |   |   | name       | string  |         | open   |
      |   |   |   | planet     | ref     | Planet  | open   |
        """,
            "example/Country",
            page_mapping={"planet": "test"},
        )
        == """
    SELECT example."Country".name,
           example."Country"."planet._id",
           example."Country"._id,
           example."Country"._revision
    FROM example."Country"
    """
    )

    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type     | ref     | access | prepare
    example                    |          |         |        |
      |   |   | Country        |          |         |        | page(geo)
      |   |   |   | name       | string   |         | open   |
      |   |   |   | geo        | geometry |         | open   |
        """,
            "example/Country",
            page_mapping={"geo": "test"},
        )
        == """
    SELECT example."Country".name,
           ST_AsEWKB(example."Country".geo) AS geo,
           example."Country"._id,
           example."Country"._revision
    FROM example."Country"
    """
    )

    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type     | ref     | access | prepare
    example                    |          |         |        |
      |   |   | Country        |          |         |        | page(fl)
      |   |   |   | name       | string   |         | open   |
      |   |   |   | fl         | file     |         | open   |
        """,
            "example/Country",
            page_mapping={"fl": "test"},
        )
        == """
    SELECT example."Country".name,
           example."Country"."fl._id",
           example."Country"."fl._content_type",
           example."Country"."fl._size",
           example."Country"."fl._bsize",
           example."Country"."fl._blocks",
           example."Country"._id,
           example."Country"._revision
    FROM example."Country"
    """
    )

    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type     | ref     | access | prepare
    example                    |          |         |        |
      |   |   | Country        |          |         |        | page(bool)
      |   |   |   | name       | string   |         | open   |
      |   |   |   | bool       | boolean  |         | open   |
        """,
            "example/Country",
            page_mapping={"bool": "test"},
        )
        == """
    SELECT example."Country".name,
           example."Country".bool,
           example."Country"._id,
           example."Country"._revision
    FROM example."Country"
    """
    )


def test_flip(rc: RawConfig):
    assert (
        _build(
            rc,
            """
        d | r | b | m | property | type     | ref     | access | prepare
        example                  |          |         |        |
          |   |   | Planet       |          |         |        | 
          |   |   |   | id       | string   |         | open   |
          |   |   |   | code     | string   |         | open   |
          |   |   |   | geo      | geometry |         | open   | flip()
        """,
            "example/Planet",
        )
        == """
    SELECT example."Planet".id,
           example."Planet".code,
           ST_AsEWKB(ST_FlipCoordinates(example."Planet".geo)) AS "ST_FlipCoordinates_1",
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    """
    )


def test_flip_select(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type           | ref     | prepare | access
    example                  |                |         |         |
      |   |                  |                |         |         |
      |   |   | Planet       |                |         |         |
      |   |   |   | id       | string         |         |         | open
      |   |   |   | code     | string         |         |         | open
      |   |   |   | geo      | geometry       |         |         | open
        """,
            "example/Planet",
            query="select(flip(geo))",
        )
        == """
    SELECT ST_AsEWKB(ST_FlipCoordinates(example."Planet".geo)) AS "ST_FlipCoordinates_1",
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    """
    )


def test_flip_combined(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type           | ref     | prepare | access
    example                  |                |         |         |
      |   |                  |                |         |         |
      |   |   | Planet       |                |         |         |
      |   |   |   | id       | string         |         |         | open
      |   |   |   | code     | string         |         |         | open
      |   |   |   | geo      | geometry       |         | flip()  | open
        """,
            "example/Planet",
            query="select(flip(geo))",
        )
        == """
    SELECT ST_AsEWKB(example."Planet".geo) AS geo,
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    """
    )


def test_flip_geometry_denorm(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type           | ref     | prepare | access
    example                     |                |         |         |
      |   |                     |                |         |         |
      |   |   | Geodata         |                | id      |         |
      |   |   |   | id          | string         |         |         | open
      |   |   |   | geo         | geometry       |         | flip()  | open
      |   |   | Planet          |                |         |         |
      |   |   |   | id          | string         |         |         | open
      |   |   |   | code        | string         |         |         | open
      |   |   |   | geodata     | ref            | Geodata |         | open
      |   |   |   | geodata.geo |                |         |         | open
        """,
            "example/Planet",
        )
        == """
    SELECT example."Planet".id,
           example."Planet".code,
           example."Planet"."geodata._id",
           ST_AsEWKB(ST_FlipCoordinates("Geodata_1".geo)) AS "ST_FlipCoordinates_1",
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    LEFT OUTER JOIN example."Geodata" AS "Geodata_1" ON example."Planet"."geodata._id" = "Geodata_1"._id
    """
    )


def test_flip_geometry_select_denorm_flip(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type           | ref     | prepare | access
    example                     |                |         |         |
      |   |                     |                |         |         |
      |   |   | Geodata         |                | id      |         |
      |   |   |   | id          | string         |         |         | open
      |   |   |   | geo         | geometry       |         |         | open
      |   |   | Planet          |                |         |         |
      |   |   |   | id          | string         |         |         | open
      |   |   |   | code        | string         |         |         | open
      |   |   |   | geodata     | ref            | Geodata |         | open
      |   |   |   | geodata.geo |                |         |         | open
        """,
            "example/Planet",
            query="select(flip(geodata.geo))",
        )
        == """
    SELECT ST_AsEWKB(ST_FlipCoordinates("Geodata_1".geo)) AS "ST_FlipCoordinates_1",
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    LEFT OUTER JOIN example."Geodata" AS "Geodata_1" ON example."Planet"."geodata._id" = "Geodata_1"._id
    """
    )


def test_flip_geometry_combined_denorm_flip(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type           | ref     | prepare | access
    example                     |                |         |         |
      |   |                     |                |         |         |
      |   |   | Geodata         |                | id      |         |
      |   |   |   | id          | string         |         |         | open
      |   |   |   | geo         | geometry       |         | flip()  | open
      |   |   | Planet          |                |         |         |
      |   |   |   | id          | string         |         |         | open
      |   |   |   | code        | string         |         |         | open
      |   |   |   | geodata     | ref            | Geodata |         | open
      |   |   |   | geodata.geo |                |         |         | open
        """,
            "example/Planet",
            query="select(geodata.geo,flip(geodata.geo),flip(flip(geodata.geo)))",
        )
        == """
    SELECT ST_AsEWKB(ST_FlipCoordinates("Geodata_1".geo)) AS "ST_FlipCoordinates_1",
           ST_AsEWKB("Geodata_1".geo) AS "geodata.geo",
           ST_AsEWKB(ST_FlipCoordinates("Geodata_1".geo)) AS "ST_FlipCoordinates_2",
           example."Planet"._id,
           example."Planet"._revision
    FROM example."Planet"
    LEFT OUTER JOIN example."Geodata" AS "Geodata_1" ON example."Planet"."geodata._id" = "Geodata_1"._id
    """
    )


def test_denorm_ref_level_3_mixed_mapping(rc: RawConfig):
    # Core concept is that City inherits `country`, but `country.planet.name` is overwritten
    # `country.planet` is mapped using `id` and `name`, so joins should be from `Country.planet.id` and `Planet.country.planet.name`
    assert (
        _build(
            rc,
            """
        d | r | b | m | property            | type    | ref      | source | level | access
        example                             |         |          |        |       |
          |   |   | Planet                  |         | id, name |        |       |
          |   |   |   | id                  | integer |          |        |       | open
          |   |   |   | name                | string  |          |        |       | open
          |   |   |   | code                | string  |          |        |       | open
          |   |   |   |                     |         |          |        |       |           
          |   |   | Country                 |         | id       |        |       |
          |   |   |   | id                  | integer |          |        |       | open
          |   |   |   | name                | string  |          |        |       | open
          |   |   |   | planet              | ref     | Planet   |        | 3     | open
          |   |   |   |                     |         |          |        |       |              
          |   |   | City                    |         |          |        |       |
          |   |   |   | name                | string  |          |        |       | open
          |   |   |   | country             | ref     | Country  |        | 3     | open
          |   |   |   | country.id          | integer |          |        |       | open
          |   |   |   | country.name        |         |          |        |       | open
          |   |   |   | country.planet.name | string  |          |        |       | open
          |   |   |   | country.planet.code |         |          |        |       | open
            """,
            "example/City",
        )
        == """
    SELECT example."City".name,
           example."City"."country.id",
           "Country_1".name AS "country.name",
           example."City"."country.planet.name",
           "Planet_1".code AS "country.planet.code",
           example."City"._id,
           example."City"._revision
    FROM example."City"
    LEFT OUTER JOIN example."Country" AS "Country_1" ON example."City"."country.id" = "Country_1".id
    LEFT OUTER JOIN example."Planet" AS "Planet_1" ON "Country_1"."planet.id" = "Planet_1".id
    AND example."City"."country.planet.name" = "Planet_1".name
    """
    )


def test_denorm_ref_level_4_mixed_mapping(rc: RawConfig):
    # Core concept is that City inherits `country`, but `country.planet.name` is overwritten
    # since ref level is 4 and over, changing these values should not affect mapping
    assert (
        _build(
            rc,
            """
        d | r | b | m | property            | type    | ref      | source | level | access
        example                             |         |          |        |       |
          |   |   | Planet                  |         | id, name |        |       |
          |   |   |   | id                  | integer |          |        |       | open
          |   |   |   | name                | string  |          |        |       | open
          |   |   |   | code                | string  |          |        |       | open
          |   |   |   |                     |         |          |        |       |           
          |   |   | Country                 |         | id       |        |       |
          |   |   |   | id                  | integer |          |        |       | open
          |   |   |   | name                | string  |          |        |       | open
          |   |   |   | planet              | ref     | Planet   |        |       | open
          |   |   |   |                     |         |          |        |       |              
          |   |   | City                    |         |          |        |       |
          |   |   |   | name                | string  |          |        |       | open
          |   |   |   | country             | ref     | Country  |        |       | open
          |   |   |   | country.id          | integer |          |        |       | open
          |   |   |   | country.name        |         |          |        |       | open
          |   |   |   | country.planet.name | string  |          |        |       | open
          |   |   |   | country.planet.code |         |          |        |       | open
            """,
            "example/City",
        )
        == """
    SELECT example."City".name,
           example."City"."country._id",
           example."City"."country.id",
           "Country_1".name AS "country.name",
           example."City"."country.planet.name",
           "Planet_1".code AS "country.planet.code",
           example."City"._id,
           example."City"._revision
    FROM example."City"
    LEFT OUTER JOIN example."Country" AS "Country_1" ON example."City"."country._id" = "Country_1"._id
    LEFT OUTER JOIN example."Planet" AS "Planet_1" ON "Country_1"."planet._id" = "Planet_1"._id
    """
    )


def test_denorm_nested_advanced(rc: RawConfig):
    # City inherits Country with level 4, so overwritten `Planet.country.id` should not affect it, since it maps with `_id`
    # since `Country.planet` is level 3, it means that `Planet.country.planet.id` should overwrite `Country.planet.id` value
    # and be used for `Ref` mapping.
    assert (
        _build(
            rc,
            """
    d | r | b | m | property            | type    | ref     | source | level | access
    example                             |         |         |        |       |
      |   |   | Planet                  |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   |                     |         |         |        |       |           
      |   |   | Country                 |         | id      |        |       |
      |   |   |   | id                  | integer |         |        |       | open
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | planet              | ref     | Planet  |        | 3     | open
      |   |   |   |                     |         |         |        |       |              
      |   |   | City                    |         |         |        |       |
      |   |   |   | name                | string  |         |        |       | open
      |   |   |   | country             | ref     | Country |        | 4     | open
      |   |   |   | country.id          | integer |         |        |       | open
      |   |   |   | country.name        |         |         |        |       | open
      |   |   |   | country.planet.name |         |         |        |       | open
      |   |   |   | country.planet.misc | string  |         |        |       | open
      |   |   |   | country.planet.id   | integer |         |        |       | open
            """,
            "example/City",
        )
        == """
    SELECT example."City".name,
           example."City"."country._id",
           example."City"."country.id",
           "Country_1".name AS "country.name",
           "Planet_1".name AS "country.planet.name",
           example."City"."country.planet.misc",
           example."City"."country.planet.id",
           example."City"._id,
           example."City"._revision
    FROM example."City"
    LEFT OUTER JOIN example."Country" AS "Country_1" ON example."City"."country._id" = "Country_1"._id
    LEFT OUTER JOIN example."Planet" AS "Planet_1" ON example."City"."country.planet.id" = "Planet_1".id
    """
    )
