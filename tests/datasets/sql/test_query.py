import textwrap
from typing import Type

import pytest
import sqlalchemy as sa
from sqlalchemy.sql import Select
from sqlalchemy.sql.type_api import TypeEngine

from spinta import commands, spyna
from spinta.auth import AdminToken
from spinta.components import Model, Context
from spinta.core.enums import Mode
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.helpers import get_enum_filters
from spinta.datasets.helpers import get_ref_filters
from spinta.manifests.components import Manifest
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.utils import create_empty_backend
from spinta.types.datatype import DataType, Integer, String, Boolean
from spinta.types.datatype import Ref
from spinta.types.geometry.components import Geometry
from spinta.ufuncs.helpers import merge_formulas
from spinta.ufuncs.loadbuilder.helpers import page_contains_unsupported_keys
from spinta.ufuncs.querybuilder.helpers import add_page_expr

_SUPPORT_NULLS = ["sql/postgresql", "sql/oracle", "sql/sqlite"]
_DEFAULT_NULL_IMPL = ["sql", "sql/mysql", "sql/mariadb", "sql/mssql"]

_DEFAULT_FLIP_IMPL = ["sql", "sql/sqlite", "sql/mysql", "sql/mssql", "sql/mariadb", "sql/oracle"]


def _qry(qry: Select, indent: int = 4) -> str:
    ln = "\n"
    indent_ = " " * indent
    qry = str(qry)
    qry = qry.replace("SELECT ", "SELECT\n  ")
    qry = qry.replace('", "', '",\n  "')
    qry = qry.replace(" LEFT OUTER JOIN ", "\nLEFT OUTER JOIN ")
    qry = qry.replace(" AND ", "\n  AND ")
    qry = "\n".join([line.rstrip() for line in qry.splitlines()])
    return ln + textwrap.indent(qry, indent_) + ln + indent_


def _get_sql_type(dtype: DataType) -> Type[TypeEngine]:
    if isinstance(dtype, Ref):
        return _get_sql_type(dtype.refprops[0].dtype)
    if isinstance(dtype, Integer):
        return sa.Integer
    if isinstance(dtype, String):
        return sa.Text
    if isinstance(dtype, Boolean):
        return sa.Boolean
    if isinstance(dtype, Geometry):
        return sa.Text
    raise NotImplementedError(dtype.name)


def _get_model_db_name(model: Model) -> str:
    return model.get_name_without_ns().upper()


def _meta_from_manifest(context: Context, manifest: Manifest) -> sa.MetaData:
    meta = sa.MetaData()
    for model in commands.get_models(context, manifest).values():
        columns = [
            sa.Column(prop.external.name, _get_sql_type(prop.dtype))
            for name, prop in model.properties.items()
            if (prop.external and prop.external.name and not prop.is_reserved())
        ]
        sa.Table(_get_model_db_name(model), meta, *columns)
    return meta


def _build(
    rc: RawConfig,
    manifest: str,
    model_name: str,
    *,
    query: str = "",
    page_mapping: dict = None,
) -> str:
    context, manifest = load_manifest_and_context(rc, manifest, mode=Mode.external)
    context.set("auth.token", AdminToken())
    model = commands.get_model(context, manifest, model_name)
    meta = _meta_from_manifest(context, manifest)
    backend = model.backend
    if not isinstance(backend, Sql):
        backend = create_empty_backend(context, "sql")

    backend.schema = meta
    query = asttoexpr(spyna.parse(query))
    query = merge_formulas(query, model.external.prepare)
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
    env = builder.init(backend, meta.tables[_get_model_db_name(model)])
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)
    return _qry(qry)


def test_unresolved(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | source     | prepare    | access
    example                    |         |         |            |            |
      |   |   | Country        |         | name    | COUNTRY    | democratic |
      |   |   |   | name       | string  |         | NAME       |            | open
      |   |   |   | democratic | boolean |         | DEMOCRATIC |            | open
      |   |   | City           |         | name    | CITY       |            |
      |   |   |   | name       | string  |         | NAME       |            | open
      |   |   |   | country    | ref     | Country | COUNTRY    |            | open
    """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."NAME",
      "COUNTRY"."DEMOCRATIC"
    FROM "COUNTRY"
    WHERE "COUNTRY"."DEMOCRATIC"
    """
    )


def test_unresolved_getattr(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref     | source     | prepare            | access
    example                    |         |         |            |                    |
      |   |   | Country        |         | name    | COUNTRY    |                    |
      |   |   |   | name       | string  |         | NAME       |                    | open
      |   |   |   | democratic | boolean |         | DEMOCRATIC |                    | open
      |   |   | City           |         | name    | CITY       | country.democratic |
      |   |   |   | name       | string  |         | NAME       |                    | open
      |   |   |   | country    | ref     | Country | COUNTRY    |                    | open
    """,
            "example/City",
        )
        == """
    SELECT
      "CITY"."NAME",
      "CITY"."COUNTRY"
    FROM "CITY"
    LEFT OUTER JOIN "COUNTRY" AS "COUNTRY_1" ON "CITY"."COUNTRY" = "COUNTRY_1"."NAME"
    WHERE "COUNTRY_1"."DEMOCRATIC"
    """
    )


def test_join_aliases(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type   | ref     | source     | prepare          | access
    example                  |        |         |            |                  |
      | data                 | sql    |         |            |                  |
      |   |                  |        |         |            |                  |
      |   |   | Planet       |        | id      | PLANET     | code = 'er'      |
      |   |   |   | id       | string |         | ID         |                  | open
      |   |   |   | code     | string |         | CODE       |                  | open
      |   |   |   | name     | string |         | NAME       |                  | open
      |   |                  |        |         |            |                  |
      |   |   | Country      |        | id      | COUNTRY    | code = 'lt'      |
      |   |   |   | id       | string |         | ID         |                  | open
      |   |   |   | code     | string |         | CODE       |                  | open
      |   |   |   | name     | string |         | NAME       |                  | open
      |   |   |   | planet   | ref    | Planet  | PLANET_ID  |                  | open
      |   |                  |        |         |            |                  |
      |   |   | City         |        | id      | CITY       | name = 'Vilnius' |
      |   |   |   | id       | string |         | ID         |                  | open
      |   |   |   | name     | string |         | NAME       |                  | open
      |   |   |   | country  | ref    | Country | COUNTRY_ID |                  | open
      |   |   |   | planet   | ref    | Planet  | PLANET_ID  |                  | open
      |   |                  |        |         |            |                  |
      |   |   | Street       |        | name    | STREET     |                  |
      |   |   |   | name     | string |         | NAME       |                  | open
      |   |   |   | city     | ref    | City    | CITY_ID    |                  | open
      |   |   |   | country  | ref    | Country | COUNTRY_ID |                  | open
      |   |   |   | planet   | ref    | Planet  | PLANET_ID  |                  | open
    """,
            "example/Street",
        )
        == """
    SELECT
      "STREET"."NAME",
      "STREET"."CITY_ID",
      "STREET"."COUNTRY_ID",
      "STREET"."PLANET_ID"
    FROM "STREET"
    LEFT OUTER JOIN "CITY" AS "CITY_1" ON "STREET"."CITY_ID" = "CITY_1"."ID"
    LEFT OUTER JOIN "COUNTRY" AS "COUNTRY_1" ON "CITY_1"."COUNTRY_ID" = "COUNTRY_1"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_1" ON "COUNTRY_1"."PLANET_ID" = "PLANET_1"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_2" ON "CITY_1"."PLANET_ID" = "PLANET_2"."ID"
    LEFT OUTER JOIN "COUNTRY" AS "COUNTRY_2" ON "STREET"."COUNTRY_ID" = "COUNTRY_2"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_3" ON "COUNTRY_2"."PLANET_ID" = "PLANET_3"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_4" ON "STREET"."PLANET_ID" = "PLANET_4"."ID"
    WHERE ("CITY_1"."NAME" IS NULL OR "CITY_1"."NAME" = :NAME_1)
      AND ("COUNTRY_1"."CODE" IS NULL OR "COUNTRY_1"."CODE" = :CODE_1)
      AND ("PLANET_1"."CODE" IS NULL OR "PLANET_1"."CODE" = :CODE_2)
      AND ("PLANET_2"."CODE" IS NULL OR "PLANET_2"."CODE" = :CODE_3)
      AND ("COUNTRY_2"."CODE" IS NULL OR "COUNTRY_2"."CODE" = :CODE_4)
      AND ("PLANET_3"."CODE" IS NULL OR "PLANET_3"."CODE" = :CODE_5)
      AND ("PLANET_4"."CODE" IS NULL OR "PLANET_4"."CODE" = :CODE_6)
    """
    )


def test_group(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type   | ref | source  | prepare                       | access
    example                  |        |     |         |                               |
      | data                 | sql    |     |         |                               |
      |   |   | Country      |        | id  | COUNTRY | (code = null \\| code = 'lt') |
      |   |   |   | id       | string |     | ID      |                               | open
      |   |   |   | code     | string |     | CODE    |                               | open
      |   |   |   | name     | string |     | NAME    |                               | open
    """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE",
      "COUNTRY"."NAME"
    FROM "COUNTRY"
    WHERE "COUNTRY"."CODE" IS NULL OR "COUNTRY"."CODE" = :CODE_1
    """
    )


def test_group_2(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type   | ref | source  | prepare                                                | access
    example                  |        |     |         |                                                        |
      | data                 | sql    |     |         |                                                        |
      |   |   | Country      |        | id  | COUNTRY | (code = null \\| code = 'lt') & (name = null \\| name) |
      |   |   |   | id       | string |     | ID      |                                                        | open
      |   |   |   | code     | string |     | CODE    |                                                        | open
      |   |   |   | name     | string |     | NAME    |                                                        | open
    """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE",
      "COUNTRY"."NAME"
    FROM "COUNTRY"
    WHERE ("COUNTRY"."CODE" IS NULL OR "COUNTRY"."CODE" = :CODE_1)
      AND ("COUNTRY"."NAME" IS NULL OR "COUNTRY"."NAME")
    """
    )


def test_explicit_ref(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property   | type    | ref           | source     | access
    example                    |         |               |            |
      |   |   | Country        |         | id            | COUNTRY    |
      |   |   |   | id         | integer |               | ID         | open
      |   |   |   | code       | string  |               | NAME       | open
      |   |   | City           |         | name          | CITY       |
      |   |   |   | name       | string  |               | NAME       | open
      |   |   |   | country    | ref     | Country[code] | COUNTRY_ID | open
    """,
            "example/City",
        )
        == """
    SELECT
      "CITY"."NAME",
      "CITY"."COUNTRY_ID"
    FROM "CITY"
    """
    )


@pytest.mark.parametrize("db_dialect", _SUPPORT_NULLS)
def test_paginate_none_values(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare | access
    example                  |              |         |            |         |
      | data                 | {db_dialect} |         |            |         |
      |   |                  |              |         |            |         |
      |   |   | Planet       |              | id      | PLANET     |         |
      |   |   |   | id       | string       |         | ID         |         | open
      |   |   |   | code     | string       |         | CODE       |         | open
      |   |   |   | name     | string       |         | NAME       |         | open
        """,
            "example/Planet",
            page_mapping={"id": None},
        )
        == """
    SELECT
      "PLANET"."ID",
      "PLANET"."CODE",
      "PLANET"."NAME"
    FROM "PLANET" ORDER BY "PLANET"."ID" ASC NULLS LAST
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_NULL_IMPL)
def test_paginate_none_values(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare | access
    example                  |              |         |            |         |
      | data                 | {db_dialect} |         |            |         |
      |   |                  |              |         |            |         |
      |   |   | Planet       |              | id      | PLANET     |         |
      |   |   |   | id       | string       |         | ID         |         | open
      |   |   |   | code     | string       |         | CODE       |         | open
      |   |   |   | name     | string       |         | NAME       |         | open
        """,
            "example/Planet",
            page_mapping={"id": None},
        )
        == """
    SELECT
      "PLANET"."ID",
      "PLANET"."CODE",
      "PLANET"."NAME"
    FROM "PLANET" ORDER BY CASE WHEN ("PLANET"."ID" IS NULL) THEN 1 ELSE 0 END, "PLANET"."ID" ASC
     LIMIT :param_1
    """
    )


def test_paginate_given_values_page_and_ref_not_given(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type   | ref     | source     | prepare | access
    example                  |        |         |            |         |
      | data                 | sql    |         |            |         |
      |   |                  |        |         |            |         |
      |   |   | Planet       |        |         | PLANET     |         |
      |   |   |   | id       | string |         | ID         |         | open
      |   |   |   | code     | string |         | CODE       |         | open
      |   |   |   | name     | string |         | NAME       |         | open
        """,
            "example/Planet",
            page_mapping={},
        )
        == """
    SELECT
      "PLANET"."CODE",
      "PLANET"."ID",
      "PLANET"."NAME"
    FROM "PLANET"
    """
    )


@pytest.mark.parametrize("db_dialect", _SUPPORT_NULLS)
def test_paginate_given_values_page_not_given(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare | access
    example                  |              |         |            |         |
      | data                 | {db_dialect} |         |            |         |
      |   |                  |              |         |            |         |
      |   |   | Planet       |              | name    | PLANET     |         |
      |   |   |   | id       | string       |         | ID         |         | open
      |   |   |   | code     | string       |         | CODE       |         | open
      |   |   |   | name     | string       |         | NAME       |         | open
        """,
            "example/Planet",
            page_mapping={"name": "test"},
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL ORDER BY "PLANET"."NAME" ASC NULLS LAST
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_NULL_IMPL)
def test_paginate_given_values_page_not_given(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare | access
    example                  |              |         |            |         |
      | data                 | {db_dialect} |         |            |         |
      |   |                  |              |         |            |         |
      |   |   | Planet       |              | name    | PLANET     |         |
      |   |   |   | id       | string       |         | ID         |         | open
      |   |   |   | code     | string       |         | CODE       |         | open
      |   |   |   | name     | string       |         | NAME       |         | open
        """,
            "example/Planet",
            page_mapping={"name": "test"},
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL ORDER BY CASE WHEN ("PLANET"."NAME" IS NULL) THEN 1 ELSE 0 END, "PLANET"."NAME" ASC
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _SUPPORT_NULLS)
def test_paginate_given_values_size_given(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare             | access
    example                  |              |         |            |                     |
      | data                 | {db_dialect} |         |            |                     |
      |   |                  |              |         |            |                     |
      |   |   | Planet       |              | name    | PLANET     | page(name, size: 2) |
      |   |   |   | id       | string       |         | ID         |                     | open
      |   |   |   | code     | string       |         | CODE       |                     | open
      |   |   |   | name     | string       |         | NAME       |                     | open
        """,
            "example/Planet",
            page_mapping={"name": "test"},
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL ORDER BY "PLANET"."NAME" ASC NULLS LAST
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_NULL_IMPL)
def test_paginate_given_values_size_given(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare             | access
    example                  |              |         |            |                     |
      | data                 | {db_dialect} |         |            |                     |
      |   |                  |              |         |            |                     |
      |   |   | Planet       |              | name    | PLANET     | page(name, size: 2) |
      |   |   |   | id       | string       |         | ID         |                     | open
      |   |   |   | code     | string       |         | CODE       |                     | open
      |   |   |   | name     | string       |         | NAME       |                     | open
        """,
            "example/Planet",
            page_mapping={"name": "test"},
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL ORDER BY CASE WHEN ("PLANET"."NAME" IS NULL) THEN 1 ELSE 0 END, "PLANET"."NAME" ASC
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _SUPPORT_NULLS)
def test_paginate_given_values_private(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              | name    | PLANET     | page(name, code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | private
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND ("PLANET"."CODE" > :CODE_1 OR "PLANET"."CODE" IS NULL) ORDER BY "PLANET"."NAME" ASC NULLS LAST, "PLANET"."CODE" ASC NULLS LAST
      LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_NULL_IMPL)
def test_paginate_given_values_private(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              | name    | PLANET     | page(name, code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | private
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND ("PLANET"."CODE" > :CODE_1 OR "PLANET"."CODE" IS NULL) ORDER BY CASE WHEN ("PLANET"."NAME" IS NULL) THEN 1 ELSE 0 END, "PLANET"."NAME" ASC, CASE WHEN ("PLANET"."CODE" IS NULL) THEN 1 ELSE 0 END, "PLANET"."CODE" ASC
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _SUPPORT_NULLS)
def test_paginate_given_values_two_keys(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              | name    | PLANET     | page(name, code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | open
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND ("PLANET"."CODE" > :CODE_1 OR "PLANET"."CODE" IS NULL) ORDER BY "PLANET"."NAME" ASC NULLS LAST, "PLANET"."CODE" ASC NULLS LAST
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_NULL_IMPL)
def test_paginate_given_values_two_keys(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              | name    | PLANET     | page(name, code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | open
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND ("PLANET"."CODE" > :CODE_1 OR "PLANET"."CODE" IS NULL) ORDER BY CASE WHEN ("PLANET"."NAME" IS NULL) THEN 1 ELSE 0 END, "PLANET"."NAME" ASC, CASE WHEN ("PLANET"."CODE" IS NULL) THEN 1 ELSE 0 END, "PLANET"."CODE" ASC
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _SUPPORT_NULLS)
def test_paginate_given_values_two_keys_ref_not_given(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              |         | PLANET     | page(name, code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | open
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."CODE",
      "PLANET"."ID",
      "PLANET"."NAME"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND ("PLANET"."CODE" > :CODE_1 OR "PLANET"."CODE" IS NULL) ORDER BY "PLANET"."NAME" ASC NULLS LAST, "PLANET"."CODE" ASC NULLS LAST
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_NULL_IMPL)
def test_paginate_given_values_two_keys_ref_not_given(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              |         | PLANET     | page(name, code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | open
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."CODE",
      "PLANET"."ID",
      "PLANET"."NAME"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND ("PLANET"."CODE" > :CODE_1 OR "PLANET"."CODE" IS NULL) ORDER BY CASE WHEN ("PLANET"."NAME" IS NULL) THEN 1 ELSE 0 END, "PLANET"."NAME" ASC, CASE WHEN ("PLANET"."CODE" IS NULL) THEN 1 ELSE 0 END, "PLANET"."CODE" ASC
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _SUPPORT_NULLS)
def test_paginate_desc(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              |         | PLANET     | page(name, -code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | open
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "-code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."CODE",
      "PLANET"."ID",
      "PLANET"."NAME"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND "PLANET"."CODE" < :CODE_1 ORDER BY "PLANET"."NAME" ASC NULLS LAST, "PLANET"."CODE" DESC NULLS FIRST
     LIMIT :param_1
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_NULL_IMPL)
def test_paginate_desc(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              |         | PLANET     | page(name, -code) |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | open
      |   |   |   | name     | string       |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "-code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."CODE",
      "PLANET"."ID",
      "PLANET"."NAME"
    FROM "PLANET"
    WHERE "PLANET"."NAME" > :NAME_1 OR "PLANET"."NAME" IS NULL OR "PLANET"."NAME" = :NAME_2
      AND "PLANET"."CODE" < :CODE_1 ORDER BY CASE WHEN ("PLANET"."NAME" IS NULL) THEN 1 ELSE 0 END, "PLANET"."NAME" ASC, CASE WHEN ("PLANET"."CODE" IS NOT NULL) THEN 1 ELSE 0 END, "PLANET"."CODE" DESC
     LIMIT :param_1
    """
    )


def test_paginate_disabled(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type   | ref     | source     | prepare          | access
    example                  |        |         |            |                  |
      | data                 | sql    |         |            |                  |
      |   |                  |        |         |            |                  |
      |   |   | Planet       |        | name    | PLANET     | page()           |
      |   |   |   | id       | string |         | ID         |                  | open
      |   |   |   | code     | string |         | CODE       |                  | open
      |   |   |   | name     | string |         | NAME       |                  | open
        """,
            "example/Planet",
            page_mapping={"name": "test"},
        )
        == """
    SELECT
      "PLANET"."NAME",
      "PLANET"."ID",
      "PLANET"."CODE"
    FROM "PLANET"
    """
    )


def test_composite_non_pk_ref_with_literal(rc: RawConfig):
    assert (
        _build(
            rc,
            """
d | r | b | m | property | type    | ref                       | source       | prepare  | access
example                  |         |                           |              |          |
  |   |   | Translation  |         | id                        | TRANSLATION  |          | open
  |   |   |   | id       | integer |                           | ID           |          |
  |   |   |   | lang     | string  |                           | LANG         |          |
  |   |   |   | name     | string  |                           | NAME         |          |
  |   |   |   | city_id  | integer |                           | CITY_ID      |          |
  |   |   |   |          |         |                           |              |          |
  |   |   | City         |         | id                        | CITY         |          | open
  |   |   |   | id       | integer |                           | ID           |          |
  |   |   |   | en       | ref     | Translation[city_id,lang] |              | id, "en" |
  |   |   |   | name_en  | string  |                           |              | en.name  |
  |   |   |   | lt       | ref     | Translation[city_id,lang] |              | id, "lt" |
  |   |   |   | name_lt  | string  |                           |              | lt.name  |
        """,
            "example/City",
        )
        == """
    SELECT
      "CITY"."ID",
      "TRANSLATION_1"."NAME",
      "TRANSLATION_2"."NAME" AS "NAME_1"
    FROM "CITY"
    LEFT OUTER JOIN "TRANSLATION" AS "TRANSLATION_1" ON "CITY"."ID" = "TRANSLATION_1"."CITY_ID"
      AND "TRANSLATION_1"."LANG" = :LANG_1
    LEFT OUTER JOIN "TRANSLATION" AS "TRANSLATION_2" ON "CITY"."ID" = "TRANSLATION_2"."CITY_ID"
      AND "TRANSLATION_2"."LANG" = :LANG_2
    """
    )


@pytest.mark.parametrize("db_dialect", _DEFAULT_FLIP_IMPL)
def test_flip_result_builder(db_dialect: str, rc: RawConfig):
    assert (
        _build(
            rc,
            f"""
    d | r | b | m | property | type         | ref     | source     | prepare          | access
    example                  |              |         |            |                  |
      | data                 | {db_dialect} |         |            |                  |
      |   |                  |              |         |            |                  |
      |   |   | Planet       |              |         | PLANET     |                  |
      |   |   |   | id       | string       |         | ID         |                  | open
      |   |   |   | code     | string       |         | CODE       |                  | open
      |   |   |   | geo      | geometry     |         | GEO        | flip()           | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "-code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."CODE",
      "PLANET"."GEO",
      "PLANET"."ID"
    FROM "PLANET"
    """
    )


def test_flip_postgresql(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type           | ref     | source     | prepare          | access
    example                  |                |         |            |                  |
      | data                 | sql/postgresql |         |            |                  |
      |   |                  |                |         |            |                  |
      |   |   | Planet       |                |         | PLANET     |                  |
      |   |   |   | id       | string         |         | ID         |                  | open
      |   |   |   | code     | string         |         | CODE       |                  | open
      |   |   |   | geo      | geometry       |         | GEO        | flip()           | open
        """,
            "example/Planet",
            page_mapping={
                "name": "test",
                "-code": 5,
            },
        )
        == """
    SELECT
      "PLANET"."CODE", ST_AsEWKB(ST_FlipCoordinates("PLANET"."GEO")) AS "ST_FlipCoordinates_1",
      "PLANET"."ID"
    FROM "PLANET"
    """
    )


def test_flip_postgresql_select(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type           | ref     | source     | prepare | access
    example                  |                |         |            |         |
      | data                 | sql/postgresql |         |            |         |
      |   |                  |                |         |            |         |
      |   |   | Planet       |                |         | PLANET     |         |
      |   |   |   | id       | string         |         | ID         |         | open
      |   |   |   | code     | string         |         | CODE       |         | open
      |   |   |   | geo      | geometry       |         | GEO        |         | open
        """,
            "example/Planet",
            query="select(flip(geo))",
        )
        == """
    SELECT
      ST_AsEWKB(ST_FlipCoordinates("PLANET"."GEO")) AS "ST_FlipCoordinates_1"
    FROM "PLANET"
    """
    )


def test_flip_postgresql_combined(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property | type           | ref     | source     | prepare | access
    example                  |                |         |            |         |
      | data                 | sql/postgresql |         |            |         |
      |   |                  |                |         |            |         |
      |   |   | Planet       |                |         | PLANET     |         |
      |   |   |   | id       | string         |         | ID         |         | open
      |   |   |   | code     | string         |         | CODE       |         | open
      |   |   |   | geo      | geometry       |         | GEO        | flip()  | open
        """,
            "example/Planet",
            query="select(flip(geo))",
        )
        == """
    SELECT
      "PLANET"."GEO"
    FROM "PLANET"
    """
    )


def test_flip_postgresql_geometry_denorm(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type           | ref     | source     | prepare | access
    example                     |                |         |            |         |
      | data                    | sql/postgresql |         |            |         |
      |   |                     |                |         |            |         |
      |   |   | Geodata         |                | id      | GEODATA    |         |
      |   |   |   | id          | string         |         | ID         |         | open
      |   |   |   | geo         | geometry       |         | GEO        | flip()  | open
      |   |   | Planet          |                |         | PLANET     |         |
      |   |   |   | id          | string         |         | ID         |         | open
      |   |   |   | code        | string         |         | CODE       |         | open
      |   |   |   | geodata     | ref            | Geodata | GEO_ID     |         | open
      |   |   |   | geodata.geo |                |         |            |         | open
        """,
            "example/Planet",
        )
        == """
    SELECT
      "PLANET"."CODE",
      "PLANET"."GEO_ID", ST_AsEWKB(ST_FlipCoordinates("GEODATA_1"."GEO")) AS "ST_FlipCoordinates_1",
      "PLANET"."ID"
    FROM "PLANET"
    LEFT OUTER JOIN "GEODATA" AS "GEODATA_1" ON "PLANET"."GEO_ID" = "GEODATA_1"."ID"
    """
    )


def test_flip_postgresql_geometry_select_denorm_flip(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type           | ref     | source     | prepare | access
    example                     |                |         |            |         |
      | data                    | sql/postgresql |         |            |         |
      |   |                     |                |         |            |         |
      |   |   | Geodata         |                | id      | GEODATA    |         |
      |   |   |   | id          | string         |         | ID         |         | open
      |   |   |   | geo         | geometry       |         | GEO        |         | open
      |   |   | Planet          |                |         | PLANET     |         |
      |   |   |   | id          | string         |         | ID         |         | open
      |   |   |   | code        | string         |         | CODE       |         | open
      |   |   |   | geodata     | ref            | Geodata | GEO_ID     |         | open
      |   |   |   | geodata.geo |                |         |            |         | open
        """,
            "example/Planet",
            query="select(flip(geodata.geo))",
        )
        == """
    SELECT
      ST_AsEWKB(ST_FlipCoordinates("GEODATA_1"."GEO")) AS "ST_FlipCoordinates_1"
    FROM "PLANET"
    LEFT OUTER JOIN "GEODATA" AS "GEODATA_1" ON "PLANET"."GEO_ID" = "GEODATA_1"."ID"
    """
    )


def test_flip_postgresql_geometry_combined_denorm_flip(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type           | ref     | source     | prepare | access
    example                     |                |         |            |         |
      | data                    | sql/postgresql |         |            |         |
      |   |                     |                |         |            |         |
      |   |   | Geodata         |                | id      | GEODATA    |         |
      |   |   |   | id          | string         |         | ID         |         | open
      |   |   |   | geo         | geometry       |         | GEO        | flip()  | open
      |   |   | Planet          |                |         | PLANET     |         |
      |   |   |   | id          | string         |         | ID         |         | open
      |   |   |   | code        | string         |         | CODE       |         | open
      |   |   |   | geodata     | ref            | Geodata | GEO_ID     |         | open
      |   |   |   | geodata.geo |                |         |            |         | open
        """,
            "example/Planet",
            query="select(geodata.geo,flip(geodata.geo),flip(flip(geodata.geo)))",
        )
        == """
    SELECT
      ST_AsEWKB(ST_FlipCoordinates("GEODATA_1"."GEO")) AS "ST_FlipCoordinates_1",
      "GEODATA_1"."GEO", ST_AsEWKB(ST_FlipCoordinates("GEODATA_1"."GEO")) AS "ST_FlipCoordinates_2"
    FROM "PLANET"
    LEFT OUTER JOIN "GEODATA" AS "GEODATA_1" ON "PLANET"."GEO_ID" = "GEODATA_1"."ID"
    """
    )


def test_array_intermediate_table_sqlite(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type       | ref             | source          | prepare | access
    example                     |            |                 |                 |         |
      | data                    | sql/sqlite |                 |                 |         |
      |   |                     |            |                 |                 |         |
      |   |   | Language        |            | id              | LANGUAGE        |         |
      |   |   |   | id          | string     |                 | ID              |         | open
      |   |   |   | code        | string     |                 | CODE            |         | open
      |   |                     |            |                 |                 |         |
      |   |   | Country         |            | id              | COUNTRY         |         |
      |   |   |   | id          | string     |                 | ID              |         | open
      |   |   |   | code        | string     |                 | CODE            |         | open
      |   |   |   | languages   | array      | CountryLanguage |                 |         | open
      |   |   |   | languages[] | ref        | Language        |                 |         | open
      |   |                     |            |                 |                 |         |
      |   |   | CountryLanguage |            |                 | COUNTRYLANGUAGE |         |
      |   |   |   | country     | ref        | Country         | COUNTRY         |         | open
      |   |   |   | language    | ref        | Language        | LANGUAGE        |         | open
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", json_group_array("COUNTRYLANGUAGE_1"."LANGUAGE") AS json_group_array_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_postgresql(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type           | ref             | source          | prepare | access
    example                     |                |                 |                 |         |
      | data                    | sql/postgresql |                 |                 |         |
      |   |                     |                |                 |                 |         |
      |   |   | Language        |                | id              | LANGUAGE        |         |
      |   |   |   | id          | string         |                 | ID              |         | open
      |   |   |   | code        | string         |                 | CODE            |         | open
      |   |                     |                |                 |                 |         |
      |   |   | Country         |                | id              | COUNTRY         |         |
      |   |   |   | id          | string         |                 | ID              |         | open
      |   |   |   | code        | string         |                 | CODE            |         | open
      |   |   |   | languages   | array          | CountryLanguage |                 |         | open
      |   |   |   | languages[] | ref            | Language        |                 |         | open
      |   |                     |                |                 |                 |         |
      |   |   | CountryLanguage |                |                 | COUNTRYLANGUAGE |         |
      |   |   |   | country     | ref            | Country         | COUNTRY         |         | open
      |   |   |   | language    | ref            | Language        | LANGUAGE        |         | open
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", jsonb_agg("COUNTRYLANGUAGE_1"."LANGUAGE") AS jsonb_agg_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_mysql(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type      | ref             | source          | prepare | access
    example                     |           |                 |                 |         |
      | data                    | sql/mysql |                 |                 |         |
      |   |                     |           |                 |                 |         |
      |   |   | Language        |           | id              | LANGUAGE        |         |
      |   |   |   | id          | string    |                 | ID              |         | open
      |   |   |   | code        | string    |                 | CODE            |         | open
      |   |                     |           |                 |                 |         |
      |   |   | Country         |           | id              | COUNTRY         |         |
      |   |   |   | id          | string    |                 | ID              |         | open
      |   |   |   | code        | string    |                 | CODE            |         | open
      |   |   |   | languages   | array     | CountryLanguage |                 |         | open
      |   |   |   | languages[] | ref       | Language        |                 |         | open
      |   |                     |           |                 |                 |         |
      |   |   | CountryLanguage |           |                 | COUNTRYLANGUAGE |         |
      |   |   |   | country     | ref       | Country         | COUNTRY         |         | open
      |   |   |   | language    | ref       | Language        | LANGUAGE        |         | open
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", json_arrayagg("COUNTRYLANGUAGE_1"."LANGUAGE") AS json_arrayagg_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_mariadb(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type        | ref             | source          | prepare | access
    example                     |             |                 |                 |         |
      | data                    | sql/mariadb |                 |                 |         |
      |   |                     |             |                 |                 |         |
      |   |   | Language        |             | id              | LANGUAGE        |         |
      |   |   |   | id          | string      |                 | ID              |         | open
      |   |   |   | code        | string      |                 | CODE            |         | open
      |   |                     |             |                 |                 |         |
      |   |   | Country         |             | id              | COUNTRY         |         |
      |   |   |   | id          | string      |                 | ID              |         | open
      |   |   |   | code        | string      |                 | CODE            |         | open
      |   |   |   | languages   | array       | CountryLanguage |                 |         | open
      |   |   |   | languages[] | ref         | Language        |                 |         | open
      |   |                     |             |                 |                 |         |
      |   |   | CountryLanguage |             |                 | COUNTRYLANGUAGE |         |
      |   |   |   | country     | ref         | Country         | COUNTRY         |         | open
      |   |   |   | language    | ref         | Language        | LANGUAGE        |         | open
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", json_arrayagg("COUNTRYLANGUAGE_1"."LANGUAGE") AS json_arrayagg_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_mssql(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property    | type      | ref             | source          | prepare | access
    example                     |           |                 |                 |         |
      | data                    | sql/mssql |                 |                 |         |
      |   |                     |           |                 |                 |         |
      |   |   | Language        |           | id              | LANGUAGE        |         |
      |   |   |   | id          | string    |                 | ID              |         | open
      |   |   |   | code        | string    |                 | CODE            |         | open
      |   |                     |           |                 |                 |         |
      |   |   | Country         |           | id              | COUNTRY         |         |
      |   |   |   | id          | string    |                 | ID              |         | open
      |   |   |   | code        | string    |                 | CODE            |         | open
      |   |   |   | languages   | array     | CountryLanguage |                 |         | open
      |   |   |   | languages[] | ref       | Language        |                 |         | open
      |   |                     |           |                 |                 |         |
      |   |   | CountryLanguage |           |                 | COUNTRYLANGUAGE |         |
      |   |   |   | country     | ref       | Country         | COUNTRY         |         | open
      |   |   |   | language    | ref       | Language        | LANGUAGE        |         | open
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", concat(:concat_2, "COUNTRYLANGUAGE_1"."LANGUAGE", :concat_3) AS concat_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_multi_column_sqlite(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property      | type       | ref             | source          | prepare                    | access | level
    example                       |            |                 |                 |                            |        |
      | data                      | sql/sqlite |                 |                 |                            |        |
      |   |                       |            |                 |                 |                            |        |
      |   |   | Language          |            | id, code        | LANGUAGE        |                            |        |
      |   |   |   | id            | string     |                 | ID              |                            | open   |    
      |   |   |   | code          | string     |                 | CODE            |                            | open   |    
      |   |                       |            |                 |                 |                            |        |
      |   |   | Country           |            | id              | COUNTRY         |                            |        |
      |   |   |   | id            | string     |                 | ID              |                            | open   |    
      |   |   |   | code          | string     |                 | CODE            |                            | open   |    
      |   |   |   | languages     | array      | CountryLanguage |                 |                            | open   |    
      |   |   |   | languages[]   | ref        | Language        |                 |                            | open   |    
      |   |                       |            |                 |                 |                            |        |
      |   |   | CountryLanguage   |            |                 | COUNTRYLANGUAGE |                            |        |
      |   |   |   | language_id   | string     |                 | LANGUAGEID      |                            | open   |    
      |   |   |   | language_code | string     |                 | LANGUAGECODE    |                            | open   |    
      |   |   |   | country       | ref        | Country         | COUNTRY         |                            | open   |    
      |   |   |   | language      | ref        | Language        |                 | language_id, language_code | open   | 3   
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", json_group_array(json_array("COUNTRYLANGUAGE_1"."LANGUAGEID",
      "COUNTRYLANGUAGE_1"."LANGUAGECODE")) AS json_group_array_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_multi_column_postgresql(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property      | type           | ref             | source          | prepare                    | access | level
    example                       |                |                 |                 |                            |        |
      | data                      | sql/postgresql |                 |                 |                            |        |
      |   |                       |                |                 |                 |                            |        |
      |   |   | Language          |                | id, code        | LANGUAGE        |                            |        |
      |   |   |   | id            | string         |                 | ID              |                            | open   |    
      |   |   |   | code          | string         |                 | CODE            |                            | open   |    
      |   |                       |                |                 |                 |                            |        |
      |   |   | Country           |                | id              | COUNTRY         |                            |        |
      |   |   |   | id            | string         |                 | ID              |                            | open   |    
      |   |   |   | code          | string         |                 | CODE            |                            | open   |    
      |   |   |   | languages     | array          | CountryLanguage |                 |                            | open   |    
      |   |   |   | languages[]   | ref            | Language        |                 |                            | open   |    
      |   |                       |                |                 |                 |                            |        |
      |   |   | CountryLanguage   |                |                 | COUNTRYLANGUAGE |                            |        |
      |   |   |   | language_id   | string         |                 | LANGUAGEID      |                            | open   |    
      |   |   |   | language_code | string         |                 | LANGUAGECODE    |                            | open   |    
      |   |   |   | country       | ref            | Country         | COUNTRY         |                            | open   |    
      |   |   |   | language      | ref            | Language        |                 | language_id, language_code | open   | 3   
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", jsonb_agg(jsonb_build_array("COUNTRYLANGUAGE_1"."LANGUAGEID",
      "COUNTRYLANGUAGE_1"."LANGUAGECODE")) AS jsonb_agg_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_multi_column_mysql(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property      | type      | ref             | source          | prepare                    | access | level
    example                       |           |                 |                 |                            |        |
      | data                      | sql/mysql |                 |                 |                            |        |
      |   |                       |           |                 |                 |                            |        |
      |   |   | Language          |           | id, code        | LANGUAGE        |                            |        |
      |   |   |   | id            | string    |                 | ID              |                            | open   |    
      |   |   |   | code          | string    |                 | CODE            |                            | open   |    
      |   |                       |           |                 |                 |                            |        |
      |   |   | Country           |           | id              | COUNTRY         |                            |        |
      |   |   |   | id            | string    |                 | ID              |                            | open   |    
      |   |   |   | code          | string    |                 | CODE            |                            | open   |    
      |   |   |   | languages     | array     | CountryLanguage |                 |                            | open   |    
      |   |   |   | languages[]   | ref       | Language        |                 |                            | open   |    
      |   |                       |           |                 |                 |                            |        |
      |   |   | CountryLanguage   |           |                 | COUNTRYLANGUAGE |                            |        |
      |   |   |   | language_id   | string    |                 | LANGUAGEID      |                            | open   |    
      |   |   |   | language_code | string    |                 | LANGUAGECODE    |                            | open   |    
      |   |   |   | country       | ref       | Country         | COUNTRY         |                            | open   |    
      |   |   |   | language      | ref       | Language        |                 | language_id, language_code | open   | 3   
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", json_arrayagg(json_array("COUNTRYLANGUAGE_1"."LANGUAGEID",
      "COUNTRYLANGUAGE_1"."LANGUAGECODE")) AS json_arrayagg_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_multi_column_mariadb(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property      | type        | ref             | source          | prepare                    | access | level
    example                       |             |                 |                 |                            |        |
      | data                      | sql/mariadb |                 |                 |                            |        |
      |   |                       |             |                 |                 |                            |        |
      |   |   | Language          |             | id, code        | LANGUAGE        |                            |        |
      |   |   |   | id            | string      |                 | ID              |                            | open   |    
      |   |   |   | code          | string      |                 | CODE            |                            | open   |    
      |   |                       |             |                 |                 |                            |        |
      |   |   | Country           |             | id              | COUNTRY         |                            |        |
      |   |   |   | id            | string      |                 | ID              |                            | open   |    
      |   |   |   | code          | string      |                 | CODE            |                            | open   |    
      |   |   |   | languages     | array       | CountryLanguage |                 |                            | open   |    
      |   |   |   | languages[]   | ref         | Language        |                 |                            | open   |    
      |   |                       |             |                 |                 |                            |        |
      |   |   | CountryLanguage   |             |                 | COUNTRYLANGUAGE |                            |        |
      |   |   |   | language_id   | string      |                 | LANGUAGEID      |                            | open   |    
      |   |   |   | language_code | string      |                 | LANGUAGECODE    |                            | open   |    
      |   |   |   | country       | ref         | Country         | COUNTRY         |                            | open   |    
      |   |   |   | language      | ref         | Language        |                 | language_id, language_code | open   | 3   
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", json_arrayagg(json_array("COUNTRYLANGUAGE_1"."LANGUAGEID",
      "COUNTRYLANGUAGE_1"."LANGUAGECODE")) AS json_arrayagg_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )


def test_array_intermediate_table_multi_column_mssql(rc: RawConfig):
    assert (
        _build(
            rc,
            """
    d | r | b | m | property      | type      | ref             | source          | prepare                    | access | level
    example                       |           |                 |                 |                            |        |
      | data                      | sql/mssql |                 |                 |                            |        |
      |   |                       |           |                 |                 |                            |        |
      |   |   | Language          |           | id, code        | LANGUAGE        |                            |        |
      |   |   |   | id            | string    |                 | ID              |                            | open   |    
      |   |   |   | code          | string    |                 | CODE            |                            | open   |    
      |   |                       |           |                 |                 |                            |        |
      |   |   | Country           |           | id              | COUNTRY         |                            |        |
      |   |   |   | id            | string    |                 | ID              |                            | open   |    
      |   |   |   | code          | string    |                 | CODE            |                            | open   |    
      |   |   |   | languages     | array     | CountryLanguage |                 |                            | open   |    
      |   |   |   | languages[]   | ref       | Language        |                 |                            | open   |    
      |   |                       |           |                 |                 |                            |        |
      |   |   | CountryLanguage   |           |                 | COUNTRYLANGUAGE |                            |        |
      |   |   |   | language_id   | string    |                 | LANGUAGEID      |                            | open   |    
      |   |   |   | language_code | string    |                 | LANGUAGECODE    |                            | open   |    
      |   |   |   | country       | ref       | Country         | COUNTRY         |                            | open   |    
      |   |   |   | language      | ref       | Language        |                 | language_id, language_code | open   | 3   
        """,
            "example/Country",
        )
        == """
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE", concat(:concat_2, json_array("COUNTRYLANGUAGE_1"."LANGUAGEID",
      "COUNTRYLANGUAGE_1"."LANGUAGECODE"), :concat_3) AS concat_1
    FROM "COUNTRY"
    LEFT OUTER JOIN "COUNTRYLANGUAGE" AS "COUNTRYLANGUAGE_1" ON "COUNTRYLANGUAGE_1"."COUNTRY" = "COUNTRY"."ID" GROUP BY "COUNTRY"."ID",
      "COUNTRY"."CODE"
    """
    )
