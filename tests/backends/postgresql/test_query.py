import textwrap
import uuid

import sqlparse
import sqlalchemy as sa
from sqlalchemy.sql import Select

from spinta import spyna
from spinta import commands
from spinta.auth import AdminToken
from spinta.backends.postgresql.ufuncs.query.components import PgQueryBuilder
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.backends.postgresql.components import PostgreSQL
from spinta.testing.manifest import load_manifest_and_context
from spinta.ufuncs.basequerybuilder.helpers import add_page_expr
from spinta.ufuncs.loadbuilder.helpers import page_contains_unsupported_keys


def _qry(qry: Select, indent: int = 4) -> str:
    sql = str(qry) % qry.compile().params
    sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
    ln = '\n'
    indent_ = ' ' * indent
    return ln + textwrap.indent(sql, indent_) + ln + indent_


def _build(rc: RawConfig, manifest: str, model_name: str, query: str, page_mapping: dict = None) -> str:
    context, manifest = load_manifest_and_context(rc, manifest)
    context.set('auth.token', AdminToken())
    backend = PostgreSQL()
    backend.name = 'default'
    backend.schema = sa.MetaData()
    backend.tables = {}
    commands.prepare(context, backend, manifest)
    model = commands.get_model(context, manifest, model_name)
    query = asttoexpr(spyna.parse(query))
    if page_mapping:
        page = model.page
        if page.is_enabled:
            for key, value in page_mapping.items():
                cleaned = key[1:] if key.startswith('-') else key
                page.update_value(key, model.properties.get(cleaned), value)
            if page_contains_unsupported_keys(page):
                page.is_enabled = False
        query = add_page_expr(query, page)
    builder = PgQueryBuilder(context)
    builder.update(model=model)
    env = builder.init(backend, backend.get_table(model))
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)
    return _qry(qry)


def test_select_id(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
    ''', 'example/City', 'select(_id)') == '''
    SELECT "example/City"._id,
           "example/City"._revision
    FROM "example/City"
    '''


def test_filter_by_ref_id(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''', 'example/City', 'country._id="ba1f89f1-066c-4a8b-bfb4-1b65627e79bb"') == '''
    SELECT "example/City".name,
           "example/City"."country._id",
           "example/City"._id,
           "example/City"._revision
    FROM "example/City"
    WHERE "example/City"."country._id" = :country._id_1
    '''


def test_join(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''', 'example/City', 'select(name, country.name)') == '''
    SELECT "example/City".name,
           "example/Country_1".name AS "country.name",
           "example/City"._id,
           "example/City"._revision
    FROM "example/City"
    LEFT OUTER JOIN "example/Country" AS "example/Country_1" ON "example/City"."country._id" = "example/Country_1"._id
    '''


def test_join_and_id(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''', 'example/City', 'select(_id, country.name)') == '''
    SELECT "example/City"._id,
           "example/Country_1".name AS "country.name",
           "example/City"._revision
    FROM "example/City"
    LEFT OUTER JOIN "example/Country" AS "example/Country_1" ON "example/City"."country._id" = "example/Country_1"._id
    '''


def test_join_two_refs(rc: RawConfig):
    assert _build(rc, '''
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
    ''', 'example/City', 'select(_id, country.name)') == '''
    SELECT "example/City"._id,
           "example/Country_1".name AS "country.name",
           "example/City"._revision
    FROM "example/City"
    LEFT OUTER JOIN "example/Country" AS "example/Country_1" ON "example/City"."country._id" = "example/Country_1"._id
    '''


def test_join_two_refs_same_model(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Planet         |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | planet1    | ref     | Planet  | open
      |   |   |   | planet2    | ref     | Planet  | open
    ''', 'example/Country', 'select(planet1.name, planet2.name)') == '''
    SELECT "example/Planet_1".name AS "planet1.name",
           "example/Planet_2".name AS "planet2.name",
           "example/Country"._id,
           "example/Country"._revision
    FROM "example/Country"
    LEFT OUTER JOIN "example/Planet" AS "example/Planet_1" ON "example/Country"."planet1._id" = "example/Planet_1"._id
    LEFT OUTER JOIN "example/Planet" AS "example/Planet_2" ON "example/Country"."planet2._id" = "example/Planet_2"._id
    '''


def test_paginate_all_none_values(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        ''', 'example/Planet', '', {
        'name': None
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet"._id,
           "example/Planet".code,
           "example/Planet"._revision
    FROM "example/Planet"
    ORDER BY "example/Planet".name ASC,
             "example/Planet"._id ASC
    '''


def test_paginate_half_none_values(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        ''', 'example/Planet', '', {
        'name': None,
        'code': 0
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet"._id,
           "example/Planet".code,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name IS NULL
      AND "example/Planet"._id IS NULL
      AND ("example/Planet".code > :code_1
           OR "example/Planet".code IS NULL)
    ORDER BY "example/Planet".name ASC,
             "example/Planet"._id ASC,
             "example/Planet".code ASC
    '''


def test_paginate_half_none_values_desc(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        ''', 'example/Planet', '', {
        '-name': None,
        '-code': 0
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet"._id,
           "example/Planet".code,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name IS NOT NULL
      OR "example/Planet".name IS NULL
      AND "example/Planet"._id IS NULL
      AND "example/Planet".code < :code_1
    ORDER BY "example/Planet".name DESC,
             "example/Planet"._id ASC,
             "example/Planet".code DESC
    '''


def test_paginate_given_values_page_and_ref_not_given(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         |         |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        ''', 'example/Planet', '', {
        '_id': uuid.uuid4()
    }) == '''
    SELECT "example/Planet"._id,
           "example/Planet".name,
           "example/Planet".code,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet"._id > :id_1
      OR "example/Planet"._id IS NULL
    ORDER BY "example/Planet"._id ASC
    '''


def test_paginate_given_values_page_not_given(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access
        example                    |         |         |
          |   |   | Planet         |         | name    |
          |   |   |   | name       | string  |         | open
          |   |   |   | code       | integer |         | open
        ''', 'example/Planet', '', {
        'name': 'test',
        '_id': uuid.uuid4()
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet"._id,
           "example/Planet".code,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name > :name_1
      OR "example/Planet".name IS NULL
      OR "example/Planet".name = :name_2
      AND ("example/Planet"._id > :id_1
           OR "example/Planet"._id IS NULL)
    ORDER BY "example/Planet".name ASC,
             "example/Planet"._id ASC
    '''


def test_paginate_given_values_size_given(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, size: 2)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        ''', 'example/Planet', '', {
        'name': 'test',
        '_id': uuid.uuid4()
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet"._id,
           "example/Planet".code,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name > :name_1
      OR "example/Planet".name IS NULL
      OR "example/Planet".name = :name_2
      AND ("example/Planet"._id > :id_1
           OR "example/Planet"._id IS NULL)
    ORDER BY "example/Planet".name ASC,
             "example/Planet"._id ASC
    LIMIT :param_1
    '''


def test_paginate_given_values_private(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access  | prepare
        example                    |         |         |         |
          |   |   | Planet         |         | name    |         | page(name, code)
          |   |   |   | name       | string  |         | open    |
          |   |   |   | code       | integer |         | private |
        ''', 'example/Planet', '', {
        'name': 'test',
        'code': 5,
        '_id': uuid.uuid4()
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet".code,
           "example/Planet"._id,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name > :name_1
      OR "example/Planet".name IS NULL
      OR "example/Planet".name = :name_2
      AND ("example/Planet".code > :code_1
           OR "example/Planet".code IS NULL)
      OR "example/Planet".name = :name_3
      AND "example/Planet".code = :code_2
      AND ("example/Planet"._id > :id_1
           OR "example/Planet"._id IS NULL)
    ORDER BY "example/Planet".name ASC,
             "example/Planet".code ASC,
             "example/Planet"._id ASC
    '''


def test_paginate_given_values_two_keys(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, code)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        ''', 'example/Planet', '', {
        'name': 'test',
        'code': 5,
        '_id': uuid.uuid4()
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet".code,
           "example/Planet"._id,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name > :name_1
      OR "example/Planet".name IS NULL
      OR "example/Planet".name = :name_2
      AND ("example/Planet".code > :code_1
           OR "example/Planet".code IS NULL)
      OR "example/Planet".name = :name_3
      AND "example/Planet".code = :code_2
      AND ("example/Planet"._id > :id_1
           OR "example/Planet"._id IS NULL)
    ORDER BY "example/Planet".name ASC,
             "example/Planet".code ASC,
             "example/Planet"._id ASC
    '''


def test_paginate_given_values_five_keys(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, code, float, user, pass)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
          |   |   |   | float      | number  |         | open   |
          |   |   |   | user       | string  |         | open   |
          |   |   |   | pass       | string  |         | open   |
        ''', 'example/Planet', '', {
        'name': 'test',
        'code': 5,
        'float': 1.5,
        'user': 'test',
        'pass': 'test',
        '_id': uuid.uuid4()
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet".code,
           "example/Planet".float,
           "example/Planet"."user",
           "example/Planet".pass,
           "example/Planet"._id,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name > :name_1
      OR "example/Planet".name IS NULL
      OR "example/Planet".name = :name_2
      AND ("example/Planet".code > :code_1
           OR "example/Planet".code IS NULL)
      OR "example/Planet".name = :name_3
      AND "example/Planet".code = :code_2
      AND ("example/Planet".float > :float_1
           OR "example/Planet".float IS NULL)
      OR "example/Planet".name = :name_4
      AND "example/Planet".code = :code_3
      AND "example/Planet".float = :float_2
      AND ("example/Planet"."user" > :user_1
           OR "example/Planet"."user" IS NULL)
      OR "example/Planet".name = :name_5
      AND "example/Planet".code = :code_4
      AND "example/Planet".float = :float_3
      AND "example/Planet"."user" = :user_2
      AND ("example/Planet".pass > :pass_1
           OR "example/Planet".pass IS NULL)
      OR "example/Planet".name = :name_6
      AND "example/Planet".code = :code_5
      AND "example/Planet".float = :float_4
      AND "example/Planet"."user" = :user_3
      AND "example/Planet".pass = :pass_2
      AND ("example/Planet"._id > :id_1
           OR "example/Planet"._id IS NULL)
    ORDER BY "example/Planet".name ASC,
             "example/Planet".code ASC,
             "example/Planet".float ASC,
             "example/Planet"."user" ASC,
             "example/Planet".pass ASC,
             "example/Planet"._id ASC
    '''


def test_paginate_desc(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page(name, -code)
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        ''', 'example/Planet', '', {
        'name': 'test',
        '-code': 5,
        '_id': uuid.uuid4()
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet".code,
           "example/Planet"._id,
           "example/Planet"._revision
    FROM "example/Planet"
    WHERE "example/Planet".name > :name_1
      OR "example/Planet".name IS NULL
      OR "example/Planet".name = :name_2
      AND "example/Planet".code < :code_1
      OR "example/Planet".name = :name_3
      AND "example/Planet".code = :code_2
      AND ("example/Planet"._id > :id_1
           OR "example/Planet"._id IS NULL)
    ORDER BY "example/Planet".name ASC,
             "example/Planet".code DESC,
             "example/Planet"._id ASC
    '''


def test_paginate_disabled(rc: RawConfig):
    assert _build(rc, '''
        d | r | b | m | property   | type    | ref     | access | prepare
        example                    |         |         |        |
          |   |   | Planet         |         | name    |        | page()
          |   |   |   | name       | string  |         | open   |
          |   |   |   | code       | integer |         | open   |
        ''', 'example/Planet', '', {
        'name': 'test'
    }) == '''
    SELECT "example/Planet".name,
           "example/Planet".code,
           "example/Planet"._id,
           "example/Planet"._revision
    FROM "example/Planet"
    '''


# This is prone to change, when more supported types are added
# list of valid types are declared in: get_allowed_page_property_types
# Currently valid types are: Integer, Number, String, Date, DateTime, Time, PrimaryKey
def test_paginate_invalid_types(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | access | prepare
    example                    |         |         |        |
      |   |   | Planet         |         |         |        |
      |   |   |   | name       | string  |         | open   | 
      |   |   | Country        |         |         |        | page(planet)
      |   |   |   | name       | string  |         | open   |
      |   |   |   | planet     | ref     | Planet  | open   |
        ''', 'example/Country', '', {
        'planet': 'test'
    }) == '''
    SELECT "example/Country".name,
           "example/Country"."planet._id",
           "example/Country"._id,
           "example/Country"._revision
    FROM "example/Country"
    '''

    assert _build(rc, '''
    d | r | b | m | property   | type     | ref     | access | prepare
    example                    |          |         |        |
      |   |   | Country        |          |         |        | page(geo)
      |   |   |   | name       | string   |         | open   |
      |   |   |   | geo        | geometry |         | open   |
        ''', 'example/Country', '', {
        'geo': 'test'
    }) == '''
    SELECT "example/Country".name,
           ST_AsEWKB("example/Country".geo) AS geo,
           "example/Country"._id,
           "example/Country"._revision
    FROM "example/Country"
    '''

    assert _build(rc, '''
    d | r | b | m | property   | type     | ref     | access | prepare
    example                    |          |         |        |
      |   |   | Country        |          |         |        | page(fl)
      |   |   |   | name       | string   |         | open   |
      |   |   |   | fl         | file     |         | open   | file()
        ''', 'example/Country', '', {
        'fl': 'test'
    }) == '''
    SELECT "example/Country".name,
           "example/Country"."fl._id",
           "example/Country"."fl._content_type",
           "example/Country"."fl._size",
           "example/Country"."fl._bsize",
           "example/Country"."fl._blocks",
           "example/Country"._id,
           "example/Country"._revision
    FROM "example/Country"
    '''

    assert _build(rc, '''
    d | r | b | m | property   | type     | ref     | access | prepare
    example                    |          |         |        |
      |   |   | Country        |          |         |        | page(bool)
      |   |   |   | name       | string   |         | open   |
      |   |   |   | bool       | boolean  |         | open   |
        ''', 'example/Country', '', {
        'bool': 'test'
    }) == '''
    SELECT "example/Country".name,
           "example/Country".bool,
           "example/Country"._id,
           "example/Country"._revision
    FROM "example/Country"
    '''
