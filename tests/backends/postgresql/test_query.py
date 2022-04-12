import textwrap

import sqlparse
import sqlalchemy as sa
from sqlalchemy.sql import Select

from spinta import spyna
from spinta import commands
from spinta.auth import AdminToken
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.backends.postgresql.commands.query import PgQueryBuilder
from spinta.backends.postgresql.components import PostgreSQL
from spinta.testing.manifest import load_manifest_and_context


def _qry(qry: Select, indent: int = 4) -> str:
    sql = str(qry) % qry.compile().params
    sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
    ln = '\n'
    indent_ = ' ' * indent
    return ln + textwrap.indent(sql, indent_) + ln + indent_


def _build(rc: RawConfig, manifest: str, model_name: str, query: str) -> str:
    context, manifest = load_manifest_and_context(rc, manifest)
    context.set('auth.token', AdminToken())
    backend = PostgreSQL()
    backend.name = 'default'
    backend.schema = sa.MetaData()
    backend.tables = {}
    commands.prepare(context, backend, manifest)
    model = manifest.models[model_name]
    builder = PgQueryBuilder(context)
    builder.update(model=model)
    env = builder.init(backend, backend.get_table(model))
    query = asttoexpr(spyna.parse(query))
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
    SELECT "example/City"._id,
           "example/City"._revision,
           "example/City".name,
           "example/City"."country._id"
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
    SELECT "example/City"._id,
           "example/City"._revision,
           "example/City".name,
           "example/Country".name AS "country.name"
    FROM "example/City"
    JOIN "example/Country" ON "example/City"."country._id" = "example/Country"._id
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
           "example/City"._revision,
           "example/Country".name AS "country.name"
    FROM "example/City"
    JOIN "example/Country" ON "example/City"."country._id" = "example/Country"._id
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
           "example/City"._revision,
           "example/Country".name AS "country.name"
    FROM "example/City"
    JOIN "example/Country" ON "example/City"."country._id" = "example/Country"._id
    '''
