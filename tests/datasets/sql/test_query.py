import textwrap
from typing import Type

import sqlalchemy as sa
from sqlalchemy.sql import Select
from sqlalchemy.sql.type_api import TypeEngine

from spinta.auth import AdminToken
from spinta.components import Model
from spinta.core.config import RawConfig
from spinta.datasets.backends.sql.commands.query import SqlQueryBuilder
from spinta.datasets.backends.sql.components import Sql
from spinta.manifests.components import Manifest
from spinta.testing.manifest import load_manifest_and_context
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref
from spinta.ufuncs.helpers import merge_formulas
from spinta.datasets.helpers import get_enum_filters
from spinta.datasets.helpers import get_ref_filters


def _qry(qry: Select, indent: int = 4) -> str:
    ln = '\n'
    indent_ = ' ' * indent
    qry = str(qry)
    qry = qry.replace('SELECT ', 'SELECT\n  ')
    qry = qry.replace('", "', '",\n  "')
    qry = qry.replace(' LEFT OUTER JOIN ', '\nLEFT OUTER JOIN ')
    qry = qry.replace(' AND ', '\n  AND ')
    qry = '\n'.join([
        line.rstrip()
        for line in qry.splitlines()
    ])
    return ln + textwrap.indent(qry, indent_) + ln + indent_


def _get_sql_type(dtype: DataType) -> Type[TypeEngine]:
    if isinstance(dtype, Ref):
        return _get_sql_type(dtype.refprops[0].dtype)
    if dtype.name == 'integer':
        return sa.Integer
    if dtype.name == 'string':
        return sa.Text
    if dtype.name == 'boolean':
        return sa.Boolean
    raise NotImplementedError(dtype.name)


def _get_model_db_name(model: Model) -> str:
    return model.get_name_without_ns().upper()


def _meta_from_manifest(manifest: Manifest) -> sa.MetaData:
    meta = sa.MetaData()
    for model in manifest.models.values():
        columns = [
            sa.Column(prop.external.name, _get_sql_type(prop.dtype))
            for name, prop in model.properties.items()
            if (
                prop.external and
                prop.external.name and
                not prop.is_reserved()
            )
        ]
        sa.Table(_get_model_db_name(model), meta, *columns)
    return meta


def _build(rc: RawConfig, manifest: str, model_name: str) -> str:
    context, manifest = load_manifest_and_context(rc, manifest)
    context.set('auth.token', AdminToken())
    model = manifest.models[model_name]
    meta = _meta_from_manifest(manifest)
    backend = Sql()
    backend.schema = meta
    builder = SqlQueryBuilder(context)
    builder.update(model=model)
    env = builder.init(backend, meta.tables[_get_model_db_name(model)])
    query = model.external.prepare
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)
    return _qry(qry)


def test_unresolved(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | source     | prepare    | access
    example                    |         |         |            |            |
      |   |   | Country        |         | name    | COUNTRY    | democratic |
      |   |   |   | name       | string  |         | NAME       |            | open
      |   |   |   | democratic | boolean |         | DEMOCRATIC |            | open
      |   |   | City           |         | name    | CITY       |            |
      |   |   |   | name       | string  |         | NAME       |            | open
      |   |   |   | country    | ref     | Country | COUNTRY    |            | open
    ''', 'example/Country') == '''
    SELECT
      "COUNTRY"."NAME",
      "COUNTRY"."DEMOCRATIC"
    FROM "COUNTRY"
    WHERE "COUNTRY"."DEMOCRATIC"
    '''


def test_unresolved_getattr(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | source     | prepare            | access
    example                    |         |         |            |                    |
      |   |   | Country        |         | name    | COUNTRY    |                    |
      |   |   |   | name       | string  |         | NAME       |                    | open
      |   |   |   | democratic | boolean |         | DEMOCRATIC |                    | open
      |   |   | City           |         | name    | CITY       | country.democratic |
      |   |   |   | name       | string  |         | NAME       |                    | open
      |   |   |   | country    | ref     | Country | COUNTRY    |                    | open
    ''', 'example/City') == '''
    SELECT
      "CITY"."NAME",
      "COUNTRY_1"."NAME" AS "NAME_1"
    FROM "CITY"
    LEFT OUTER JOIN "COUNTRY" AS "COUNTRY_1" ON "CITY"."COUNTRY" = "COUNTRY_1"."NAME"
    WHERE "COUNTRY_1"."DEMOCRATIC"
    '''


def test_join_aliases(rc: RawConfig):
    assert _build(rc, '''
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
    ''', 'example/Street') == '''
    SELECT
      "STREET"."NAME",
      "CITY_1"."ID",
      "COUNTRY_1"."ID" AS "ID_1",
      "PLANET_1"."ID" AS "ID_2"
    FROM "STREET"
    LEFT OUTER JOIN "CITY" AS "CITY_1" ON "STREET"."CITY_ID" = "CITY_1"."ID"
    LEFT OUTER JOIN "COUNTRY" AS "COUNTRY_2" ON "CITY_1"."COUNTRY_ID" = "COUNTRY_2"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_2" ON "COUNTRY_2"."PLANET_ID" = "PLANET_2"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_3" ON "CITY_1"."PLANET_ID" = "PLANET_3"."ID"
    LEFT OUTER JOIN "COUNTRY" AS "COUNTRY_1" ON "STREET"."COUNTRY_ID" = "COUNTRY_1"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_4" ON "COUNTRY_1"."PLANET_ID" = "PLANET_4"."ID"
    LEFT OUTER JOIN "PLANET" AS "PLANET_1" ON "STREET"."PLANET_ID" = "PLANET_1"."ID"
    WHERE ("CITY_1"."NAME" IS NULL OR "CITY_1"."NAME" = :NAME_1)
      AND ("COUNTRY_2"."CODE" IS NULL OR "COUNTRY_2"."CODE" = :CODE_1)
      AND ("PLANET_2"."CODE" IS NULL OR "PLANET_2"."CODE" = :CODE_2)
      AND ("PLANET_3"."CODE" IS NULL OR "PLANET_3"."CODE" = :CODE_3)
      AND ("COUNTRY_1"."CODE" IS NULL OR "COUNTRY_1"."CODE" = :CODE_4)
      AND ("PLANET_4"."CODE" IS NULL OR "PLANET_4"."CODE" = :CODE_5)
      AND ("PLANET_1"."CODE" IS NULL OR "PLANET_1"."CODE" = :CODE_6)
    '''


def test_group(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property | type   | ref | source  | prepare                       | access
    example                  |        |     |         |                               |
      | data                 | sql    |     |         |                               |
      |   |   | Country      |        | id  | COUNTRY | (code = null \\| code = 'lt') |
      |   |   |   | id       | string |     | ID      |                               | open
      |   |   |   | code     | string |     | CODE    |                               | open
      |   |   |   | name     | string |     | NAME    |                               | open
    ''', 'example/Country') == '''
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE",
      "COUNTRY"."NAME"
    FROM "COUNTRY"
    WHERE "COUNTRY"."CODE" IS NULL OR "COUNTRY"."CODE" = :CODE_1
    '''


def test_group_2(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property | type   | ref | source  | prepare                                                | access
    example                  |        |     |         |                                                        |
      | data                 | sql    |     |         |                                                        |
      |   |   | Country      |        | id  | COUNTRY | (code = null \\| code = 'lt') & (name = null \\| name) |
      |   |   |   | id       | string |     | ID      |                                                        | open
      |   |   |   | code     | string |     | CODE    |                                                        | open
      |   |   |   | name     | string |     | NAME    |                                                        | open
    ''', 'example/Country') == '''
    SELECT
      "COUNTRY"."ID",
      "COUNTRY"."CODE",
      "COUNTRY"."NAME"
    FROM "COUNTRY"
    WHERE ("COUNTRY"."CODE" IS NULL OR "COUNTRY"."CODE" = :CODE_1)
      AND ("COUNTRY"."NAME" IS NULL OR "COUNTRY"."NAME")
    '''


def test_explicit_ref(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref           | source     | access
    example                    |         |               |            |
      |   |   | Country        |         | id            | COUNTRY    |
      |   |   |   | id         | integer |               | ID         | open
      |   |   |   | code       | string  |               | NAME       | open
      |   |   | City           |         | name          | CITY       |
      |   |   |   | name       | string  |               | NAME       | open
      |   |   |   | country    | ref     | Country[code] | COUNTRY_ID | open
    ''', 'example/City') == '''
    SELECT
      "CITY"."NAME",
      "COUNTRY_1"."ID"
    FROM "CITY"
    LEFT OUTER JOIN "COUNTRY" AS "COUNTRY_1" ON "CITY"."COUNTRY_ID" = "COUNTRY_1"."NAME"
    '''
