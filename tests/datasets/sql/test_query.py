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


def _qry(qry: Select, indent: int = 4) -> str:
    ln = '\n'
    indent_ = ' ' * indent
    qry = str(qry)
    qry = qry.replace(' JOIN ', '\nJOIN ')
    qry = '\n'.join([
        line.rstrip()
        for line in qry.splitlines()
    ])
    return ln + textwrap.indent(qry, indent_) + ln + indent_


def _get_sql_type(dtype: DataType) -> Type[TypeEngine]:
    if isinstance(dtype, Ref):
        return _get_sql_type(dtype.refprops[0].dtype)
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
            sa.Column(name.upper(), _get_sql_type(prop.dtype))
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
    expr = env.resolve(model.external.prepare)
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
    SELECT "COUNTRY"."NAME", "COUNTRY"."DEMOCRATIC"
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
    SELECT "CITY"."NAME", "CITY"."COUNTRY"
    FROM "CITY"
    JOIN "COUNTRY" ON "CITY"."COUNTRY" = "COUNTRY"."NAME"
    WHERE "COUNTRY"."DEMOCRATIC"
    '''
