import textwrap
from typing import Type
from typing import Dict
from typing import Tuple

import sqlalchemy as sa
from sqlalchemy.sql import Select
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.dialects.postgresql import UUID

from spinta import spyna
from spinta.auth import AdminToken
from spinta.components import Model
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr
from spinta.backends.postgresql.commands.query import PgQueryBuilder
from spinta.backends.postgresql.components import PostgreSQL
from spinta.manifests.components import Manifest
from spinta.testing.manifest import load_manifest_and_context
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref


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
    if dtype.name == 'string':
        return sa.Text
    if dtype.name == 'boolean':
        return sa.Boolean
    raise NotImplementedError(dtype.name)


def _get_model_db_name(model: Model) -> str:
    return model.get_name_without_ns().upper()


def _meta_from_manifest(
    manifest: Manifest,
) -> Tuple[sa.MetaData, Dict[str, sa.Table]]:
    meta = sa.MetaData()
    tables: Dict[str, sa.Table] = {}
    for model in manifest.models.values():
        columns = [
            sa.Column('_id', UUID(), primary_key=True),
            sa.Column('_revision', sa.String()),
        ] + [
            sa.Column(prop.external.name, _get_sql_type(prop.dtype))
            for name, prop in model.properties.items()
            if (
                prop.external and
                prop.external.name and
                not prop.is_reserved()
            )
        ]
        table = sa.Table(_get_model_db_name(model), meta, *columns)
        tables[model.model_type()] = table
    return meta, tables


def _build(rc: RawConfig, manifest: str, model_name: str, query: str) -> str:
    context, manifest = load_manifest_and_context(rc, manifest)
    context.set('auth.token', AdminToken())
    model = manifest.models[model_name]
    meta, tables = _meta_from_manifest(manifest)
    backend = PostgreSQL()
    backend.schema = meta
    backend.tables = tables
    builder = PgQueryBuilder(context)
    builder.update(model=model)
    env = builder.init(backend, meta.tables[_get_model_db_name(model)])
    query = asttoexpr(spyna.parse(query))
    expr = env.resolve(query)
    where = env.execute(expr)
    qry = env.build(where)
    return _qry(qry)


def test_select_id(rc: RawConfig):
    assert _build(rc, '''
    d | r | b | m | property   | type    | ref     | source     | prepare    | access
    example                    |         |         |            |            |
      |   |   | City           |         | name    | CITY       |            |
      |   |   |   | name       | string  |         | NAME       |            | open
    ''', 'example/City', 'select(_id)') == '''
    SELECT
      "CITY"._id, "CITY"._revision
    FROM "CITY"
    '''
