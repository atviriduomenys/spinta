import logging
from typing import Iterator

from spinta import commands
from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.helpers import handle_ref_key_assignment, generate_pk_for_row
from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.datasets.helpers import get_enum_filters
from spinta.datasets.helpers import get_ref_filters
from spinta.datasets.keymaps.components import KeyMap
from spinta.datasets.utils import iterparams
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.typing import ObjectData
from spinta.ufuncs.basequerybuilder.components import QueryParams
from spinta.ufuncs.basequerybuilder.helpers import get_page_values
from spinta.ufuncs.helpers import merge_formulas
from spinta.ufuncs.resultbuilder.helpers import get_row_value
from spinta.utils.nestedstruct import flat_dicts_to_nested

log = logging.getLogger(__name__)


@commands.getall.register(Context, Model, Sql)
def getall(
    context: Context,
    model: Model,
    backend: Sql,
    *,
    query: Expr = None,
    params: QueryParams = None,
    **kwargs
) -> Iterator[ObjectData]:
    conn = context.get(f'transaction.{backend.name}')
    builder = SqlQueryBuilder(context)
    builder.update(model=model)
    # Merge user passed query with query set in manifest.
    query = merge_formulas(model.external.prepare, query)
    query = merge_formulas(query, get_enum_filters(context, model))
    query = merge_formulas(query, get_ref_filters(context, model))
    keymap: KeyMap = context.get(f'keymap.{model.keymap.name}')
    for model_params in iterparams(context, model, model.manifest):
        table = model.external.name.format(**model_params)
        table = backend.get_table(model, table)
        env = builder.init(backend, table, params)
        env.update(params=model_params)
        expr = env.resolve(query)
        where = env.execute(expr)
        qry = env.build(where)
        for row in conn.execute(qry):
            res = {}

            for key, sel in env.selected.items():
                val = get_row_value(context, backend, row, sel)
                if sel.prop:
                    if isinstance(sel.prop.dtype, PrimaryKey):
                        val = generate_pk_for_row(sel.prop.model, row, keymap, val)
                    elif isinstance(sel.prop.dtype, Ref):
                        val = handle_ref_key_assignment(keymap, env, val, sel.prop.dtype)
                res[key] = val

            if model.page.is_enabled:
                res['_page'] = get_page_values(env, row)

            res['_type'] = model.model_type()
            res = flat_dicts_to_nested(res)
            res = commands.cast_backend_to_python(context, model, backend, res)
            yield res

