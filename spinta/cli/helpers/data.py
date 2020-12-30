from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List

import tqdm

from spinta import commands
from spinta import components
from spinta.cli import log
from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.utils.itertools import peek

ModelRow = Dict[str, Any]


def _get_row_count(
    context: components.Context,
    model: components.Model,
) -> int:
    query = Expr('select', Expr('count'))
    stream = commands.getall(context, model, model.backend, query=query)
    for data in stream:
        return data['count()']


def count_rows(
    context: Context,
    models: List[Model],
    limit: int = None,
) -> Dict[str, int]:
    counts = {}
    for model in tqdm.tqdm(models, 'Count rows', ascii=True, leave=False):
        try:
            count = _get_row_count(context, model)
        except Exception:
            log.exception("Error on _get_row_count({model.name}).")
        else:
            if limit:
                count = min(count, limit)
            counts[model.name] = count
    return counts


def _read_model_data(
    context: components.Context,
    model: components.Model,
    limit: int = None,
) -> Iterable[ModelRow]:

    if limit is None:
        query = None
    else:
        query = Expr('limit', limit)

    stream = commands.getall(context, model, model.backend, query=query)

    try:
        stream = peek(stream)
    except Exception:
        log.exception(f"Error when reading data from model {model.name}")
        return

    yield from stream


def iter_model_rows(
    context: Context,
    models: List[Model],
    counts: Dict[str, int],
    limit: int = None,
) -> Iterator[ModelRow]:
    for model in models:
        rows = _read_model_data(context, model, limit)
        count = counts.get(model.name)
        rows = tqdm.tqdm(rows, model.name, ascii=True, total=count, leave=False)
        yield from rows
