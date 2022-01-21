import logging
import pathlib
import types
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import tqdm

from spinta import commands
from spinta import components
from spinta.formats.components import Format
from spinta.components import Context
from spinta.components import DataStream
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.utils.aiotools import alist
from spinta.utils.itertools import peek

ModelRow = Tuple[Model, Dict[str, Any]]

log = logging.getLogger(__name__)


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
    *,
    stop_on_error: bool = False,
) -> Dict[str, int]:
    counts = {}
    for model in tqdm.tqdm(models, 'Count rows', ascii=True, leave=False):
        try:
            count = _get_row_count(context, model)
        except Exception:
            if stop_on_error:
                raise
            else:
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
    stop_on_error: bool = False,
) -> Iterable[Dict[str, Any]]:

    if limit is None:
        query = None
    else:
        query = Expr('limit', limit)

    stream = commands.getall(context, model, model.backend, query=query)

    if stop_on_error:
        stream = peek(stream)
    else:
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
    *,
    stop_on_error: bool = False,
) -> Iterator[ModelRow]:
    for model in models:
        rows = _read_model_data(context, model, limit, stop_on_error)
        count = counts.get(model.name)
        rows = tqdm.tqdm(rows, model.name, ascii=True, total=count, leave=False)
        for row in rows:
            yield model, row


async def process_stream(
    source: str,
    stream: DataStream,
    exporter: Optional[Format] = None,
    path: Optional[pathlib.Path] = None,
) -> None:
    if exporter:
        # TODO: Probably exporters should support async generators.
        if isinstance(stream, types.AsyncGeneratorType):
            stream = await alist(stream)
        chunks = exporter(stream)
        if path is None:
            for chunk in chunks:
                print(chunk, end='')
        else:
            with path.open('wb') as f:
                for chunk in chunks:
                    f.write(chunk)
    else:
        with tqdm.tqdm(desc=source, ascii=True) as pbar:
            async for _ in stream:
                pbar.update(1)


def ensure_data_dir(
    path: pathlib.Path,
):
    """Create default data directory if it does not exits"""
    path.mkdir(parents=True, exist_ok=True)
