import logging
import pathlib
import types
from copy import deepcopy
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
from spinta.cli.helpers.errors import ErrorCounter
from spinta.formats.components import Format
from spinta.components import Context, pagination_enabled
from spinta.components import DataStream
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.ufuncs.basequerybuilder.components import QueryParams
from spinta.types.datatype import Inherit
from spinta.ufuncs.basequerybuilder.helpers import add_page_expr
from spinta.utils.aiotools import alist
from spinta.utils.itertools import peek

ModelRow = Tuple[Model, Dict[str, Any]]

log = logging.getLogger(__name__)


def _get_row_count(
    context: components.Context,
    model: components.Model,
    page_data: Any
) -> int:
    query = Expr('select', Expr('count'))
    if pagination_enabled(model):
        copied = commands.create_page(model.page, page_data)
        copied.filter_only = True
        query = add_page_expr(query, copied)
    stream = commands.getall(context, model, model.backend, query=query)
    for data in stream:
        return data['count()']


def count_rows(
    context: Context,
    models: List[Model],
    limit: int = None,
    *,
    initial_page_data: dict = None,
    stop_on_error: bool = False,
    error_counter: ErrorCounter = None,
    no_progress_bar: bool = False
) -> Dict[str, int]:
    counts = {}
    if initial_page_data is None:
        initial_page_data = {}

    models = models if no_progress_bar else tqdm.tqdm(models, 'Count rows', ascii=True, leave=False)
    for model in models:
        try:
            count = _get_row_count(context, model, initial_page_data.get(model.model_type(), None))
        except Exception:
            if error_counter:
                error_counter.increase()
                if error_counter.has_reached_max():
                    break
            if stop_on_error:
                break
            else:
                log.exception("Error on _get_row_count({model.name}).")
        else:
            if limit:
                count = min(count, limit)
            counts[model.name] = count
    return counts


def read_model_data(
    context: components.Context,
    model: components.Model,
    limit: int = None,
    stop_on_error: bool = False,
    params: QueryParams = None
) -> Iterable[Dict[str, Any]]:

    if limit is None:
        query = None
    else:
        query = Expr('limit', limit)

    stream = commands.getall(context, model, model.backend, query=query, params=params)

    if stop_on_error:
        stream = peek(stream)
    else:
        try:
            stream = peek(stream)
        except Exception:
            log.exception(f"Error when reading data from model {model.name}")
            return

    prop_filer, needs_filtering = filter_allowed_props_for_model(model)
    for item in stream:
        if needs_filtering:
            item = filter_dict_by_keys(prop_filer, item)
        yield item


def filter_allowed_props_for_model(model: Model) -> (dict, bool):
    if model.base:
        allowed_props = model.properties
        for name, prop in model.base.parent.properties.items():
            if not name.startswith('_'):
                if name in allowed_props and isinstance(allowed_props[name].dtype, Inherit):
                    allowed_props.pop(name)
        return allowed_props, True
    return model.properties, False


def filter_dict_by_keys(dict1: dict, dict2: dict) -> dict:
    result = {}
    for key in dict1:
        if key in dict2:
            result[key] = dict2[key]
    return result


def iter_model_rows(
    context: Context,
    models: List[Model],
    counts: Dict[str, int],
    limit: int = None,
    *,
    stop_on_error: bool = False,
    no_progress_bar: bool = False,
) -> Iterator[ModelRow]:
    for model in models:
        rows = read_model_data(context, model, limit, stop_on_error)
        if not no_progress_bar:
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
