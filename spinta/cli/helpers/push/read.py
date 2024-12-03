from copy import deepcopy
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List

import requests
import sqlalchemy as sa
import tqdm

from spinta import commands
from spinta.cli.helpers.data import ModelRow, count_rows, read_model_data, filter_allowed_props_for_model, \
    filter_dict_by_keys
from spinta.cli.helpers.errors import ErrorCounter
from spinta.cli.helpers.push.components import State, PushRow, PUSH_NOW
from spinta.cli.helpers.push.delete import get_deleted_rows
from spinta.cli.helpers.push.error import get_rows_with_errors, get_rows_with_errors_counts
from spinta.cli.helpers.push.utils import get_data_checksum, extract_state_page_keys, update_model_page_with_new, \
    construct_where_condition_from_page
import spinta.cli.push as cli_push
from spinta.commands.read import get_page, PaginationMetaData, get_paginated_values
from spinta.components import Context, pagination_enabled
from spinta.components import Model
from spinta.components import Page, get_page_size
from spinta.core.ufuncs import Expr
from spinta.ufuncs.basequerybuilder.components import QueryParams
from spinta.utils.itertools import peek


def _iter_model_rows(
    context: Context,
    models: List[Model],
    counts: Dict[str, int],
    metadata: sa.MetaData,
    limit: int = None,
    *,
    initial_page_data: dict = None,
    stop_on_error: bool = False,
    no_progress_bar: bool = False,
    push_counter: tqdm.tqdm = None,
) -> Iterator[ModelRow]:
    if initial_page_data is None:
        initial_page_data = {}

    params = QueryParams()
    params.push = True
    for model in models:

        model_push_counter = None
        if not no_progress_bar:
            count = counts.get(model.name)
            model_push_counter = tqdm.tqdm(desc=model.name, ascii=True, total=count, leave=False)

        if pagination_enabled(model):
            page = commands.create_page(model.page, initial_page_data.get(model.model_type(), None))
            rows = _read_rows_by_pages(
                context,
                model,
                page,
                metadata,
                limit,
                stop_on_error,
                push_counter,
                model_push_counter,
                params=params
            )
            for row in rows:
                yield row
        else:
            stream = read_model_data(
                context,
                model,
                limit,
                stop_on_error,
                params=params
            )
            for item in stream:
                if push_counter:
                    push_counter.update(1)
                if model_push_counter:
                    model_push_counter.update(1)
                yield PushRow(model, item)

        if model_push_counter is not None:
            model_push_counter.close()


def _get_model_rows(
    context: Context,
    models: List[Model],
    metadata: sa.MetaData,
    limit: int = None,
    *,
    initial_page_data: dict,
    stop_on_error: bool = False,
    no_progress_bar: bool = False,
    error_counter: ErrorCounter = None,
) -> Iterator[PushRow]:
    counts = (
        count_rows(
            context,
            models,
            limit,
            stop_on_error=stop_on_error,
            error_counter=error_counter,
            initial_page_data=initial_page_data
        )
        if not no_progress_bar
        else {}
    )
    push_counter = None
    if not no_progress_bar:
        push_counter = tqdm.tqdm(desc='PUSH', ascii=True, total=sum(counts.values()))
    rows = _iter_model_rows(
        context,
        models,
        counts,
        metadata,
        limit,
        initial_page_data=initial_page_data,
        stop_on_error=stop_on_error,
        no_progress_bar=no_progress_bar,
        push_counter=push_counter,
    )
    for row in rows:
        yield row

    if push_counter is not None:
        push_counter.close()


def read_rows(
    context: Context,
    client: requests.Session,
    server: str,
    models: List[Model],
    state: State,
    limit: int = None,
    *,
    timeout: tuple[float, float],
    stop_on_error: bool = False,
    retry_count: int = 5,
    no_progress_bar: bool = False,
    error_counter: ErrorCounter = None,
    initial_page_data: dict = None
) -> Iterator[PushRow]:
    if initial_page_data is None:
        initial_page_data = {}

    yield from _get_model_rows(
        context,
        models,
        state.metadata,
        limit,
        stop_on_error=stop_on_error,
        no_progress_bar=no_progress_bar,
        error_counter=error_counter,
        initial_page_data=initial_page_data
    )

    yield PUSH_NOW
    yield from get_deleted_rows(
        models,
        context,
        state.metadata,
        no_progress_bar=no_progress_bar,
    )

    for i in range(1, retry_count + 1):
        yield PUSH_NOW
        counts = (
            get_rows_with_errors_counts(
                models,
                context,
                state.metadata
            )
        )
        total_count = sum(counts.values())

        if total_count > 0:
            yield from get_rows_with_errors(
                client,
                server,
                models,
                context,
                state.metadata,
                counts,
                retry=i,
                timeout=timeout,
                no_progress_bar=no_progress_bar,
                error_counter=error_counter,
            )
        else:
            break


def _read_model_data_with_page(
    context: Context,
    model: Model,
    model_page: Page,
    limit: int = None,
    stop_on_error: bool = False,
    params: QueryParams = None
) -> Iterable[Dict[str, Any]]:

    if limit is None:
        query = None
    else:
        query = Expr('limit', limit)

    stream = get_page(
        context,
        model,
        model.backend,
        model_page,
        query,
        limit,
        params=params
    )

    if stop_on_error:
        stream = peek(stream)
    else:
        try:
            stream = peek(stream)
        except Exception:
            cli_push.log.exception(f"Error when reading data from model {model.name}")
            return

    prop_filer, needs_filtering = filter_allowed_props_for_model(model)
    for item in stream:
        if needs_filtering:
            item = filter_dict_by_keys(prop_filer, item)
        yield item


def _read_rows_by_pages(
    context: Context,
    model: Model,
    page: Page,
    metadata: sa.MetaData,
    limit: int = None,
    stop_on_error: bool = False,
    push_counter: tqdm.tqdm = None,
    model_push_counter: tqdm.tqdm = None,
    params: QueryParams = None
) -> Iterator[PushRow]:
    conn = context.get('push.state.conn')
    config = context.get('config')

    size = get_page_size(config, model)
    model_table = metadata.tables[model.name]
    state_rows = _get_state_rows_with_page(
        context,
        deepcopy(page),
        model_table,
        size
    )
    rows = _read_model_data_with_page(
        context,
        model,
        deepcopy(page),
        limit,
        stop_on_error,
        params
    )
    total_count = 0
    data_push_count = 0
    state_push_count = 0
    data_row = next(rows, None)
    state_row = next(state_rows, None)

    while True:
        if limit:
            if total_count >= limit:
                break
            total_count += 1

        update_counter = True

        if data_push_count >= size or state_push_count >= size:
            state_rows = _get_state_rows_with_page(
                context,
                deepcopy(page),
                model_table,
                size
            )
            rows = _read_model_data_with_page(
                context,
                model,
                deepcopy(page),
                limit,
                stop_on_error
            )

            data_push_count = 0
            state_push_count = 0
            data_row = next(rows, None)
            state_row = next(state_rows, None)

        if data_row is None and state_row is None:
            break

        if data_row is not None and state_row is not None:
            row = PushRow(model, data_row)
            row.op = 'insert'
            row.checksum = get_data_checksum(row.data, model)

            equals = _compare_data_with_state_rows(data_row, state_row, model_table)
            if equals:
                row.op = 'patch'
                row.saved = True
                row.data['_revision'] = state_row[model_table.c.revision]
                if state_row[model_table.c.checksum] != row.checksum or not _compare_data_with_state_row_keys(data_row, state_row, model_table, page):
                    yield row

                data_push_count += 1
                state_push_count += 1
                update_model_page_with_new(page, model_table, data_row=data_row)
                data_row = next(rows, None)
                state_row = next(state_rows, None)

            else:
                delete_cond = _compare_for_delete_row(state_row, data_row, model_table, page)
                if delete_cond:
                    conn.execute(
                        sa.update(model_table).where(model_table.c.id == state_row["id"]).values(pushed=None)
                    )

                    state_push_count += 1
                    update_model_page_with_new(page, model_table, state_row=state_row)
                    state_row = next(state_rows, None)
                    update_counter = False
                else:
                    yield row

                    data_push_count += 1
                    update_model_page_with_new(page, model_table, data_row=data_row)
                    data_row = next(rows, None)
        else:
            if data_row is not None:
                row = PushRow(model, data_row)
                row.op = 'insert'
                row.checksum = get_data_checksum(row.data, model)
                yield row

                data_push_count += 1
                update_model_page_with_new(page, model_table, data_row=data_row)
                data_row = next(rows, None)
            elif state_row is not None:
                conn.execute(
                    sa.update(model_table).where(model_table.c.id == state_row["id"]).values(pushed=None)
                )

                state_push_count += 1
                update_model_page_with_new(page, model_table, state_row=state_row)
                state_row = next(state_rows, None)
                update_counter = False

        if update_counter:
            if push_counter is not None:
                push_counter.update(1)
            if model_push_counter is not None:
                model_push_counter.update(1)


def _get_state_rows_with_page(
    context: Context,
    model_page: Page,
    table: sa.Table,
    size: int
) -> sa.engine.LegacyCursorResult:
    conn = context.get('push.state.conn')
    model_page.size = size + 1
    order_by = []

    for by, page_by in model_page.by.items():
        if by.startswith('-'):
            order_by.append(sa.sql.expression.nullsfirst(sa.desc(table.c[f"page.{page_by.prop.name}"])))
        else:
            order_by.append(sa.sql.expression.nullslast(sa.asc(table.c[f"page.{page_by.prop.name}"])))

    page_meta = PaginationMetaData(
        page_size=size,
    )

    while not page_meta.is_finished:
        page_meta.is_finished = True
        from_cond = construct_where_condition_from_page(model_page, table)

        if from_cond is not None:
            stmt = sa.select([table]).where(
                from_cond
            ).order_by(*order_by).limit(model_page.size)
            rows = conn.execute(
                stmt
            )
        else:
            stmt = sa.select([table]).order_by(*order_by).limit(model_page.size)
            rows = conn.execute(
                stmt
            )

        yield from get_paginated_values(model_page, page_meta, rows, extract_state_page_keys)


def _compare_data_with_state_rows(
    data_row: Any,
    state_row: Any,
    model_table: sa.Table,
):
    return data_row["_id"] == state_row[model_table.c["id"]]


def _compare_data_with_state_row_keys(
    data_row: Any,
    state_row: Any,
    model_table: sa.Table,
    page: Page
):
    equals = True
    decoded = data_row.get("_page")
    for i, (by, page_by) in enumerate(page.by.items()):
        state_val = state_row[model_table.c[f"page.{page_by.prop.name}"]]
        data_val = decoded[i]
        if state_val != data_val:
            equals = False
            break
    return equals


def _compare_for_delete_row(
    state_row: Any,
    data_row: Any,
    model_table: sa.Table,
    page: Page,
):
    delete_cond = False

    if '_page' in data_row:
        decoded = data_row['_page']
        for i, (by, page_by) in enumerate(page.by.items()):
            state_val = state_row[model_table.c[f"page.{page_by.prop.name}"]]
            data_val = decoded[i]
            compare = True
            if state_val is not None and data_val is not None:
                if by.startswith('-'):
                    compare = state_val > data_val
                else:
                    compare = state_val < data_val
            if state_val != data_val:
                delete_cond = compare
                break
    else:
        delete_cond = True
    return delete_cond
