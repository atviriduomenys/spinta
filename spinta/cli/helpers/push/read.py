from copy import deepcopy
from typing import Any

import requests
import sqlalchemy as sa
import tqdm

from spinta import commands
from spinta.cli.helpers.data import count_rows, read_model_data
from spinta.cli.helpers.errors import ErrorCounter
from spinta.cli.helpers.push.components import PUSH_NOW, PushOperation, PushRow, PushRows, State
from spinta.cli.helpers.push.delete import get_deleted_rows
from spinta.cli.helpers.push.error import get_rows_with_errors, get_rows_with_errors_counts
from spinta.cli.helpers.push.state import get_state_row
from spinta.cli.helpers.push.utils import (
    construct_where_condition_from_page,
    extract_state_page_keys,
    get_data_checksum,
    update_model_page_with_new,
)
from spinta.commands.read import PaginationMetaData, get_paginated_values
from spinta.components import Context, Model, Page, get_page_size, pagination_enabled
from spinta.ufuncs.querybuilder.components import QueryParams


def _generate_push_rows(
    context: Context,
    models: list[Model],
    counts: dict[str, int],
    metadata: sa.MetaData,
    *,
    limit: int | None = None,
    initial_page_data: dict | None = None,
    stop_on_error: bool = False,
    no_progress_bar: bool = False,
    push_counter: tqdm.tqdm | None = None,
) -> PushRows:
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
            yield from _generate_paginated_push_rows(
                context,
                model,
                page,
                metadata,
                params,
                limit=limit,
                stop_on_error=stop_on_error,
                push_counter=push_counter,
                model_push_counter=model_push_counter,
            )
        else:
            yield from _generate_non_paginated_push_rows(
                context,
                model,
                params,
                push_counter=push_counter,
                model_push_counter=model_push_counter,
                limit=limit,
                stop_on_error=stop_on_error,
            )

        if model_push_counter is not None:
            model_push_counter.close()


def _generate_non_paginated_push_rows(
    context: Context,
    model: Model,
    params: QueryParams,
    *,
    push_counter: tqdm.tqdm | None = None,
    model_push_counter: tqdm.tqdm | None = None,
    limit: int | None = None,
    stop_on_error: bool = False,
) -> PushRows:
    """
    This algorithm is unable to return the ` delete ` action, since it is unable to deterministically sort values
    to use merge join.

    To be able to know which rows need to be deleted, we need to return PushRow with empty ` op ` which will be used
    to know which push state rows need to update ` session_id `. We can delete rows by comparaing current ` session_id `
    with stored ones.
    """

    conn = context.get("push.state.conn")
    table = conn.metadata.tables[model.name]

    stream = read_model_data(context, model, limit, stop_on_error, params=params)
    for item in stream:
        if push_counter is not None:
            push_counter.update(1)
        if model_push_counter is not None:
            model_push_counter.update(1)

        item_id = item.get("_id", None)
        state_row = get_state_row(context, model, item_id)

        row = PushRow(model=model, data=item, checksum=get_data_checksum(item, model), op=PushOperation.UNCHANGED)
        if state_row is None:
            row.op = PushOperation.INSERT
            yield row
            continue

        if state_row[table.c.checksum] != row.checksum:
            row.op = PushOperation.PATCH
            row.saved = True
            row.data["_revision"] = state_row[table.c.revision]
            yield row
            continue

        # Yield row without op; this way we only know that session_id needs to be updated.
        yield row


def _generate_paginated_push_rows(
    context: Context,
    model: Model,
    page: Page,
    metadata: sa.MetaData,
    params: QueryParams,
    *,
    limit: int | None = None,
    stop_on_error: bool = False,
    push_counter: tqdm.tqdm | None = None,
    model_push_counter: tqdm.tqdm | None = None,
) -> PushRows:
    """
    This algorithm is the more advanced version of the non-paginated one, since it is able to sort values by deterministic keys, which
    in turn allows us to use merge join to compare data rows with push state rows.

    This algorithm reuses delete logic from non-paginated algorithm by updating ` session_id ` of all existing data in the source.

    The main advantage of this vs. non-paginated is that it does not need to call a select state row with a specific ` _ id ` for each
    data row.
    """

    def _generate_rows() -> PushRows:
        config = context.get("config")

        size = get_page_size(config, model)
        model_table = metadata.tables[model.name]
        state_rows = _get_state_rows_with_page(context, deepcopy(page), model_table, size)
        rows = read_model_data(
            context, model, page=deepcopy(page), limit=limit, stop_on_error=stop_on_error, params=params
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

            if data_push_count >= size or state_push_count >= size:
                state_rows = _get_state_rows_with_page(context, deepcopy(page), model_table, size)
                rows = read_model_data(context, model, page=deepcopy(page), limit=limit, stop_on_error=stop_on_error)

                data_push_count = 0
                state_push_count = 0
                data_row = next(rows, None)
                state_row = next(state_rows, None)

            if data_row is None and state_row is None:
                break

            if data_row is not None and state_row is not None:
                row = PushRow(
                    model=model, data=data_row, checksum=get_data_checksum(data_row, model), op=PushOperation.UNCHANGED
                )

                equals = _compare_data_with_state_rows(data_row, state_row, model_table)
                if equals:
                    if state_row[model_table.c.checksum] != row.checksum or not _compare_data_with_state_row_keys(
                        data_row, state_row, model_table, page
                    ):
                        row.op = PushOperation.PATCH
                        row.saved = True
                        row.data["_revision"] = state_row[model_table.c.revision]
                        yield row

                    update_model_page_with_new(page, model_table, data_row=data_row)
                    data_row = next(rows, None)
                    state_row = next(state_rows, None)
                    data_push_count += 1
                    state_push_count += 1
                    continue

                delete_cond = _compare_for_delete_row(state_row, data_row, model_table, page)
                update_model_page_with_new(page, model_table, data_row=data_row)
                if not delete_cond:
                    data_row = next(rows, None)
                    data_push_count += 1
                    row.op = PushOperation.INSERT
                    yield row
                    continue

                state_row = next(state_rows, None)
                state_push_count += 1
                # By not returning the row, it will be treated for deletion, since session_id will be outdated
                continue

            # Only data from source given
            if data_row is not None:
                row = PushRow(
                    model,
                    data_row,
                    op=PushOperation.INSERT,
                    checksum=get_data_checksum(data_row, model),
                )
                data_push_count += 1
                update_model_page_with_new(page, model_table, data_row=data_row)
                data_row = next(rows, None)
                yield row
                continue

            # Only state from target given
            state_push_count += 1
            update_model_page_with_new(page, model_table, state_row=state_row)
            state_row = next(state_rows, None)
            # By not returning the row, it will be treated for deletion, since session_id will be outdated

    for result_row in _generate_rows():
        if push_counter is not None:
            push_counter.update(1)
        if model_push_counter is not None:
            model_push_counter.update(1)

        yield result_row


def _get_model_rows(
    context: Context,
    models: list[Model],
    metadata: sa.MetaData,
    *,
    limit: int | None = None,
    initial_page_data: dict,
    stop_on_error: bool = False,
    no_progress_bar: bool = False,
    error_counter: ErrorCounter | None = None,
) -> PushRows:
    counts = (
        count_rows(
            context,
            models,
            limit=limit,
            stop_on_error=stop_on_error,
            error_counter=error_counter,
            initial_page_data=initial_page_data,
        )
        if not no_progress_bar
        else {}
    )
    push_counter = None
    if not no_progress_bar:
        push_counter = tqdm.tqdm(desc="PUSH", ascii=True, total=sum(counts.values()))
    rows = _generate_push_rows(
        context,
        models,
        counts,
        metadata,
        limit=limit,
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
    models: list[Model],
    state: State,
    timeout: tuple[float, float],
    *,
    limit: int | None = None,
    stop_on_error: bool = False,
    retry_count: int = 5,
    no_progress_bar: bool = False,
    error_counter: ErrorCounter | None = None,
    initial_page_data: dict | None = None,
) -> PushRows:
    if initial_page_data is None:
        initial_page_data = {}

    yield from _get_model_rows(
        context,
        models,
        state.metadata,
        limit=limit,
        stop_on_error=stop_on_error,
        no_progress_bar=no_progress_bar,
        error_counter=error_counter,
        initial_page_data=initial_page_data,
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
        counts = get_rows_with_errors_counts(models, context, state.metadata)
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


def _get_state_rows_with_page(
    context: Context,
    model_page: Page,
    table: sa.Table,
    size: int,
) -> sa.engine.LegacyCursorResult:
    conn = context.get("push.state.conn")
    model_page.size = size + 1
    order_by = []

    for by, page_by in model_page.by.items():
        if by.startswith("-"):
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
            stmt = sa.select([table]).where(from_cond).order_by(*order_by).limit(model_page.size)
            rows = conn.execute(stmt)
        else:
            stmt = sa.select([table]).order_by(*order_by).limit(model_page.size)
            rows = conn.execute(stmt)

        yield from get_paginated_values(model_page, page_meta, rows, extract_state_page_keys)


def _compare_data_with_state_rows(
    data_row: Any,
    state_row: Any,
    model_table: sa.Table,
):
    return data_row["_id"] == state_row[model_table.c["id"]]


def _compare_data_with_state_row_keys(data_row: Any, state_row: Any, model_table: sa.Table, page: Page):
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

    if "_page" in data_row:
        decoded = data_row["_page"]
        for i, (by, page_by) in enumerate(page.by.items()):
            state_val = state_row[model_table.c[f"page.{page_by.prop.name}"]]
            data_val = decoded[i]
            compare = True
            if state_val is not None and data_val is not None:
                if by.startswith("-"):
                    compare = state_val > data_val
                else:
                    compare = state_val < data_val
            if state_val != data_val:
                delete_cond = compare
                break
    else:
        delete_cond = True
    return delete_cond
