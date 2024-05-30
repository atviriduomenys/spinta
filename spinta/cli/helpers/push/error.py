from typing import Dict
from typing import Iterable
from typing import List

import requests
import sqlalchemy as sa
import tqdm

from spinta.cli.helpers.data import ModelRow
from spinta.cli.helpers.errors import ErrorCounter
from spinta.cli.helpers.push.utils import construct_where_condition_to_page, \
    construct_where_condition_from_page
from spinta.cli.helpers.push.write import prepare_rows_with_errors
from spinta.components import Context
from spinta.components import Model
from spinta.components import Page, get_page_size
from spinta.exceptions import InfiniteLoopWithPagination, TooShortPageSize


def get_rows_with_errors(
    client: requests.Session,
    server: str,
    models: List[Model],
    context: Context,
    metadata: sa.MetaData,
    counts: Dict[str, int],
    retry: int,
    no_progress_bar: bool = False,
    error_counter: ErrorCounter = None
):
    rows = _iter_rows_with_errors(
        client,
        server,
        models,
        context,
        metadata,
        counts,
        no_progress_bar,
        error_counter,
    )
    if not no_progress_bar:
        rows = tqdm.tqdm(rows, f'RETRY #{retry}', ascii=True, total=sum(counts.values()))
    for row in rows:
        yield row


def get_rows_with_errors_counts(
    models: List[Model],
    context: Context,
    metadata: sa.MetaData,
) -> dict:
    counts = {}
    conn = context.get('push.state.conn')
    for model in models:
        table = metadata.tables[model.name]
        required_condition = sa.and_(
            table.c.error.is_(True)
        )
        if model.page and model.page.is_enabled and model.page.by:
            where_cond = construct_where_condition_to_page(model.page, table)
        else:
            where_cond = None
        if where_cond is not None:
            where_cond = sa.and_(
                where_cond,
                required_condition
            )
        else:
            where_cond = required_condition

        row_count = conn.execute(
            sa.select(sa.func.count(table.c.id)).
            where(
                where_cond
            )
        )
        counts[model.name] = row_count.scalar()
    return counts


def _iter_rows_with_errors(
    client: requests.Session,
    server: str,
    models: List[Model],
    context: Context,
    metadata: sa.MetaData,
    counts: Dict[str, int],
    no_progress_bar: bool = False,
    error_counter: ErrorCounter = None,
) -> Iterable[ModelRow]:
    conn = context.get('push.state.conn')
    config = context.get('config')

    for model in models:
        size = get_page_size(config, model)
        table = metadata.tables[model.name]

        if model.page and model.page.is_enabled and model.page.by:
            rows = _get_error_rows_with_page(context, model.page, table, size)
        else:
            rows = conn.execute(
                sa.select([table.c.id, table.c.checksum, table.c.data]).
                where(
                    table.c.error.is_(True)
                )
            )
        if not no_progress_bar:
            rows = tqdm.tqdm(
                rows,
                model.name,
                ascii=True,
                total=counts.get(model.name),
                leave=False
            )

        yield from prepare_rows_with_errors(
            client,
            server,
            context,
            rows,
            model,
            table,
            error_counter
        )


def _get_error_rows_with_page(
    context: Context,
    model_page: Page,
    table: sa.Table,
    size: int
):
    conn = context.get('push.state.conn')
    order_by = []

    for page_by in model_page.by.values():
        order_by.append(sa.asc(table.c[f"page.{page_by.prop.name}"]))

    model_page.size = size + 1
    from_page = Page()
    from_page.size = size + 1
    from_page.update_values_from_page(model_page)
    from_page.clear()

    required_where_cond = sa.and_(
        table.c.error.is_(True)
    )
    last_value = None
    while True:
        finished = True
        from_cond = construct_where_condition_from_page(from_page, table)
        to_cond = construct_where_condition_to_page(model_page, table)

        where_cond = None
        if from_cond is not None:
            where_cond = from_cond
        if to_cond is not None:
            if where_cond is not None:
                sa.and_(
                    where_cond,
                    to_cond
                )
            else:
                where_cond = to_cond

        if where_cond is not None:
            where_cond = sa.and_(
                required_where_cond,
                where_cond
            )
        else:
            where_cond = required_where_cond

        rows = conn.execute(
            sa.select([table]).
            where(
                where_cond
            ).order_by(*order_by).limit(size)
        )
        first_value = None
        previous_value = None
        for i, row in enumerate(rows):
            if i > size - 1:
                if row == previous_value:
                    raise TooShortPageSize(
                        from_page,
                        page_size=size,
                        page_values=previous_value
                    )
                break
            previous_value = row
            if finished:
                finished = False

            if first_value is None:
                first_value = row
                if first_value == last_value:
                    raise InfiniteLoopWithPagination(
                        from_page,
                        page_size=size,
                        page_values=first_value
                    )
                else:
                    last_value = first_value

            for by, page_by in from_page.by.items():
                from_page.update_value(by, page_by.prop, row[f"page.{page_by.prop.name}"])
            yield row

        if finished:
            break

