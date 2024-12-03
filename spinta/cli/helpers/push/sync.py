import datetime
from typing import List, Any

import sqlalchemy as sa
import tqdm
from typer import echo

from spinta import commands
from spinta.auth import authorized
from spinta.backends.constants import BackendFeatures
from spinta.cli.helpers.errors import ErrorCounter
from spinta.cli.helpers.push import prepare_data_for_push_state
from spinta.cli.helpers.push.utils import extract_state_page_id_key, construct_where_condition_from_page
from spinta.commands.read import PaginationMetaData, get_paginated_values
from spinta.components import Context, Action, Config
from spinta.components import Model
from spinta.components import Page, get_page_size, Property, PageBy
from spinta.utils.response import get_request


def _build_push_state_sync_url(
    server: str,
    model: str,
    page: str,
    page_columns: List[str],
    limit: int
):
    base = f'{server}/{model}/:format/json?'

    required = [
        '_id',
        '_revision',
        '_page',
        'checksum()'
    ]

    base = f'{base}select({",".join(required)}'
    for page_column in page_columns:
        base = f'{base},{page_column}'
    base = f'{base})&sort(_id)'

    if page:
        base = f'{base}&page(\'{page}\')'

    if limit:
        base = f'{base}&limit({limit})'

    return base


def _fetch_all_model_data(
    config: Config,
    model: Model,
    client,
    server: str,
    error_counter: ErrorCounter,
    timeout: tuple[float, float],
    initial_page_data: Any = None
):
    limit = config.sync_page_size
    page_hash = ''
    page_columns = []
    page = commands.create_page(model.page, initial_page_data)
    if model.backend.supports(BackendFeatures.PAGINATION) and page.by and page.enabled:
        for page_by in page.by.values():
            page_columns.append(page_by.prop.name)
    while True:
        url = _build_push_state_sync_url(
            server=server,
            model=model.model_type(),
            page=page_hash,
            page_columns=page_columns,
            limit=limit
        )
        status_code, resp = get_request(
            client,
            url,
            error_counter=error_counter,
            timeout=timeout
        )
        if status_code == 200:
            data = resp['_data']
            if not data:
                break

            if '_page' in resp:
                page_hash = resp['_page']['next']

            for row in data:
                yield row

        else:
            break


def _get_state_rows_with_id(
    context: Context,
    table: sa.Table,
    size: int
) -> sa.engine.LegacyCursorResult:
    conn = context.get('push.state.conn')

    model_page = Page()
    model_page.size = size + 1
    prop = Property()
    prop.name = 'id'
    model_page.by = {
        'id': PageBy(prop)
    }
    order_by = sa.sql.expression.nullslast(sa.asc(table.c['id']))

    page_meta = PaginationMetaData(
        page_size=size,
    )

    while not page_meta.is_finished:
        page_meta.is_finished = True
        from_cond = construct_where_condition_from_page(model_page, table, prefix='')

        if from_cond is not None:
            stmt = sa.select([table]).where(
                from_cond
            ).order_by(order_by).limit(model_page.size)
            rows = conn.execute(
                stmt
            )
        else:
            stmt = sa.select([table]).order_by(order_by).limit(model_page.size)
            rows = conn.execute(
                stmt
            )

        yield from get_paginated_values(model_page, page_meta, rows, extract_state_page_id_key)


def _delete_row_from_push_state(
    conn: sa.engine.Connection,
    table: sa.Table,
    id_: str
):
    conn.execute(
        table.delete().
        where(table.c.id == id_)
    )


def _update_row_from_push_state(
    context: Context,
    model: Model,
    conn: sa.engine.Connection,
    table: sa.Table,
    id_: str,
    target_row: dict,
    state_row: dict
):
    target_checksum = target_row.get('checksum()')
    state_checksum = getattr(state_row, 'checksum')

    # Check if target db and pushed data is the same

    page_keys = []
    reserved_keys = [
        '_id',
        '_revision',
        'checksum()'
    ]

    page_mapping = {}
    for key in target_row.keys():
        if key not in reserved_keys:
            page_keys.append(key)

    skip_update = False
    if target_checksum == state_checksum:
        skip_update = True
        # Check if pushed state did not have errors while pushing already existing data
        if getattr(state_row, 'error') or getattr(state_row, 'data'):
            skip_update = False

        # Check if revisions match, they need match in order to do proper updates
        if getattr(state_row, 'revision') != target_row['_revision']:
            skip_update = False

        # Check if page key values match (could be cases when migrating from old to new versions, or changing page keys)
        if skip_update:
            for key in page_keys:
                if target_row[key] != getattr(state_row, f'page.{key}'):
                    skip_update = False
                    break

    # Skip update if nothing has changed
    if skip_update:
        return

    if page_keys:
        page_mapping = {key: target_row[key] for key in page_keys}
        page_mapping = prepare_data_for_push_state(context, model, page_mapping)
        page_mapping = {f'page.{key}': value for key, value in page_mapping.items()}

    conn.execute(
        table.update().
        where(table.c.id == id_).values(
            {
                'revision': target_row.get('_revision'),
                'checksum': target_checksum,
                'pushed': datetime.datetime.now(),
                'error': False,
                'data': None,
                **page_mapping
            }
        )
    )


def _insert_row_to_push_state(
    context: Context,
    model: Model,
    conn: sa.engine.Connection,
    table: sa.Table,
    target_row: dict
):
    page_keys = []
    reserved_keys = [
        '_id',
        '_revision',
        'checksum()'
    ]

    page_mapping = {}
    for key in target_row.keys():
        if key not in reserved_keys:
            page_keys.append(key)

    if page_keys:
        page_mapping = {key: target_row[key] for key in page_keys}
        page_mapping = prepare_data_for_push_state(context, model, page_mapping)
        page_mapping = {f'page.{key}': value for key, value in page_mapping.items()}

    conn.execute(
        table.insert().
        values(
            {
                'id': target_row['_id'],
                'revision': target_row.get('_revision'),
                'checksum': target_row.get('checksum()'),
                'pushed': datetime.datetime.now(),
                'error': False,
                'data': None,
                **page_mapping
            }
        )
    )


def sync_push_state(
    context: Context,
    client,
    server: str,
    models: List[Model],
    error_counter: ErrorCounter,
    no_progress_bar: bool,
    metadata: sa.MetaData,
    timeout: tuple[float, float]
):
    config = context.get('config')
    conn = context.get('push.state.conn')
    counters = {}
    if not no_progress_bar:
        counters = {
            '_total': tqdm.tqdm(desc='SYNCHRONIZING PUSH STATE', ascii=True)
        }
        echo()

    for model in models:
        model_name = model.model_type()
        primary_keys = model.external.pkeys
        size = get_page_size(config, model)
        skip_model = False
        # Check permissions
        for key in primary_keys:
            is_authorized = authorized(context, key, action=Action.SEARCH)
            if not is_authorized:
                echo(f"SKIPPED PUSH STATE '{model.model_type()}' MODEL SYNC, NO PERMISSION.")
                skip_model = True
                break

        if skip_model:
            continue

        if not no_progress_bar:
            counters[model_name] = tqdm.tqdm(desc=model_name, ascii=True)

        model_table = metadata.tables[model.name]
        target_data = _fetch_all_model_data(
            config=config,
            model=model,
            client=client,
            server=server,
            error_counter=error_counter,
            timeout=timeout
        )
        state_data = _get_state_rows_with_id(
            context=context,
            table=model_table,
            size=size
        )
        target_row = next(target_data, None)
        state_row = next(state_data, None)
        while target_row is not None or state_row is not None:
            target_id = target_row['_id'] if target_row is not None else None
            state_id = state_row['id'] if state_row is not None else None

            if not no_progress_bar:
                counters['_total'].update(1)
                counters[model_name].update(1)

            if target_row is None:
                _delete_row_from_push_state(conn, model_table, state_id)
                state_row = next(state_data, None)
                continue

            if state_row is None:
                _insert_row_to_push_state(context, model, conn, model_table, target_row)
                target_row = next(target_data, None)
                continue

            if target_id == state_id:
                _update_row_from_push_state(context, model, conn, model_table, state_id, target_row, state_row)
                target_row = next(target_data, None)
                state_row = next(state_data, None)
            elif target_id < state_id:
                _insert_row_to_push_state(context, model, conn, model_table, target_row)
                target_row = next(target_data, None)
            else:
                _delete_row_from_push_state(conn, model_table, state_id)
                state_row = next(state_data, None)

        if model_name in counters:
            counters[model_name].close()

    if '_total' in counters:
        counters['_total'].close()
