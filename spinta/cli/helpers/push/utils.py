import hashlib
import json
from typing import Any
from typing import List
from typing import Union

import msgpack
import sqlalchemy as sa
from spinta import commands

from sqlalchemy.engine.row import Row
from spinta.auth import authorized
from spinta.cli.helpers.push.components import PushRow
from spinta.components import Action, Page, pagination_enabled
from spinta.components import Context
from spinta.components import Model
from spinta.types.datatype import Ref
from spinta.utils.data import take
from spinta.utils.json import fix_data_for_json
from spinta.utils.nestedstruct import flatten, sepgetter


def get_model(row: PushRow) -> Model:
    return row.model


def get_data_checksum(data: dict, model: Model = None):
    data = fix_data_for_json(take(data))
    data = flatten([data], sepgetter(model))
    data = [[k, v] for x in data for k, v in sorted(x.items())]
    data = msgpack.dumps(data, strict_types=True)
    checksum = hashlib.sha1(data).hexdigest()
    return checksum


def extract_state_page_keys(row: Union[dict, Row]):
    result = []

    if isinstance(row, Row):
        row = row._mapping

    for key, value in row.items():
        if 'page.' in key:
            result.append(value)
    return result


def extract_state_page_id_key(row: Union[dict, Row]):
    result = []

    if isinstance(row, Row):
        row = row._mapping

    for key, value in row.items():
        if key == 'id':
            result.append(value)
            break
    return result


def extract_dependant_nodes(context: Context, models: List[Model], filter_pushed: bool):
    extracted_models = [] if filter_pushed else models.copy()
    for model in models:
        if model.base:
            if commands.identifiable(model.base):
                if model.base.parent not in extracted_models:
                    if filter_pushed and not (model.base.parent in models):
                        extracted_models.append(model.base.parent)
                    elif not filter_pushed:
                        extracted_models.append(model.base.parent)

        for prop in model.properties.values():
            if isinstance(prop.dtype, Ref):
                if authorized(context, prop, action=Action.SEARCH):
                    if commands.identifiable(prop):
                        if prop.dtype.model not in extracted_models:
                            if filter_pushed and not (prop.dtype.model in models):
                                extracted_models.append(prop.dtype.model)
                            elif not filter_pushed:
                                extracted_models.append(prop.dtype.model)

    return extracted_models


def load_initial_page_data(
    context: Context,
    metadata: sa.MetaData,
    models: List[Model],
    incremental: bool,
    override_page: dict
) -> dict:
    if not incremental:
        return {}

    conn = context.get('push.state.conn')
    table = metadata.tables['_page']
    result = {}

    for model in models:
        model_name = model.model_type()
        if pagination_enabled(model):
            if model_name in override_page:
                result[model_name] = override_page[model_name]
                continue

            page = conn.execute(
                sa.select([table.c.value]).
                where(
                    table.c.model == model.name
                )
            ).scalar()
            values = json.loads(page) if page else {}
            result[model_name] = values

    return result


def update_model_page_with_new(
    model_page: Page,
    model_table: sa.Table,
    state_row: Any = None,
    data_row: Any = None,
):
    if state_row:
        for by, page_by in model_page.by.items():
            val = state_row[model_table.c[f"page.{page_by.prop.name}"]]
            model_page.update_value(by, page_by.prop, val)
    elif data_row:
        model_page.update_values_from_list(data_row['_page'])


def construct_where_condition_from_page(
    page: Page,
    table: sa.Table,
    prefix: str = 'page.'
):
    item_count = len(page.by.keys())
    where_list = []
    for i in range(item_count):
        where_list.append([])
    if not page.all_none():
        for i, (by, page_by) in enumerate(page.by.items()):
            for n in range(item_count):
                if n >= i:
                    if n == i:
                        if page_by.value is not None:
                            if by.startswith('-'):
                                where_list[n].append(table.c[f"{prefix}{page_by.prop.name}"] < page_by.value)
                            else:
                                where_list[n].append(
                                    sa.or_(
                                        table.c[f"{prefix}{page_by.prop.name}"] > page_by.value,
                                        table.c[f"{prefix}{page_by.prop.name}"] == None
                                    )
                                )
                        else:
                            if by.startswith('-'):
                                where_list[n].append(table.c[f"{prefix}{page_by.prop.name}"] != page_by.value)
                    else:
                        where_list[n].append(table.c[f"{prefix}{page_by.prop.name}"] == page_by.value)

    where_condition = None

    remove_list = []
    for i, (by, value) in enumerate(page.by.items()):
        if value.value is None and not by.startswith('-'):
            remove_list.append(where_list[i])
    for item in remove_list:
        where_list.remove(item)

    for where in where_list:
        condition = None
        for item in where:
            if condition is not None:
                condition = sa.and_(
                    condition,
                    item
                )
            else:
                condition = item
        if where_condition is not None:
            where_condition = sa.or_(
                where_condition,
                condition
            )
        else:
            where_condition = condition

    return where_condition
