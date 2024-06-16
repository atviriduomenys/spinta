import datetime
import itertools
import json
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Tuple

import sqlalchemy as sa

from spinta import spyna
from spinta.cli.helpers.push import prepare_data_for_push_state
from spinta.cli.helpers.push.components import PushRow, Saved
from spinta.cli.helpers.push.utils import get_data_checksum
from spinta.components import Context
from spinta.components import Model
from spinta.utils.json import fix_data_for_json
from spinta.utils.sqlite import migrate_table


def init_push_state(
    dburi: str,
    models: List[Model],
) -> Tuple[sa.engine.Engine, sa.MetaData]:
    engine = sa.create_engine(dburi)
    metadata = sa.MetaData(engine)
    inspector = sa.inspect(engine)

    page_table = sa.Table(
        '_page', metadata,
        sa.Column('model', sa.Text, primary_key=True),
        sa.Column('property', sa.Text),
        sa.Column('value', sa.Text),
    )
    page_table.create(checkfirst=True)

    types = {
        'string': sa.Text,
        'date': sa.Date,
        'datetime': sa.DateTime,
        'time': sa.Time,
        'integer': sa.Integer,
        'number': sa.Numeric
    }

    for model in models:
        pagination_cols = []
        if model.page and model.page.by and model.backend.paginated:
            for page_by in model.page.by.values():
                _type = types.get(page_by.prop.dtype.name, sa.Text)
                pagination_cols.append(
                    sa.Column(f"page.{page_by.prop.name}", _type, index=True)
                )

        table = sa.Table(
            model.name, metadata,
            sa.Column('id', sa.Unicode, primary_key=True),
            sa.Column('checksum', sa.Unicode),
            sa.Column('revision', sa.Unicode),
            sa.Column('pushed', sa.DateTime),
            sa.Column('error', sa.Boolean),
            sa.Column('data', sa.Text),
            *pagination_cols
        )
        migrate_table(engine, metadata, inspector, table, renames={
            'rev': 'checksum',
        })

    return engine, metadata


def reset_pushed(
    context: Context,
    models: List[Model],
    metadata: sa.MetaData,
):
    conn = context.get('push.state.conn')
    for model in models:
        table = metadata.tables[model.name]

        # reset pushed so we could see which objects were deleted
        conn.execute(
            table.update().
            values(pushed=None)
        )


def check_push_state(
    context: Context,
    rows: Iterable[PushRow],
    metadata: sa.MetaData
):
    conn = context.get('push.state.conn')

    for model_type, group in itertools.groupby(rows, key=_get_model_type):
        saved_rows = {}
        if model_type:
            table = metadata.tables[model_type]

            query = sa.select([table.c.id, table.c.revision, table.c.checksum])
            saved_rows = {
                state[table.c.id]: Saved(
                    state[table.c.revision],
                    state[table.c.checksum],
                )
                for state in conn.execute(query)
            }

        for row in group:
            if row.send and not row.error and row.op != "delete":
                _id = row.data['_id']
                row.checksum = get_data_checksum(row.data, row.model)
                saved = saved_rows.get(_id)
                if saved is None:
                    row.op = "insert"
                    row.saved = False
                else:
                    row.op = "patch"
                    row.saved = True
                    row.data['_revision'] = saved.revision
                    if saved.checksum == row.checksum:
                        conn.execute(
                            table.update().
                            where(table.c.id == _id).
                            values(
                                pushed=datetime.datetime.now()
                            )
                        )
                        # Skip if no changes, but only if not paginated, since pagination already skips those
                        if '_page' not in row.data:
                            continue

            yield row


def save_push_state(
    context: Context,
    rows: Iterable[PushRow],
    metadata: sa.MetaData,
) -> Iterator[PushRow]:
    conn = context.get('push.state.conn')
    for row in rows:
        table = metadata.tables[row.data['_type']]

        if row.model.page and row.model.page.by and '_page' in row.data:
            loaded = row.data['_page']
            page = {
                page_by.prop.name: loaded[i]
                for i, page_by in enumerate(row.model.page.by.values())
            }
            page = prepare_data_for_push_state(context, row.model, page)
            page = {f'page.{key}': value for key, value in page.items()}
            row.data.pop('_page')
        else:
            page = {}

        if row.error and row.op != "delete":
            data = fix_data_for_json(row.data)
            data = json.dumps(data)
        else:
            data = None

        if '_id' in row.data:
            _id = row.data['_id']
        else:
            _id = spyna.parse(row.data['_where'])['args'][1]

        if row.op == "delete" and not row.error:
            conn.execute(
                table.delete().
                where(table.c.id == _id)
            )
        elif row.saved:
            conn.execute(
                table.update().
                where(table.c.id == _id).
                values(
                    id=_id,
                    revision=row.data.get('_revision'),
                    checksum=row.checksum,
                    pushed=datetime.datetime.now(),
                    error=row.error,
                    data=data,
                    **page
                )
            )
        else:
            conn.execute(
                table.insert().
                values(
                    id=_id,
                    revision=row.data.get('_revision'),
                    checksum=row.checksum,
                    pushed=datetime.datetime.now(),
                    error=row.error,
                    data=data,
                    **page
                )
            )
        yield row


def save_page_values(
    context: Context,
    models: List[Model],
    metadata: sa.MetaData,
):
    conn = context.get('push.state.conn')
    page_table = metadata.tables['_page']

    for model in models:
        pagination_props = []
        saved = False
        if model.page and model.page.is_enabled and model.page.by:
            pagination_props = model.page.by.values()
            page_row = conn.execute(
                sa.select(page_table.c.model)
                .where(page_table.c.model == model.name)
            ).scalar()

            if page_row is not None:
                saved = True

        if pagination_props:
            value = {}
            for page_by in pagination_props:
                if page_by.value is not None:
                    value.update({
                        page_by.prop.name: page_by.value
                    })
            if value:
                value = fix_data_for_json(value)
                value = json.dumps(value)
                if saved:
                    conn.execute(
                        page_table.update().
                        where(
                            (page_table.c.model == model.name)
                        ).
                        values(
                            value=value
                        )
                    )
                else:
                    conn.execute(
                        page_table.insert().
                        values(
                            model=model.name,
                            property=','.join([page_by.prop.name for page_by in pagination_props]),
                            value=value
                        )
                    )


def _get_model_type(row: PushRow) -> str:
    return row.data['_type']
