from __future__ import annotations

from typing import AsyncIterator

from sqlalchemy import exc

from spinta import commands, exceptions, spyna
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.redirect import remove_from_redirect
from spinta.backends.postgresql.sqlalchemy import utcnow
from spinta.commands import create_exception
from spinta.commands.write import push_stream, dataitem_from_payload
from spinta.components import Context, Model, DataItem, DataSubItem, Store, Property
from spinta.core.enums import Action
from spinta.core.ufuncs import asttoexpr, Expr
from spinta.types.datatype import Ref
from spinta.utils.aiotools import adrain
from spinta.utils.data import take
from spinta.utils.nestedstruct import flatten, flat_dicts_to_nested


@commands.insert.register(Context, Model, PostgreSQL)
async def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get("transaction")
    config = context.get("config")
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = config.max_error_count_on_insert
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False

    redirect_table = backend.get_table(model, TableType.REDIRECT)

    async for data in dstream:
        try:
            savepoint = connection.begin_nested()
            patch = commands.before_write(context, model, backend, data=data)
            # TODO: Refactor this to insert batches with single query.
            qry = table.insert().values(
                _id=patch["_id"],
                _revision=patch["_revision"],
                _txn=transaction.id,
                _created=utcnow(),
            )
            connection.execute(qry, patch)
            commands.after_write(context, model, backend, data=data)

            # On insert remove redirect entry if _id already exists
            remove_from_redirect(connection, redirect_table, patch["_id"])
            savepoint.commit()
        except exc.DatabaseError as error:
            rollback_full = True
            savepoint.rollback()
            exception = create_exception(data, error)
            error_list.append(exception)
            data.error = exception
            if len(error_list) >= max_error_count or stop_on_error:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)
    savepoint_transaction_start.commit()


@commands.update.register(Context, Model, PostgreSQL)
async def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    transaction = context.get("transaction")
    config = context.get("config")
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = config.max_error_count_on_insert
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        if not data.patch:
            yield data
            continue
        try:
            savepoint = connection.begin_nested()
            pk = data.saved["_id"]
            patch = commands.before_write(context, model, backend, data=data)
            result = connection.execute(
                table.update()
                .where(table.c._id == pk)
                .where(table.c._revision == data.saved["_revision"])
                .values(patch)
            )
            if result.rowcount == 0:
                raise Exception(f"Update failed, {model} with {pk} not found.")
            elif result.rowcount > 1:
                raise Exception(f"Update failed, {model} with {pk} has found and update {result.rowcount} rows.")
            commands.after_write(context, model, backend, data=data)
            savepoint.commit()
        except exc.DatabaseError as error:
            rollback_full = True
            savepoint.rollback()
            exception = create_exception(data, error)
            error_list.append(exception)
            data.error = exception

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)
    savepoint_transaction_start.commit()


@commands.delete.register(Context, Model, PostgreSQL)
async def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get("transaction")
    config = context.get("config")
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = config.max_error_count_on_insert
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        try:
            savepoint = connection.begin_nested()
            commands.before_write(context, model, backend, data=data)
            connection.execute(table.delete().where(table.c._id == data.saved["_id"]))
            commands.after_write(context, model, backend, data=data)
            savepoint.commit()
        except exc.DatabaseError as error:
            rollback_full = True
            savepoint.rollback()
            exception = create_exception(data, error)
            error_list.append(exception)
            data.error = exception

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)
    savepoint_transaction_start.commit()


def _get_data_action(data: DataSubItem | DataItem) -> Action:
    if isinstance(data, DataItem):
        return data.action

    return _get_data_action(data.root)


def _apply_action_parameters(patch: dict, action: Action):
    if action in (Action.INSERT, Action.UPSERT):
        patch["_created"] = utcnow()
    elif action in (Action.UPDATE, Action.PATCH, Action.MOVE):
        patch["_updated"] = utcnow()


@commands.before_write.register(Context, Model, PostgreSQL)
def before_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    patch = take(["_id"], data.patch)
    patch["_revision"] = take("_revision", data.patch, data.saved)
    patch["_txn"] = context.get("transaction").id
    _apply_action_parameters(patch, _get_data_action(data))
    for prop in take(model.properties).values():
        if not prop.dtype.inherited:
            prop_data = data[prop.name]
            value = commands.before_write(
                context,
                prop.dtype,
                backend,
                data=prop_data,
            )
            patch.update(value)
    return patch


@commands.after_write.register(Context, Model, PostgreSQL)
def after_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    for key in take(data.patch or {}):
        prop = model.properties[key]
        commands.after_write(context, prop.dtype, backend, data=data[key])


@commands.move.register(Context, Model, PostgreSQL)
async def move(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get("transaction")
    config = context.get("config")
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = config.max_error_count_on_insert
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False

    affected_models = list(_gather_affected_reference_model_properties(context, model))

    async for data in dstream:
        try:
            savepoint = connection.begin_nested()
            patch = commands.before_write(context, model, backend, data=data)
            pk = data.saved.get("_id")
            await update_affected_reference_values(
                context, backend, model, str(pk), str(patch.get("_id")), affected_models
            )
            connection.execute(table.delete().where(table.c._id == pk))

            commands.after_write(context, model, backend, data=data)
            savepoint.commit()
        except exc.DatabaseError as error:
            rollback_full = True
            savepoint.rollback()
            exception = create_exception(data, error)
            error_list.append(exception)
            data.error = exception

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)
    savepoint_transaction_start.commit()


def _gather_affected_reference_model_properties(context: Context, model: Model):
    store: Store = context.get("store")
    manifest = store.manifest

    for model_ in commands.get_models(context, manifest).values():
        props = []
        for prop in model_.flatprops.values():
            if (
                isinstance(prop.dtype, Ref)
                and commands.identifiable(prop)
                and prop.dtype.model.model_type() == model.model_type()
            ):
                props.append(prop)
        if props:
            yield {"model": model_, "properties": props}


async def update_affected_reference_values(
    context: Context, backend: PostgreSQL, model: Model, old_pk: str, pk_: str, affected_models: list[dict]
):
    for model_data in affected_models:
        model_ = model_data.get("model")
        props = model_data.get("properties", [])
        query = _build_select_query(props, old_pk)
        rows = commands.getall(context, model_, backend, query=query)
        rows = flatten(rows)
        dataitems = _build_dataitems(context, model_, props, rows, old_pk, pk_)
        dstream = push_stream(context, dataitems)
        await adrain(dstream)
    pass


async def _build_dataitems(
    context: Context, model: Model, props: list[Property], rows: list[dict], old_pk: str, new_pk: str
):
    for row in rows:
        patch = {"_where": f'eq(_id, "{row.get("_id")}")', "_op": Action.PATCH.value, "_revision": row.get("_revision")}
        for prop in props:
            prop_place = f"{prop.place}._id"
            if prop_place in row and row[prop_place] == old_pk:
                patch[prop_place] = new_pk
        yield dataitem_from_payload(context, model, flat_dicts_to_nested(patch))


def _build_select_query(props: list[Property], old_pk: str) -> Expr:
    result_str = ""
    for prop in props:
        query = f'{prop.place}._id.eq("{old_pk}")'

        if result_str:
            result_str = f"{result_str}|{query}"
        else:
            result_str = query
    return asttoexpr(spyna.parse(result_str))
