from typing import AsyncIterator, cast

import psycopg2
from sqlalchemy import exc
from spinta import commands, exceptions
from spinta.types.datatype import Denorm
from spinta.utils.data import take
from spinta.components import Context, Model, DataItem, DataSubItem, Property
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.sqlalchemy import utcnow
from spinta.backends.postgresql.helpers.extractors import extract_error_property_name, extract_error_ref_id, \
    extract_error_model



@commands.insert.register(Context, Model, PostgreSQL)
async def insert(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = 100
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        try:
            savepoint = connection.begin_nested()
            patch = commands.before_write(context, model, backend, data=data)
            # TODO: Refactor this to insert batches with single query.
            qry = table.insert().values(
                _id=patch['_id'],
                _revision=patch['_revision'],
                _txn=transaction.id,
                _created=utcnow(),
            )
            connection.execute(qry, patch)
            commands.after_write(context, model, backend, data=data)
        except exc.IntegrityError as error:
            rollback_full = True
            savepoint.rollback()
            if isinstance(error.orig, psycopg2.errors.ForeignKeyViolation):
                error_message = error.orig.diag.message_detail
                error_property_name = extract_error_property_name(error_message)
                error_ref_id = extract_error_ref_id(error_message)
                error_property = model.properties.get(error_property_name)
                exception = exceptions.ReferencedObjectNotFound(error_property, id=error_ref_id)
                error_list.append(exception)
                data.error = exception
            else:
                # Might need to append it to error_list, but these errors are not part of BaseError class
                raise error

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)


@commands.update.register(Context, Model, PostgreSQL)
async def update(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: dict,
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = 100
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        if not data.patch:
            yield data
            continue

        try:
            savepoint = connection.begin_nested()
            pk = data.saved['_id']
            patch = commands.before_write(context, model, backend, data=data)
            result = connection.execute(
                table.update().
                where(table.c._id == pk).
                where(table.c._revision == data.saved['_revision']).
                values(patch)
            )
            if result.rowcount == 0:
                raise Exception(f"Update failed, {model} with {pk} not found.")
            elif result.rowcount > 1:
                raise Exception(
                    f"Update failed, {model} with {pk} has found and update "
                    f"{result.rowcount} rows."
                )
            commands.after_write(context, model, backend, data=data)
        except exc.IntegrityError as error:
            rollback_full = True
            savepoint.rollback()
            if isinstance(error.orig, psycopg2.errors.ForeignKeyViolation):
                error_message = error.orig.diag.message_detail
                error_property_name = extract_error_property_name(error_message)
                error_ref_id = extract_error_ref_id(error_message)
                error_property = model.properties.get(error_property_name)
                exception = exceptions.ReferencedObjectNotFound(error_property, id=error_ref_id)
                error_list.append(exception)
                data.error = exception
            else:
                # Might need to append it to error_list, but these errors are not part of BaseError class
                raise error

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)


@commands.delete.register(Context, Model, PostgreSQL)
async def delete(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(model)

    # Need to set specific amount of max errors, to prevent memory problems
    max_error_count = 100
    error_list = []
    savepoint_transaction_start = connection.begin_nested()
    rollback_full = False
    async for data in dstream:
        try:
            savepoint = connection.begin_nested()
            commands.before_write(context, model, backend, data=data)
            connection.execute(
                table.delete().
                where(table.c._id == data.saved['_id'])
            )
            commands.after_write(context, model, backend, data=data)
        except exc.IntegrityError as error:
            rollback_full = True
            savepoint.rollback()
            if isinstance(error.orig, psycopg2.errors.ForeignKeyViolation):
                error_message = error.orig.diag.message_detail
                error_model = extract_error_model(error_message)
                exception = exceptions.ReferringObjectFound(model.properties.get("_id"), model=error_model, id=data.saved.get("_id"))
                error_list.append(exception)
                data.error = exception
            else:
                # Might need to append it to error_list, but these errors are not part of BaseError class
                raise error

            if len(error_list) >= max_error_count:
                yield data
                savepoint_transaction_start.rollback()
                raise exceptions.MultipleErrors(error_list)
        yield data

    if rollback_full:
        savepoint_transaction_start.rollback()
        raise exceptions.MultipleErrors(error_list)


@commands.before_write.register(Context, Model, PostgreSQL)
def before_write(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    data: DataSubItem,
) -> dict:
    patch = take(['_id'], data.patch)
    patch['_revision'] = take('_revision', data.patch, data.saved)
    patch['_txn'] = context.get('transaction').id
    patch['_created'] = utcnow()
    for prop in take(model.properties).values():
        if isinstance(prop.dtype, Denorm):
            prop_data = data[prop.name.split('.')[0]]
        else:
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
