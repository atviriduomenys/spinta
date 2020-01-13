from typing import AsyncIterator, Union, Optional

import itertools
import json
import pyrql

from authlib.oauth2.rfc6750.errors import InsufficientScopeError

from starlette.requests import Request
from starlette.responses import Response

from spinta import commands
from spinta import exceptions
from spinta.auth import check_scope
from spinta.backends import Backend
from spinta.components import Context, Node, UrlParams, Action, DataItem, Namespace, Model, Property, DataStream
from spinta.renderer import render
from spinta.types import dataset
from spinta.types.datatype import DataType, Object, Array, File, Ref
from spinta.urlparams import get_model_by_name
from spinta.utils.aiotools import agroupby
from spinta.utils.aiotools import aslice, alist
from spinta.utils.errors import report_error
from spinta.utils.streams import splitlines
from spinta.utils.schema import NotAvailable, NA, strip_metadata
from spinta.utils.time import rfc822_now
from spinta.types.namespace import traverse_ns_models


STREAMING_CONTENT_TYPES = [
    'application/x-jsonlines',
    'application/x-ndjson',
]


@commands.push.register()
async def push(
    context: Context,
    request: Request,
    scope: Node,
    backend: (type(None), Backend),
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    stop_on_error = not params.fault_tolerant
    if is_streaming_request(request):
        stream = _read_request_stream(context, request, scope, stop_on_error)
    else:
        stream = _read_request_body(
            context, request, scope, action, params, stop_on_error,
        )
    dstream = push_stream(context, stream, stop_on_error)
    batch = False
    if params.summary:
        status_code, response = await _summary_response(context, dstream)
    elif await is_batch(request, scope):
        batch = True
        status_code, response = await _batch_response(context, dstream)
    else:
        status_code, response = await simple_response(context, dstream)
    headers = prepare_headers(context, scope, response, action, is_batch=batch)
    return render(context, request, scope, params, response,
                  action=action, status_code=status_code, headers=headers)


async def push_stream(
    context: Context,
    stream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
) -> AsyncIterator[DataItem]:

    cmds = {
        Action.INSERT: commands.insert,
        Action.UPSERT: commands.upsert,
        Action.UPDATE: commands.update,
        Action.PATCH: commands.patch,
        Action.DELETE: commands.delete,
    }

    async for (model, prop, backend, action), dstream in agroupby(stream, key=_stream_group_key):
        if model is None or action is None:
            async for data in dstream:
                yield data
            continue

        if action not in cmds:
            raise exceptions.UnknownAction(
                prop or model,
                action=action,
                supported_actions=sorted(x.value for x in cmds),
            )

        commands.authorize(context, action, prop or model)
        dstream = prepare_data(context, dstream)
        dstream = read_existing_data(context, dstream)
        dstream = validate_data(context, dstream)
        dstream = prepare_patch(context, dstream)
        if prop:
            dstream = cmds[action](
                context, prop, prop.dtype, prop.backend, dstream=dstream,
                stop_on_error=stop_on_error,
            )
        else:
            dstream = cmds[action](
                context, model, model.backend, dstream=dstream,
                stop_on_error=stop_on_error,
            )
        dstream = commands.create_changelog_entry(
            context, model, model.backend, dstream=dstream,
        )
        async for data in dstream:
            yield data


def _stream_group_key(data: DataItem):
    return data.model, data.prop, data.backend, data.action


def is_streaming_request(request: Request):
    content_type = request.headers.get('content-type')
    return content_type in STREAMING_CONTENT_TYPES


async def is_batch(request: Request, node: Node):
    if is_streaming_request(request):
        return True

    ct = request.headers.get('content-type')
    if ct == 'application/json':
        try:
            payload = await request.json()
        except json.decoder.JSONDecodeError as e:
            raise exceptions.JSONError(node, error=str(e))
        else:
            return '_data' in payload

    return False


async def _read_request_body(
    context: Context,
    request: Request,
    scope: Node,
    action: Action,
    params: UrlParams,
    stop_on_error: bool = True,
) -> AsyncIterator[DataItem]:

    if isinstance(scope, (Property, dataset.Property)):
        model = scope.model
        prop = scope
        propref = params.propref
    else:
        model = scope
        prop = None
        propref = False

    if prop and not propref:
        backend = prop.backend
    else:
        backend = model.backend

    if action == Action.DELETE:
        payload = _add_where(params, {})
        yield DataItem(model, prop, propref, backend, action, payload)
        return

    ct = request.headers.get('content-type')
    if ct != 'application/json':
        raise exceptions.UnknownContentType(
            scope,
            content_type=ct,
            supported_content_types=['application/json'],
        )

    try:
        payload = await request.json()
    except json.decoder.JSONDecodeError as e:
        raise exceptions.JSONError(scope, error=str(e))

    if '_data' in payload:
        for data in payload['_data']:
            # TODO: Handler propref in batch case.
            yield dataitem_from_payload(context, scope, data, stop_on_error)
    else:
        payload = _add_where(params, payload)
        # TODO: payload `_type` should be validated to match with `scope` or
        #       `node` given in URL.

        if '_op' in payload:
            action = _action_from_op(scope, payload, stop_on_error)
            if isinstance(action, exceptions.UserError):
                yield DataItem(model, prop, propref, backend, payload=payload, error=action)

        yield DataItem(model, prop, propref, backend, action, payload)


def _add_where(params: UrlParams, payload: dict):
    if '_where' in payload:
        return {
            **payload,
            '_where': pyrql.parse(payload['_where']),
        }
    elif params.pk:
        return {
            **payload,
            '_where': {'name': 'eq', 'args': ['_id', params.pk]},
        }
    else:
        return payload


async def _read_request_stream(
    context: Context,
    request: Request,
    scope: Node,
    stop_on_error: bool = True,
) -> AsyncIterator[DataItem]:
    transaction = context.get('transaction')
    async for line in splitlines(request.stream()):
        try:
            payload = json.loads(line.strip())
        except json.decoder.JSONDecodeError as e:
            error = exceptions.JSONError(scope, error=str(e), transaction=transaction.id)
            report_error(error, stop_on_error)
            yield DataItem(error=error)
            continue
        yield dataitem_from_payload(context, scope, payload, stop_on_error)


def dataitem_from_payload(
    context: Context,
    scope: Node,
    payload: dict,
    stop_on_error: bool = True,
) -> DataItem:
    transaction = context.get('transaction')

    # TODO: We need a proper data validation functions, something like that:
    #
    #           validate(payload, {
    #               'type': 'object',
    #               'properties': {
    #                   '_op': {
    #                       'type': 'string',
    #                       'cast': str_to_action,
    #                   }
    #               }
    #           })
    if not isinstance(payload, dict):
        error = exceptions.InvalidValue(scope, transaction=transaction.id)
        report_error(error, stop_on_error)
        return DataItem(error=error)

    if '_type' not in payload:
        error = exceptions.MissingRequiredProperty(scope, prop='_type')
        return DataItem(payload=payload, error=error)

    model = payload['_type']
    if '.' in model:
        model, prop = model.split('.', 1)
        if prop.endswith(':ref'):
            prop = prop[:-len(':ref')]
            propref = True
        else:
            propref = False
    else:
        prop = None
        propref = False

    try:
        model = get_model_by_name(context, scope.manifest, model)
    except exceptions.UserError as error:
        report_error(error, stop_on_error)
        return DataItem(model, payload=payload, error=error)

    if model and prop:
        if prop in model.flatprops:
            prop = model.flatprops[prop]
        else:
            error = exceptions.FieldNotInResource(model, property=prop)
            report_error(error, stop_on_error)
            return DataItem(model, payload=payload, error=error)

    if prop and not propref:
        backend = prop.backend
    else:
        backend = model.backend

    if not commands.in_namespace(model, scope):
        error = exceptions.OutOfScope(model, scope=scope)
        report_error(error, stop_on_error)
        return DataItem(model, prop, propref, backend, payload=payload, error=error)

    if '_op' not in payload:
        error = exceptions.MissingRequiredProperty(scope, prop='_op')
        return DataItem(payload=payload, error=error)

    action = _action_from_op(scope, payload, stop_on_error)
    if isinstance(action, exceptions.UserError):
        return DataItem(model, prop, propref, backend, payload=payload, error=action)

    if '_where' in payload:
        payload['_where'] = pyrql.parse(payload['_where'])

    return DataItem(model, prop, propref, backend, action, payload)


def _action_from_op(
    scope: Node,
    payload: dict,
    stop_on_error: bool = True,
) -> Union[Action, exceptions.UserError]:
    action = payload.get('_op')
    if not Action.has_value(action):
        error = exceptions.UnknownAction(
            scope,
            action=action,
            supported_actions=Action.values(),
        )
        report_error(error, stop_on_error)
        return error
    return Action.by_value(action)


async def prepare_data(
    context: Context,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
) -> AsyncIterator[DataItem]:
    async for data in dstream:
        if data.error is not None:
            yield data
            continue
        try:
            data.payload = commands.rename_metadata(context, data.payload)
            if data.prop:
                data.given = commands.load(context, data.prop, data.payload)
                data.given = commands.prepare(context, data.prop, data.given, action=data.action)
                commands.simple_data_check(context, data, data.prop, data.model.backend)
            else:
                data.given = commands.load(context, data.model, data.payload)
                data.given = commands.prepare(context, data.model, data.given, action=data.action)
                commands.simple_data_check(context, data, data.model, data.model.backend)
                data.given = commands.prepare_given_ufunc(context, data.model, data.given, action=data.action)
        except (exceptions.UserError, InsufficientScopeError) as error:
            report_error(error, stop_on_error)
        yield data


async def read_existing_data(
    context: Context,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
) -> AsyncIterator[DataItem]:
    async for data in dstream:
        # On insert there are no existing data.
        if data.action == Action.INSERT:
            # XXX: Maybe read exisint data if model has unique constraint?
            yield data
            continue

        try:
            if data.prop:
                rows = [commands.getone(
                    context,
                    data.prop,
                    data.prop.dtype,
                    data.backend,
                    id_=data.given['_where']['args'][1],
                )]
            else:
                rows = [commands.getone(
                    context,
                    data.model,
                    data.model.backend,
                    id_=data.given['_where']['args'][1],
                )]
        except exceptions.ItemDoesNotExist:
            rows = []
        # FIXME: Above is a temporary hack, because PostgreSQL backend's getall
        #        does not support filtering.
        # rows = commands.getall(
        #     context,
        #     data.model,
        #     data.model.backend,
        #     query=[data.given['_where']],
        # )

        # When updating by id only, there must be exactly one existing record.
        if data.action == Action.UPSERT or _has_id_in_where(data.given):
            rows = list(itertools.islice(rows, 2))
            if len(rows) == 1:
                data.saved = rows[0]
                data.saved['_type'] = data.model.model_type()
            elif len(rows) > 1:
                data.error = exceptions.MultipleRowsFound(data.model, _id=rows[0]['_id'])
                report_error(data.error, stop_on_error)
            elif data.action != Action.UPSERT:
                data.error = exceptions.ItemDoesNotExist(data.model, id=data.given['_where']['args'][1])
                report_error(data.error, stop_on_error)
            yield data
            continue

        # In other case, update multiple rows, and get multiple exising data.
        for row in rows:
            data = data.copy()
            data.saved = row
            data.saved['_type'] = data.model.model_type()
            yield data


def _has_id_in_where(given: dict):
    return (
        '_where' in given and
        given['_where']['name'] == 'eq' and
        given['_where']['args'][0] == '_id'
    )


def _generate_patch(old: dict, new: dict):
    patch = {}
    for k, v in new.items():
        if old.get(k) != v:
            patch[k] = v
    return patch


async def validate_data(
    context: Context,
    dstream: AsyncIterator[DataItem],
) -> AsyncIterator[DataItem]:
    async for data in dstream:
        if data.error is None:
            if '_id' in data.given and data.prop is None:
                check_scope(context, 'set_meta_fields')
            if data.action == Action.INSERT:
                if '_revision' in data.given:
                    raise exceptions.ManagedProperty(data.model, property='_revision')
            if data.prop:
                commands.complex_data_check(
                    context,
                    data,
                    data.prop.dtype,
                    data.prop,
                    data.backend,
                    data.given.get(data.prop.name),
                )
            else:
                commands.complex_data_check(
                    context,
                    data,
                    data.model,
                    data.model.backend,
                )
        yield data


async def prepare_patch(
    context: Context,
    dstream: AsyncIterator[DataItem],
) -> AsyncIterator[DataItem]:
    async for data in dstream:
        data.patch = build_data_patch_for_write(
            context,
            data.prop or data.model,
            given=strip_metadata(data.given),
            saved=strip_metadata(data.saved) if data.saved else data.saved,
            fill=data.action in (Action.INSERT, Action.UPDATE),
        )

        if data.patch is NA:
            data.patch = {}

        if '_id' in data.given and (data.saved is None or data.given['_id'] != data.saved['_id']):
            data.patch['_id'] = data.given['_id']
        elif data.action == Action.INSERT:
            data.patch['_id'] = commands.gen_object_id(context, data.model.backend, data.model)
        if data.patch:
            data.patch['_revision'] = commands.gen_object_id(context, data.model.backend, data.model)

        yield data


@commands.build_data_patch_for_write.register()
def build_data_patch_for_write(
    context: Context,
    model: (Model, dataset.Model),
    *,
    given: dict,
    saved: dict,
    fill: bool = False,
) -> dict:
    if fill:
        props = (
            prop
            for prop in model.properties.values()
            if not prop.name.startswith('_')
        )
    else:
        props = (model.properties[k] for k in given)

    patch = {}
    for prop in props:
        value = build_data_patch_for_write(
            context,
            prop.dtype,
            given=given.get(prop.name, NA),
            saved=saved.get(prop.name, NA) if saved else saved,
            fill=fill,
        )
        if value is not NA:
            patch[prop.name] = value
    return patch


@commands.build_data_patch_for_write.register()  # noqa
def build_data_patch_for_write(  # noqa
    context: Context,
    prop: (Property, dataset.Property),
    *,
    given: dict,
    saved: dict,
    fill: bool = False,
) -> dict:
    value = build_data_patch_for_write(
        context,
        prop.dtype,
        given=given.get(prop.name, NA),
        saved=saved.get(prop.name, NA) if saved else saved,
        fill=fill,
    )
    if value is not NA:
        return {prop.name: value}
    else:
        return {}


@commands.build_data_patch_for_write.register()  # noqa
def build_data_patch_for_write(  # noqa
    context: Context,
    dtype: Object,
    *,
    given: Optional[dict],
    saved: Optional[dict],
    fill: bool = False,
) -> Union[dict, NotAvailable]:
    if fill:
        props = (
            prop
            for prop in dtype.properties.values()
            if not prop.name.startswith('_')
        )
    else:
        props = (dtype.properties[k] for k in given)

    patch = {}
    for prop in props:
        value = build_data_patch_for_write(
            context,
            prop.dtype,
            given=given.get(prop.name, NA) if given else NA,
            saved=saved.get(prop.name, NA) if saved else NA,
            fill=fill,
        )
        if value is not NA:
            patch[prop.name] = value
    return patch or NA


@commands.build_data_patch_for_write.register()  # noqa
def build_data_patch_for_write(  # noqa
    context: Context,
    dtype: Array,
    *,
    given: Optional[object],
    saved: Optional[object],
    fill: bool = False,
) -> Union[dict, NotAvailable]:
    if given is NA and not fill:
        return NA
    if given is NA:
        return saved or []
    if given is None:
        # XXX: not sure if arrays can be None?
        return None
    patch = [
        build_data_patch_for_write(
            context,
            dtype.items.dtype,
            given=value,
            # We can't deterministically compare arrays, so we always overwrite
            # array content, by pretending, that nothing is saved previously and
            # we must fill all missing values with defaults.
            saved=NA,
            fill=True,
        )
        for value in given
    ]

    # Even if we always overwrite arrays, but in the end, we still check if
    # whole array has changed or not.
    if saved == patch:
        return NA
    else:
        return patch



@commands.build_data_patch_for_write.register()  # noqa
def build_data_patch_for_write(  # noqa
    context: Context,
    dtype: DataType,
    *,
    given: Optional[object],
    saved: Optional[object],
    fill: bool = False,
) -> Union[dict, NotAvailable]:
    if given is NA:
        if fill:
            given = dtype.prop.default
        else:
            return NA
    if given != saved:
        return given
    else:
        return NA


def prepare_response(
    context: Context,
    data: DataItem,
) -> (DataItem, dict):
    if data.action == Action.UPDATE:
        # Minor optimisation: if we querying subresource, then build
        # response only for the subresource tree, do not walk through
        # whole model property tree.
        if data.prop:
            dtype = data.prop.dtype
            patch = strip_metadata(data.patch.get(data.prop.name, {}))
            saved = strip_metadata(data.saved.get(data.prop.name, {}))
        else:
            dtype = data.model
            patch = strip_metadata(data.patch)
            saved = strip_metadata(data.saved)

        resp = build_full_response(
            context,
            dtype,
            patch=patch,
            saved=saved,
        )

        # When querying subresources, response still must be enclosed with
        # the subresource key.
        if data.prop:
            resp = {
                data.prop.name: resp,
            }
    elif data.patch:
        resp = data.patch
    else:
        resp = {}
    return resp


@commands.build_full_response.register()
def build_full_response(
    context: Context,
    dtype: (Object, Model, dataset.Model),
    *,
    patch: Optional[object],
    saved: Optional[object],
):
    full_patch = {}
    for prop in dtype.properties.values():
        if prop.name.startswith('_'):
            continue
        value = build_full_response(
            context,
            prop.dtype,
            patch=patch.get(prop.name, NA) if patch else NA,
            saved=saved.get(prop.name, NA) if saved else NA,
        )
        if value is not NA:
            full_patch[prop.name] = value
    return full_patch


@commands.build_full_response.register()  # noqa
def build_full_response(  # noqa
    context: Context,
    dtype: DataType,
    *,
    patch: Optional[object],
    saved: Optional[object],
):
    if patch is not NA:
        return patch
    elif saved is not NA:
        return saved
    else:
        return dtype.default


@commands.build_full_data_patch_for_nested_attrs.register()
def build_full_data_patch_for_nested_attrs(
    context: Context,
    model: Model,
    *,
    patch: dict,
    saved: dict,
) -> dict:
    # Creates full_patch from `data.saved` and `data.patch`
    #
    # For PostgreSQL only nested fields need to be converted to full patch,
    # because PostgreSQL does not support partial updates, thus we need
    # to fill nested fields with data from `data.saved` if it is missing in
    # `data.patch`
    full_patch = {}
    for name in patch:
        prop = model.properties[name]
        value = commands.build_full_data_patch_for_nested_attrs(
            context,
            prop.dtype,
            patch=patch.get(prop.name, NA),
            saved=saved.get(prop.name, NA) if saved else saved,
        )
        full_patch.update(value)
    return full_patch


@commands.build_full_data_patch_for_nested_attrs.register()  # noqa
def build_full_data_patch_for_nested_attrs(  # noqa
    context: Context,
    dtype: DataType,
    *,
    patch: object,
    saved: object,
) -> dict:
    if patch is NA:
        return {}
    else:
        return {dtype.prop.place: patch}


@commands.build_full_data_patch_for_nested_attrs.register()  # noqa
def build_full_data_patch_for_nested_attrs(  # noqa
    context: Context,
    dtype: Object,
    *,
    patch: dict,
    saved: dict,
) -> dict:
    if patch is NA:
        return saved
    else:
        # We should work with keys which are available to us from
        # patch and saved data. Do not rely on manifest data for this.
        prop_keys = set(patch or []) | set(saved or [])

        full_patch = {}
        for prop_key in prop_keys:
            # drop metadata columns
            if prop_key.startswith('_'):
                continue

            prop = dtype.properties[prop_key]
            value = commands.build_full_data_patch_for_nested_attrs(
                context,
                prop.dtype,
                patch=patch.get(prop.name, NA) if patch else patch,
                saved=saved.get(prop.name, NA) if saved else saved,
            )
            full_patch.update(value)
        return full_patch or {}


@commands.build_full_data_patch_for_nested_attrs.register()  # noqa
def build_full_data_patch_for_nested_attrs(  # noqa
    context: Context,
    dtype: File,
    *,
    patch: dict,
    saved: dict,
) -> dict:
    # if key for file is not available in patch - return NotAvailable
    if patch is NA:
        return saved

    # if key for file is available but value is falsy (None, {}),
    # then return None for metadata values
    if not patch:
        return {
            f'{dtype.prop.place}._content_type': None,
            f'{dtype.prop.place}._id': None,
        }

    # for any other case - return patch values and if not available
    # fallback to saved values or None
    return {
        f'{dtype.prop.place}._content_type': patch.get('_content_type'),
        f'{dtype.prop.place}._id': patch.get('_id'),
    }


@commands.build_full_data_patch_for_nested_attrs.register()  # noqa
def build_full_data_patch_for_nested_attrs(  # noqa
    context: Context,
    dtype: Ref,
    *,
    patch: dict,
    saved: dict,
) -> dict:
    # if key for file is not available in patch - return NotAvailable
    if patch is NA:
        return saved

    # if key for file is available but value is falsy (None, {}),
    # then return None for metadata values
    if not patch:
        return {
            f'{dtype.prop.place}._id': None,
        }

    # for any other case - return patch values and if not available
    # fallback to saved values or None
    return {
        f'{dtype.prop.place}._id': patch.get('_id'),
    }


async def _summary_response(context: Context, results: AsyncIterator[DataItem]) -> dict:
    errors = 0
    async for data in results:
        if data.error is not None:
            errors += 1

    if errors > 0:
        status_code = 400
    else:
        status_code = 200

    transaction = context.get('transaction')
    return status_code, {
        '_transaction': transaction.id,
        '_status': 'error' if errors > 0 else 'ok',
    }


async def _batch_response(context: Context, results: AsyncIterator[DataItem]) -> dict:
    errors = 0
    batch = []
    async for data in results:
        errors += data.error is not None
        batch.append(_get_simple_response(context, data))

    if errors > 0:
        status_code = 400
    else:
        status_code = 200

    transaction = context.get('transaction')
    return status_code, {
        '_transaction': transaction.id,
        '_status': 'error' if errors > 0 else 'ok',
        '_data': batch,
    }


async def simple_response(context: Context, results: AsyncIterator[DataItem]) -> dict:
    results = await alist(aslice(results, 2))
    assert len(results) == 1
    data = results[0]
    if data.error is not None:
        status_code = data.error.status_code
    elif data.action == Action.INSERT or (data.action == Action.UPSERT and data.saved is None):
        status_code = 201
    elif data.action == Action.DELETE:
        status_code = 204
    else:
        status_code = 200
    return status_code, _get_simple_response(context, data)


def _get_simple_response(context: Context, data: DataItem) -> dict:
    resp = prepare_response(context, data)
    resp = {k: v for k, v in resp.items() if not k.startswith('_')}
    if data.patch and '_id' in data.patch:
        resp['_id'] = data.patch['_id']
    elif data.saved:
        resp['_id'] = data.saved['_id']
    if data.patch and '_revision' in data.patch:
        resp['_revision'] = data.patch['_revision']
    elif data.saved:
        resp['_revision'] = data.saved['_revision']
    if data.action and data.model:
        if data.prop:
            resp = commands.prepare(context, data.action, data.prop.dtype, data.backend, resp)
        else:
            resp = commands.prepare(context, data.action, data.model, data.backend, resp)
    resp = {k: v for k, v in resp.items() if k in ('_id', '_revision', '_type') or not k.startswith('_')}
    if data.error is not None:
        return {
            **resp,
            '_errors': [exceptions.error_response(data.error)],
        }
    else:
        return resp


@commands.upsert.register()
async def upsert(
    context: Context,
    model: (Model, dataset.Model),
    backend: Backend,
    *,
    dstream: DataStream,
    stop_on_error: bool = True,
):
    async for saved, dstream in agroupby(dstream, key=lambda d: d.saved is not None):
        if saved:
            cmd = commands.update
        else:
            cmd = commands.insert
        dstream = cmd(
            context, model, model.backend, dstream=dstream,
            stop_on_error=stop_on_error,
        )
        async for data in dstream:
            yield data


@commands.patch.register()
async def patch(
    context: Context,
    model: (Model, dataset.Model),
    backend: Backend,
    *,
    dstream: DataStream,
    stop_on_error: bool = True,
):
    dstream = commands.update(
        context, model, model.backend, dstream=dstream,
        stop_on_error=stop_on_error,
    )
    async for data in dstream:
        yield data


@commands.wipe.register()
async def wipe(
    context: Context,
    request: Request,
    model: Model,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, Action.WIPE, model)
    commands.wipe(context, model, backend)
    response = {'wiped': True}
    return render(context, request, model, params, response, status_code=200)


@commands.wipe.register()  # noqa
async def wipe(  # noqa
    context: Context,
    request: Request,
    ns: Namespace,
    backend: type(None),
    *,
    action: Action,
    params: UrlParams,
):
    for model in traverse_ns_models(ns):
        commands.authorize(context, Action.WIPE, model)
    commands.wipe(context, ns, backend)
    response = {'wiped': True}
    return render(context, request, ns, params, response, status_code=200)


def prepare_headers(
    context: Context,
    node: Node,
    resp: dict,
    action: Action,
    is_batch: Optional[bool] = False
):
    headers = {}

    headers['date'] = rfc822_now()

    if action == Action.INSERT and not is_batch:
        server_url = context.get('config').server_url
        headers['location'] = f'{server_url}{node.endpoint}/{resp["_id"]}'
    return headers
