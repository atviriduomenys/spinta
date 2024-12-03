import json
from io import TextIOWrapper
from typing import cast, Optional, List, Dict, Any, Tuple

import itertools
from urllib.error import HTTPError

import requests
from starlette.datastructures import UploadFile
from starlette.requests import Request

from spinta import commands
from spinta import exceptions
from spinta.api.schema import schema_api
from spinta.backends.helpers import validate_and_return_transaction, validate_and_return_begin
from spinta.cli.helpers.errors import ErrorCounter
from spinta.formats.components import Format
from spinta.components import Action, Model
from spinta.components import Context
from spinta.components import Node
from spinta.components import Store
from spinta.components import UrlParams
from spinta.exceptions import BaseError
from spinta.exceptions import NoBackendConfigured
from spinta.exceptions import error_response
from spinta.api.inspect import inspect_api
from spinta.renderer import render


async def _check_post(context: Context, request: Request, params: UrlParams):
    form = await request.form()
    field = cast(UploadFile, form['manifest'])
    filename = field.filename

    # ---8<----
    # FIXME: https://github.com/pallets/werkzeug/issues/1344
    field.file.readable = field.file._file.readable
    field.file.writable = field.file._file.writable
    field.file.seekable = field.file._file.seekable
    # --->8----

    # XXX: This is a quick and dirty hack, uploaded manifest file should be
    #      loaded as any other manifest. Here only tabluar manifest loading is
    #      hardcoded.
    from spinta.core.config import RawConfig
    from spinta.manifests.helpers import clone_manifest
    from spinta.manifests.helpers import detect_manifest_from_path
    from spinta.manifests.helpers import load_manifest_nodes
    from spinta.manifests.tabular.components import TabularManifest
    from spinta.manifests.tabular.helpers import read_tabular_manifest
    rc: RawConfig = context.get('rc')
    Manifest_ = detect_manifest_from_path(rc, filename)
    if issubclass(Manifest_, TabularManifest):
        schemas = read_tabular_manifest(
            Manifest_.format,
            path=filename,
            file=TextIOWrapper(field.file, encoding='utf-8'),
        )
    else:
        return {
            'status': 'error',
            'errors': [{
                'type': 'manifest',
                'code': 'ValueError',
                'message': (
                    "Can't detect manifest type from given path {filename!r}."
                ),
            }],
        }

    try:
        manifest = clone_manifest(context)
        load_manifest_nodes(context, manifest, schemas)
        commands.link(context, manifest)
        commands.check(context, manifest)
    except BaseError as e:
        return {
            'status': 'error',
            'errors': [error_response(e)],
        }
    else:
        return {
            'status': 'OK',
        }


async def _check(context: Context, request: Request, params: UrlParams):
    commands.authorize(context, Action.CHECK, params.model)

    if request.method == 'POST':
        data = await _check_post(context, request, params)
    else:
        data = None

    return render(
        context,
        request,
        params.model,
        params,
        data,
        action=Action.CHECK,
    )


async def create_http_response(
    context: Context,
    params: UrlParams,
    request: Request,
):
    store: Store = context.get('store')
    manifest = store.manifest

    if manifest.backend is None:
        raise NoBackendConfigured(manifest)

    if params.action == Action.CHECK:
        return await _check(context, request, params)

    if request.method == 'POST' and params.action == Action.INSPECT:
        return await inspect_api(context, request, params)

    if request.method == 'POST' and params.action == Action.SCHEMA:
        return await schema_api(context, request, params)

    if request.method in ('GET', 'HEAD'):
        context.attach('transaction', validate_and_return_transaction, context, manifest.backend)

        if params.changes:
            _enforce_limit(context, params)
            return await commands.changes(
                context,
                params.model,
                request,
                action=params.action,
                params=params,
            )

        elif params.pk:
            model = params.model
            action = params.action

            if model.keymap:
                context.attach(
                    f'keymap.{model.keymap.name}',
                    lambda: model.keymap,
                )

            return await commands.getone(
                context,
                request,
                model,
                action=action,
                params=params,
            )
        elif params.summary:
            model = params.model
            action = params.action
            return await commands.summary(
                context,
                request,
                model,
                action=action,
                params=params
            )
        else:
            _enforce_limit(context, params)
            action = params.action
            model = params.model
            if isinstance(model, Model):
                model = commands.get_model(context, store.manifest, model.name)
            backend = model.backend

            if backend is not None:
                # Namespace nodes do not have backend.
                context.attach(f'transaction.{backend.name}', validate_and_return_begin, context, backend)

            if model.keymap:
                context.attach(
                    f'keymap.{model.keymap.name}',
                    lambda: model.keymap,
                )

            return await commands.getall(
                context,
                model,
                request,
                action=action,
                params=params,
            )

    elif request.method == 'DELETE' and params.action == Action.WIPE:
        if params.pk:
            raise NotImplementedError
        else:
            context.attach(
                'transaction',
                validate_and_return_transaction,
                context,
                manifest.backend,
                write=True,
            )
            return await commands.wipe(
                context,
                request,
                params.model,
                params.model.backend,
                action=params.action,
                params=params,
            )

    else:
        context.attach('transaction', validate_and_return_transaction, context, manifest.backend, write=True)
        action = params.action
        if params.prop and params.propref:
            return await commands.push(
                context,
                request,
                params.prop.dtype,
                params.model.backend,
                action=action,
                params=params,
            )
        elif params.prop:
            return await commands.push(
                context,
                request,
                params.prop.dtype,
                params.prop.dtype.backend,
                action=action,
                params=params,
            )
        else:
            return await commands.push(
                context,
                request,
                params.model,
                params.model.backend,
                action=action,
                params=params,
            )


def _enforce_limit(context: Context, params: UrlParams):
    fmt: Format = params.fmt
    # XXX: I think this is not the best way to enforce limit, maybe simply
    #      an error should be raised?
    # XXX: Max resource count should be configurable.
    if not fmt.streamable and (params.limit is None or params.limit > 100):
        params.limit = params.limit_enforced_to + 1
        params.limit_enforced = True


def peek_and_stream(stream):
    peek = list(itertools.islice(stream, 2))

    def _iter():
        for data in itertools.chain(peek, stream):
            yield data

    return _iter()


async def aiter(stream):
    for data in stream:
        yield data


async def get_request_data(node: Node, request: Request):
    ct = request.headers.get('content-type')
    if ct != 'application/json':
        raise exceptions.UnknownContentType(
            node,
            content_type=ct,
            supported_content_types=['application/json'],
        )

    try:
        data = await request.json()
    except json.decoder.JSONDecodeError as e:
        raise exceptions.JSONError(node, error=str(e))

    return data


def get_request(
    client: requests.Session,
    server: str,
    timeout: Tuple[float, float],
    *,
    stop_on_error: bool = False,
    ignore_errors: Optional[List[int]] = None,
    error_counter: ErrorCounter = None,
) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    if not ignore_errors:
        ignore_errors = []

    try:
        resp = client.request("GET", server, timeout=timeout)
    except IOError as e:
        if error_counter:
            error_counter.increase()
        if stop_on_error:
            raise
        return None, None

    try:
        resp.raise_for_status()
    except HTTPError:
        if resp.status_code not in ignore_errors:
            if error_counter:
                error_counter.increase()
            try:
                recv = resp.json()
            except requests.JSONDecodeError:
                if stop_on_error:
                    raise

            if stop_on_error:
                raise
        return resp.status_code, None

    return resp.status_code, resp.json()
