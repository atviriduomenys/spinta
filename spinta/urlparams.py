from typing import List

import cgi
from typing import Union

import itertools
import urllib.parse

from starlette.requests import Request

from spinta.commands import prepare
from spinta.components import Action
from spinta.components import Config
from spinta.components import Context, Node
from spinta.components import Model
from spinta.components import Namespace
from spinta.manifests.components import Manifest
from spinta.utils import url as urlutil
from spinta.components import UrlParams, Version
from spinta.commands import is_object_id
from spinta import exceptions
from spinta import spyna
from spinta.exceptions import ModelNotFound


@prepare.register(Context, UrlParams, Version, Request)
def prepare(
    context: Context,
    params: UrlParams,
    version: Version,
    request: Request,
) -> UrlParams:
    params.parsetree = (
        urlutil.parse_url_path(request.path_params['path'].strip('/')) +
        parse_url_query(urllib.parse.unquote(request.url.query))
    )
    prepare_urlparams(context, params, request)
    params.head = request.method == 'HEAD'
    params.action = get_action(params, request)
    return params


METHOD_TO_ACTION = {
    'POST': Action.INSERT,
    'PUT': Action.UPDATE,
    'PATCH': Action.PATCH,
    'DELETE': Action.DELETE,
}


def get_action(params: UrlParams, request: Request) -> Action:
    if params.action is not None:
        # Might be set in _prepare_urlparams_from_path
        return params.action
    if request.method in ('GET', 'HEAD'):
        if params.changes:
            return Action.CHANGES
        elif params.pk:
            return Action.GETONE
        else:
            search = any((
                params.query,
                params.select,
                params.limit is not None,
                params.offset is not None,
                params.sort,
                params.count,
            ))
            return Action.SEARCH if search else Action.GETALL
    else:
        return METHOD_TO_ACTION[request.method]


def parse_url_query(query):
    if not query:
        return []
    rql = spyna.parse(query)
    if rql['name'] == 'and':
        return rql['args']
    else:
        return [rql]


def prepare_urlparams(context: Context, params: UrlParams, request: Request):
    _prepare_urlparams_from_path(params)
    _resolve_path(context, params)
    params.format = get_response_type(context, request, params)
    config: Config = context.get('config')
    params.fmt = config.exporters[params.format]


def _prepare_urlparams_from_path(params: UrlParams):
    params.formatparams = {}

    for param in params.parsetree:
        name = param['name']
        args = param['args']

        if name == 'path':
            params.path_parts = args
        elif name == 'ns':
            params.ns = True
        elif name == 'all':
            params.all = True
        elif name == 'select':
            # TODO: Traverse args and resolve all properties, etc.
            # XXX: select can be used only once.
            if params.select is None:
                params.select = args
            else:
                params.select += args
            # TODO: This is a temporary hack, there is no need to specifically
            #       mark count in params.
            for arg in args:
                if arg['name'] == 'count':
                    params.count = True
                    break
        elif name == 'sort':
            # TODO: Traverse args and resolve all properties, etc.
            if params.sort is None:
                params.sort = args
            else:
                params.sort += args
        elif name == 'limit':
            if params.limit is not None:
                raise exceptions.InvalidValue(
                    operator='limit',
                    message="Limit operator can be set only once.",
                )
            if len(args) == 2:
                if params.offset is not None:
                    raise exceptions.InvalidValue(
                        operator='offset',
                        message="Offset operator can be set only once.",
                    )
                params.limit, params.offset = map(int, args)
            elif len(args) == 1:
                params.limit = int(args[0])
            else:
                raise exceptions.InvalidValue(
                    operator='limit',
                    message="Too many arguments.",
                )
        elif name == 'offset':
            if params.offset is not None:
                raise exceptions.InvalidValue(
                    operator='offset',
                    message="Offset operator can be set only once.",
                )
            if len(args) == 1:
                params.offset = int(args[0])
            else:
                raise exceptions.InvalidValue(
                    operator='offset',
                    message="Too many or too few arguments. One argument is expected.",
                )
        # XXX: `count` can't be at the top level, well it can, but at the top
        #      level it is used as where condition.
        elif name == 'count':
            params.count = True
        elif name == 'changes':
            params.changes = True
            params.changes_offset = int(args[0]) if args else None
        elif name == 'format':
            if isinstance(args[0], dict) and args[0]['name'] == 'bind':
                params.format = _read_format_params(args[0])
                args = args[1:]
            elif isinstance(args[0], str):
                params.format = args[0]
                args = args[1:]
            for arg in args:
                assert len(arg['args']) == 1, arg
                params.formatparams[arg['name']] = _read_format_params(arg['args'][0])
        elif name == 'summary':
            params.summary = True
        elif name == 'fault-tolerant':
            params.fault_tolerant = True
        elif name == 'wipe':
            params.action = Action.WIPE
        elif name == 'check':
            params.action = Action.CHECK
        else:
            if params.query is None:
                params.query = []
            params.query.append(param)


def _read_format_params(node):
    if not isinstance(node, dict):
        return node

    if node['name'] == 'bind':
        return node['args'][0]

    raise Exception(f"Unknown node {node!r}.")


def _join_path_parts(*parts: str) -> str:
    return '/'.join(parts)


def _find_model_name_index(
    manifest: Manifest,
    parts: List[str],
) -> int:
    keys = (
        set(manifest.namespaces) |
        set(manifest.models) |
        set(manifest.endpoints)
    )
    for i, name in enumerate(itertools.accumulate(parts, _join_path_parts)):
        if name not in keys:
            return i
    return len(parts)


def _resolve_path(context: Context, params: UrlParams) -> None:
    if params.path_parts is None:
        params.path = ''
        params.path_parts = []

    manifest = context.get('store').manifest
    i = _find_model_name_index(manifest, params.path_parts)
    parts = params.path_parts[i:]
    params.path = '/'.join(params.path_parts[:i])
    params.model = get_model_from_params(manifest, params)

    if parts:
        # Resolve ID.
        params.pk = parts.pop(0)
        if not is_object_id(context, params.model.backend, params.model, params.pk):
            given_path = '/'.join(params.path_parts)
            raise ModelNotFound(model=given_path)

    if parts:
        # Resolve property (subresource).
        prop = parts.pop(0)
        if prop.endswith(':ref'):
            params.propref = True
            prop = prop[:-len(':ref')]
        if prop not in params.model.flatprops:
            raise exceptions.PropertyNotFound(params.model, property=prop)
        params.prop = params.model.flatprops[prop]

    if parts:
        given_path = '/'.join(params.path_parts)
        raise ModelNotFound(model=given_path)


def get_model_by_name(context: Context, manifest: Manifest, name: str) -> Node:
    config = context.get('config')
    UrlParams_ = config.components['urlparams']['component']
    params = UrlParams_()
    params.parsetree = urlutil.parse_url_path(name)
    _prepare_urlparams_from_path(params)
    _resolve_path(context, params)
    return params.model


def get_model_from_params(
    manifest: Manifest,
    params: UrlParams,
) -> Union[Namespace, Model]:
    name = params.path

    if name in manifest.endpoints:
        # Allow users to specify a different URL endpoint to make URL look
        # nicer, but that is optional, they can still use model.name.
        name = manifest.endpoints[name]

    if params.ns:
        if name in manifest.namespaces:
            return manifest.namespaces[name]
        else:
            raise ModelNotFound(manifest, model=name)

    elif name in manifest.models:
        return manifest.models[name]

    elif name in manifest.namespaces:
        return manifest.namespaces[name]

    else:
        raise ModelNotFound(model=name)


def get_response_type(
    context: Context,
    request: Request,
    params: UrlParams = None,
) -> str:
    config = context.get('config')

    if params is not None and params.format:
        if params.format not in config.exporters:
            raise exceptions.UnknownOutputFormat(name=params.format)
        return params.format

    if 'accept' in request.headers and request.headers['accept']:
        formats = {
            'text/html': 'html',
            'application/xhtml+xml': 'html',
        }
        for name, exporter in config.exporters.items():
            for media_type in exporter.accept_types:
                formats[media_type] = name

        media_types, _ = cgi.parse_header(request.headers['accept'])
        for media_type in media_types.lower().split(','):
            if media_type in formats:
                return formats[media_type]

    return 'json'
