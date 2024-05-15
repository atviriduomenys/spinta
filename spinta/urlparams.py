import cgi
import re
import urllib.parse
from collections import OrderedDict
from typing import List
from typing import Union

from starlette.requests import Request

from spinta import exceptions, commands
from spinta import spyna
from spinta.commands import is_object_id
from spinta.commands import prepare
from spinta.components import Action, ParamsPage, decode_page_values
from spinta.components import Config
from spinta.components import Context, Node
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import UrlParams, Version
from spinta.core.ufuncs import Bind, Expr, asttoexpr
from spinta.exceptions import ModelNotFound, InvalidPageParameterCount, InvalidPageKey
from spinta.manifests.components import Manifest
from spinta.ufuncs.basequerybuilder.components import BaseQueryBuilder
from spinta.ufuncs.basequerybuilder.ufuncs import Star
from spinta.ufuncs.requestparamsbuilder.components import RequestParamsBuilder
from spinta.utils import url as urlutil
from spinta.utils.config import asbool
from spinta.utils.encoding import is_url_safe_base64


@prepare.register(Context, UrlParams, Version, Request)
def prepare(
    context: Context,
    params: UrlParams,
    version: Version,
    request: Request,
    **kwargs
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
    params.accept_langs = get_preferred_accept_lang(request, params)
    params.content_langs = get_preferred_content_lang(request, params)
    params.lang = get_required_lang(context, params)
    config: Config = context.get('config')
    params.fmt = config.exporters[params.format if params.format else 'json']

    if params.select:
        params_builder = RequestParamsBuilder(context).init(params)
        select_expr = _get_select_expr_from_params(params)
        params_builder.resolve(select_expr)
    if params.expand:
        params.expand = _get_expand_expr_from_params(params)


def _get_expand_expr_from_params(params: UrlParams) -> Expr:
    ast = {'name': 'expand', 'args': [
        arg if isinstance(arg, dict) else {'name': 'bind', 'args': [arg]}
        for arg in params.expand
    ]}
    return asttoexpr(ast)


def _get_select_expr_from_params(params: UrlParams) -> Expr:
    ast = {'name': 'select', 'args': [
        arg if isinstance(arg, dict) else {'name': 'bind', 'args': [arg]}
        for arg in params.select
    ]}
    return asttoexpr(ast)


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
                try:
                    limit = int(args[0])
                    if limit <= 0:
                        raise Exception
                    params.limit = limit
                except Exception:
                    raise exceptions.InvalidValue(
                        operator='limit',
                        message="Limit must be a number higher than 0"
                    )

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
        # Added for backwards compatibility
        # this adds count to select, so it works the same way as select(count())
        elif name == 'count':
            if not params.select or ('count()' not in params.select and param not in params.select):
                if params.select:
                    params.select.append(param)
                else:
                    params.select = [param]
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
            if args:
                params.select = args
        elif name == 'bbox':
            params.bbox = args
        elif name == 'schema':
            params.action = Action.SCHEMA
        elif name == 'inspect':
            params.action = Action.INSPECT
        elif name == 'fault-tolerant':
            params.fault_tolerant = True
        elif name == 'wipe':
            params.action = Action.WIPE
        elif name == 'check':
            params.action = Action.CHECK
        elif name == 'page':
            params.page = ParamsPage()
            is_sort_attr = False
            is_disabled = False
            key_given = False
            for arg in args:
                if isinstance(arg, dict):
                    if is_sort_attr:
                        raise InvalidPageParameterCount()
                    elif is_disabled:
                        raise InvalidPageParameterCount()
                    if arg['name'] == 'bind' and 'size' in arg['args']:
                        params.page.size = arg['args'][1]
                        is_sort_attr = True
                    elif arg['name'] == 'bind' and 'disable' in arg['args']:
                        params.page.is_enabled = not asbool(arg['args'][1])
                        is_disabled = True
                else:
                    if key_given:
                        raise InvalidPageParameterCount()
                    if not is_url_safe_base64(bytes(arg, 'ascii')):
                        raise InvalidPageKey(key=arg)
                    params.page.values = decode_page_values(arg)
                    key_given = True
        elif name == 'expand':
            if params.expand is None:
                params.expand = args
            else:
                params.expand += args
        elif name == 'lang':
            if not args:
                raise Exception("LANG REQUIRES ARGS")
            if params.lang is None:
                params.lang = args
            else:
                params.lang += args
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
    parts: List[str],
) -> int:
    # By default, namespace, properties starts with lower case and Model starts with upper
    for i, part in enumerate(parts):
        if part[0].isupper():
            return i + 1
    return len(parts)


def _resolve_path(context: Context, params: UrlParams) -> None:
    if params.path_parts is None:
        params.path = ''
        params.path_parts = []

    manifest = context.get('store').manifest
    i = _find_model_name_index(params.path_parts)
    parts = params.path_parts[i:]
    params.path = '/'.join(params.path_parts[:i])
    params.model = get_model_from_params(context, manifest, params)

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
    context: Context,
    manifest: Manifest,
    params: UrlParams,
) -> Union[Namespace, Model]:
    name = params.path
    if params.ns:
        if commands.has_namespace(context, manifest, name):
            return commands.get_namespace(context, manifest, name)
        else:
            raise ModelNotFound(manifest, model=name)

    elif commands.has_model(context, manifest, name):
        return commands.get_model(context, manifest, name)

    elif commands.has_namespace(context, manifest, name):
        return commands.get_namespace(context, manifest, name)

    else:
        if params.action == Action.SCHEMA:
            return None
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


ACCEPT_LANGUAGE_HEADER_MAX_LENGTH = 500
accept_language_re = re.compile(
    r"""
        # "en", "en-au", "x-y-z", "es-419", "*"
        ([A-Za-z]{1,8}(?:-[A-Za-z0-9]{1,8})*|\*)
        # Optional "q=1.00", "q=0.8"
        (?:\s*;\s*q=(0(?:\.[0-9]{,3})?|1(?:\.0{,3})?))?
        # Multiple accepts per header.
        (?:\s*,\s*|$)
    """,
    re.VERBOSE,
)


def _parse_accept_lang_header(lang_string):
    """
    Parse the lang_string, which is the body of an HTTP Accept-Language
    header, and return a tuple of (lang, q-value), ordered by 'q' values.

    Return an empty tuple if there are any format errors in lang_string.
    """
    result = []
    pieces = accept_language_re.split(lang_string.lower())
    if pieces[-1]:
        return ()
    for i in range(0, len(pieces) - 1, 3):
        first, lang, priority = pieces[i : i + 3]
        if first:
            return []
        if priority:
            priority = float(priority)
        else:
            priority = 1.0
        result.append((lang, priority))
    result.sort(key=lambda k: k[1], reverse=True)
    return list(OrderedDict.fromkeys(key.split('-')[0] for key, value in result))


def parse_accept_lang_header(lang_string):
    """
    Parse the value of the Accept-Language header up to a maximum length.

    The value of the header is truncated to a maximum length to avoid potential
    denial of service and memory exhaustion attacks. Excessive memory could be
    used if the raw value is very large as it would be cached due to the use of
    functools.lru_cache() to avoid repetitive parsing of common header values.
    """
    # If the header value doesn't exceed the maximum allowed length, parse it.
    if len(lang_string) <= ACCEPT_LANGUAGE_HEADER_MAX_LENGTH:
        return _parse_accept_lang_header(lang_string)

    # If there is at least one comma in the value, parse up to the last comma
    # before the max length, skipping any truncated parts at the end of the
    # header value.
    if (index := lang_string.rfind(",", 0, ACCEPT_LANGUAGE_HEADER_MAX_LENGTH)) > 0:
        return _parse_accept_lang_header(lang_string[:index])

    # Don't attempt to parse if there is only one language-range value which is
    # longer than the maximum allowed length and so truncated.
    return []


def get_required_lang(context: Context, params: UrlParams):
    env = BaseQueryBuilder(context)
    langs = []
    if params.lang:
        stared = False
        for lang in params.lang:
            result = lang
            if isinstance(lang, dict):
                result = env.call(lang.get("name", ""), *lang.get("args", []), **lang.get("kwargs", {}))

            if isinstance(result, Bind):
                result = result.name

            if isinstance(result, Star):
                stared = True
                if len(langs) > 0:
                    raise Exception("STARED NEEDS TO BE ONLY ARGUMENT")
                langs.append(result)
            elif isinstance(result, str):
                if stared:
                    raise Exception("STARED NEEDS TO BE ONLY ARGUMENT")
                langs.append(result)
    return langs


def get_preferred_accept_lang(
    request: Request,
    params: UrlParams = None,
) -> List[str]:
    header = 'accept-language'
    if params is not None and params.accept_langs:
        return params.accept_langs
    if header in request.headers and request.headers[header]:
        return parse_accept_lang_header(request.headers[header])
    return []


def get_preferred_content_lang(
    request: Request,
    params: UrlParams = None,
) -> List[str]:
    header = 'content-language'
    if params is not None and params.content_langs:
        return params.content_langs
    if header in request.headers and request.headers[header]:
        return parse_accept_lang_header(request.headers[header])
    return []
