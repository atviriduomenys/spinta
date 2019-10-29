from typing import Optional, List, Dict, Iterator, Union

import cgi
import itertools

import pyrql

from starlette.requests import Request

from spinta.commands import prepare
from spinta.components import Context, Manifest, Node
from spinta.utils import url as urlutil
from spinta.components import UrlParams, Version
from spinta.commands import is_object_id
from spinta import exceptions
from spinta.exceptions import (
    NodeNotFound,
    ModelNotFound,
    MultipleDatasetModelsFoundError,
)


@prepare.register()
def prepare(context: Context, params: UrlParams, version: Version, request: Request) -> UrlParams:
    params.parsetree = (
        urlutil.parse_url_path(request.path_params['path'].strip('/')) +
        parse_url_query(request.url.query)
    )
    prepare_urlparams(context, params, request)
    return params


def parse_url_query(query):
    if not query:
        return []
    rql = pyrql.parse(query)
    if rql['name'] == 'and':
        return rql['args']
    else:
        return [rql]


def prepare_urlparams(context: Context, params: UrlParams, request: Request):
    _prepare_urlparams_from_path(params)
    _resolve_path(context, params)
    params.format = get_response_type(context, request, params)


def _prepare_urlparams_from_path(params):
    for param in params.parsetree:
        name = param['name']
        args = param['args']

        if name == 'path':
            params.path = args
        elif name == 'ns':
            params.ns = True
        elif name == 'all':
            params.all = True
        elif name in ('dataset', 'resource', 'origin'):
            setattr(params, name, '/'.join(args))
        elif name == 'select':
            # TODO: Traverse args and resolve all properties, etc.
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
        elif name == 'count':
            params.count = True
        elif name == 'changes':
            params.changes = True
            params.changes_offset = args[0] if args else None
        elif name == 'format':
            params.fmt = args[0]
        elif name == 'summary':
            params.summary = True
        elif name == 'fault-tolerant':
            params.fault_tolerant = True
        else:
            if params.query is None:
                params.query = []
            params.query.append(param)


def _find_model_name_index(nss: Dict[str, Node], endpoints: dict, parts: List[str]) -> int:
    for i, name in enumerate(itertools.accumulate(parts, '{}/{}'.format)):
        if name not in nss and name not in endpoints:
            return i
    return len(parts)


def _resolve_path(context: Context, params: UrlParams) -> None:
    if params.path is None:
        params.path = ''

    path = '/'.join(params.path)
    manifest = context.get('store').manifests['default']
    nss = manifest.objects['ns']
    i = _find_model_name_index(nss, manifest.endpoints, params.path)
    parts = params.path[i:]
    params.path = '/'.join(params.path[:i])
    params.model = get_model_from_params(manifest, params)

    if parts:
        # Resolve ID.
        params.pk = parts.pop(0)
        if not is_object_id(context, params.model.backend, params.model, params.pk):
            raise ModelNotFound(model=path)

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
        raise ModelNotFound(model=path)


def get_model_by_name(context: Context, manifest: Manifest, name: str) -> Node:
    config = context.get('config')
    UrlParams = config.components['urlparams']['component']
    params = UrlParams()
    params.parsetree = urlutil.parse_url_path(name)
    _prepare_urlparams_from_path(params)
    _resolve_path(context, params)
    return params.model


def _get_nodes_by_name(
    type_: str, name: Optional[str], candidates: List[dict],
) -> Iterator[Union[Node, dict]]:
    """Try to find nodes by given name in given list of node dicts.

    Full dataset model name looks like this:

        orgs/:dataset/gov/:resource/sql/:origin/org

    But resource and origin is optional and can be empty strings:

        orgs/:dataset/gov/:resource//:origin/

    Also one can specify only model and dataset:

        orgs/:dataset/gov

    When resource and origin is not given, then Spinta tries to find model in
    givne dataset, by looking into all resources and all origins. If only one
    model is found, then that model is returned. But if multiple models are
    found, then it's an error.

    Also, if resource and/or origin is not given, then Spinta tries to check if
    there is a resource or a node with empty name, and if it finds one, uses it
    by default. So that means, if resoruce and/or origin is not specified, then
    this can mean resource and/or origin with empty name or if there is no
    resource and/or origin with empty name, then all resources and/or origins
    are considered.
    """
    nodes = {}
    found = name is None
    for node, children in candidates:
        if name is None:
            if '' in children:
                yield node, children['']
            else:
                yield from zip(itertools.repeat(node), children.values())
        elif name in children:
            yield node, children[name]
            found = True
        else:
            nodes[node.name] = node
    if not found:
        # Find closest single parent.
        while len(nodes) > 1:
            nodes = {
                node.parent.name: node.parent
                for node in nodes.values()
            }
        node = next(iter(nodes.values()))
        raise NodeNotFound(node, type=type_, name=name)


def get_model_from_params(manifest, params: UrlParams):
    name = params.path

    if name in manifest.endpoints:
        # Allow users to specify a different URL endpoint to make URL look
        # nicer, but that is optional, they can still use model.name.
        name = manifest.endpoints[name]

    if params.ns:
        if name in manifest.objects['ns']:
            return manifest.objects['ns'][name]
        else:
            raise ModelNotFound(manifest, model=name)

    elif params.dataset:
        # TODO: write tests for this thing below
        datasets = _get_nodes_by_name('dataset', params.dataset or '', [
            (manifest, manifest.objects['dataset']),
        ])

        resources = _get_nodes_by_name('resource', params.resource, (
            (dataset, dataset.resources) for _, dataset in datasets
        ))

        origins = _get_nodes_by_name('origin', params.origin, (
            (resource, resource.objects) for _, resource in resources
        ))

        if name == '':
            next(origins)
            return manifest.objects['ns'][name]

        # FIXME: Here we lose origin in error context. I'm starting to thing,
        #        that origin should be a normal node object too, not a bare
        #        dict.
        models = _get_nodes_by_name('model', name, (
            (resource, origin) for resource, origin in origins
        ))

        models = list(itertools.islice(models, 2))

        if len(models) == 0:
            return None
        elif len(models) == 1:
            return models[0][1]
        else:
            # FIXME: add resource and origin to error context
            # FIXME: change name to model
            raise MultipleDatasetModelsFoundError(name=name, dataset=params.dataset)

    elif name in manifest.objects['model']:
        return manifest.objects['model'].get(name)

    elif name in manifest.objects['ns']:
        return manifest.objects['ns'][name]

    else:
        raise ModelNotFound(model=name)


def get_response_type(context: Context, request: Request, params: UrlParams = None):
    config = context.get('config')

    if params is not None and params.fmt:
        if params.fmt not in config.exporters:
            raise exceptions.UnknownOutputFormat(name=params.fmt)
        return params.fmt

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
