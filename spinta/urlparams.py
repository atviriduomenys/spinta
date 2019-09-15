import cgi
import itertools

from typing import Optional, List, Type, Iterator, Union

from starlette.requests import Request

from spinta.commands import prepare
from spinta.components import Context, Manifest, Node
from spinta.utils.url import parse_url_path
from spinta.components import UrlParams, Version
from spinta.exceptions import (
    BaseError,
    DatasetNotFoundError,
    DatasetResourceNotFoundError,
    ModelNotFound,
    MultipleDatasetModelsFoundError,
    DatasetModelOriginNotFoundError,
)


@prepare.register()
def prepare(context: Context, params: UrlParams, version: Version, request: Request) -> UrlParams:
    path = request.path_params['path'].strip('/')
    p = params.params = parse_url_path(context, path)
    _prepare_model_params(params)
    params.search = any([
        'show' in p,
        'sort' in p,
        'limit' in p,
        'offset' in p,
        'count' in p,
        'query_params' in p,
    ])
    params.sort = p.get('sort')
    params.limit = p.get('limit')
    params.show = p.get('show')
    params.query = p.get('query_params')

    if 'changes' in p:
        params.changes = True
        params.offset = p['changes'] or -10
    else:
        params.changes = False
        params.offset = p.get('offset')

    params.format = get_response_type(context, request, p)
    params.count = 'count' in p
    params.contents = 'contents' in p
    return params


def _prepare_model_params(params: UrlParams):
    p = params.params
    params.path = p['path']
    params.id = p.get('id')
    params.properties = p.get('properties')
    params.dataset = p.get('dataset')
    params.resource = p.get('resource')
    params.origin = p.get('origin')


def get_model_by_name(context: Context, manifest: Manifest, name: str):
    config = context.get('config')
    UrlParams = config.components['urlparams']['component']
    params = UrlParams()
    params.params = parse_url_path(context, name)
    _prepare_model_params(params)
    model = get_model_from_params(manifest, params)
    if model is None:
        raise ModelNotFound(model=name)
    return model


def _get_nodes_by_name(
    name: Optional[str],
    nodes: List[dict],
    _NotFoundError: Type[BaseError],
    **kwargs
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
    found = False
    for node in nodes:
        if name is None:
            if '' in node:
                yield node['']
                found = True
            else:
                yield from node.values()
                found = True
        elif name in node:
            yield node[name]
            found = True
    if name is not None and not found:
        raise _NotFoundError(**kwargs)


def get_model_from_params(manifest, params: UrlParams):
    name = params.path

    if name in manifest.endpoints:
        # Allow users to specify a different URL endpoint to make URL look
        # nicer, but that is optional, they can still use model.name.
        name = manifest.endpoints[name]

    if name not in manifest.tree:
        raise ModelNotFound(model=name)

    if params.dataset:
        # TODO: write tests for this thing below
        datasets = _get_nodes_by_name(
            params.dataset or '',
            [manifest.objects['dataset']],
            DatasetNotFoundError,
            dataset_name=params.dataset,
        )

        resources = _get_nodes_by_name(
            params.resource,
            (x.resources for x in datasets),
            DatasetResourceNotFoundError,
            dataset_name=params.dataset,
            resource_name=params.resource,
        )

        origins = _get_nodes_by_name(
            params.origin,
            (x.objects for x in resources),
            DatasetModelOriginNotFoundError,
            dataset=params.dataset,
            resource=params.resource,
            origin=params.origin,
        )

        models = _get_nodes_by_name(
            name,
            origins,
            # FIXME: add dataset, resource and origin to error context
            ModelNotFound,
            model=name,
        )

        models = list(itertools.islice(models, 2))

        if len(models) == 0:
            return None
        elif len(models) == 1:
            return models[0]
        else:
            # FIXME: add resource and origin to error context
            # FIXME: change name to model
            raise MultipleDatasetModelsFoundError(name=name, dataset=params.dataset)

    else:
        return manifest.objects['model'].get(name)


def get_response_type(context: Context, request: Request, params: dict = None):
    if params is None and 'path' in request.path_params:
        path = request.path_params['path'].strip('/')
        params = parse_url_path(context, path)
    elif params is None:
        params = {}

    if 'format' in params:
        return params['format']

    if 'accept' in request.headers and request.headers['accept']:
        formats = {
            'text/html': 'html',
            'application/xhtml+xml': 'html',
        }
        config = context.get('config')
        for name, exporter in config.exporters.items():
            for media_type in exporter.accept_types:
                formats[media_type] = name

        media_types, _ = cgi.parse_header(request.headers['accept'])
        for media_type in media_types.lower().split(','):
            if media_type in formats:
                return formats[media_type]

    return 'json'
