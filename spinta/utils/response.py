import cgi
import collections
import datetime
import itertools
import json
import operator

import pkg_resources as pres

from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.responses import StreamingResponse
from starlette.templating import Jinja2Templates
from starlette.requests import Request

from spinta import commands
from spinta.types.type import Type
from spinta.types.store import get_model_from_params
from spinta.utils.tree import build_path_tree
from spinta.utils.url import build_url_path
from spinta.utils.url import parse_url_path
from spinta.exceptions import NotFound
from spinta.components import Context


async def create_http_response(context, params, request):
    config = context.get('config')
    _params = params.params
    path = params.model

    store = context.get('store')
    manifest = store.manifests['default']

    if params.format == 'html':
        header = []
        data = []
        formats = []
        items = []
        datasets = []
        row = []

        context.bind('transaction', manifest.backend.transaction)

        try:
            model = get_model_from_params(manifest.endpoints, params.model, _params)
        except NotFound:
            model = None

        loc = get_current_location(model, path, _params)

        if model:
            formats = [
                ('CSV', '/' + build_url_path({**_params, 'format': 'csv'})),
                ('JSON', '/' + build_url_path({**_params, 'format': 'json'})),
                ('JSONL', '/' + build_url_path({**_params, 'format': 'jsonl'})),
                ('ASCII', '/' + build_url_path({**_params, 'format': 'ascii'})),
            ]

            if params.changes:
                data = get_changes(context, _params)
                header = next(data)
                data = list(reversed(list(data)))
            else:
                if params.id:
                    row = list(get_row(context, _params))
                else:
                    data = get_data(context, _params)
                    header = next(data)
                    data = list(data)

        else:
            datasets_by_object = get_datasets_by_object(context)
            tree = build_path_tree(
                manifest.objects.get('model', {}).keys(),
                datasets_by_object.keys(),
            )
            items = get_directory_content(tree, path) if path in tree else []
            datasets = list(get_directory_datasets(datasets_by_object, path))

        templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))

        return templates.TemplateResponse('base.html', {
            'request': request,
            'location': loc,
            'formats': formats,
            'items': items,
            'datasets': datasets,
            'header': header,
            'data': data,
            'row': row,
        })

    elif params.format in config.exporters:

        if not params.model:
            raise HTTPException(status_code=404)

        try:
            model = get_model_from_params(manifest.endpoints, params.model, _params)
        except NotFound as e:
            raise HTTPException(status_code=404, detail=str(e))

        if request.method == 'POST':
            context.bind('transaction', manifest.backend.transaction, write=True)

            # only `application/json` is supported
            ct = request.headers.get('content-type')
            if ct != 'application/json':
                raise HTTPException(
                    status_code=415,
                    detail="only 'application/json' content-type is supported",
                )

            # make sure json is valid
            try:
                data = await request.json()
            except json.decoder.JSONDecodeError:
                raise HTTPException(
                    status_code=400,
                    detail="not a valid json",
                )

            if 'revision' in data.keys():
                raise HTTPException(
                    status_code=400,
                    detail="cannot create 'revision'",
                )

            data = commands.push(context, model, model.backend, data, action='insert')
            return JSONResponse(data, status_code=201)

        context.set('transaction', manifest.backend.transaction())

        if params.changes:
            rows = commands.changes(
                context, model, model.backend,
                id=_params.get('id', {}).get('value'),
                limit=_params.get('limit', 100),
                offset=_params.get('changes', -10),
            )
            # TODO: see below (look for peek_and_stream)
            rows = peek_and_stream(rows)

        elif params.id:
            rows = [commands.get(context, model, model.backend, _params['id']['value'])]
            _params['wrap'] = False

        else:
            rows = commands.getall(
                context, model, model.backend,
                show=_params.get('show'),
                sort=_params.get('sort', [{'name': 'id', 'ascending': True}]),
                offset=_params.get('offset'),
                limit=_params.get('limit', 100),
                count='count' in _params,
                query_params=_params.get('query_params'),
            )

            # TODO: Currently authorization and many other thins happens inside
            #       backend functions and on iterator. If anything failes, we
            #       get a RuntimeError, because error happens after response is
            #       started, that means we no longer can return correct HTTP
            #       response with an error. That is why, we first run one
            #       iteration before returning StreamingResponse, to catch
            #       possible errors.
            #
            #       In other words, all possible checks should happend before
            #       starting response.
            #
            #       Current solution with PeekStream is just a temporary
            #       workaround.
            rows = peek_and_stream(rows)

        exporter = config.exporters[params.format]
        media_type = exporter.content_type
        _params = {
            k: v for k, v in _params.items()
            if k in exporter.params
        }

        stream = aiter(exporter(rows, **_params))

        return StreamingResponse(stream, media_type=media_type)

    else:
        raise HTTPException(status_code=404)


def get_current_location(model, path, params):
    parts = path.split('/') if path else []
    loc = [('root', '/')]

    if 'source' in params:
        if 'id' in params:
            if 'changes' in params:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':source/' + params['source'], '/' + '/'.join(parts + [':source', params['source']]))] +
                    [(params['id']['value'][:8], '/' + build_url_path({
                        'path': path,
                        'id': params['id'],
                        'source': params['source'],
                    }))] +
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':source/' + params['source'], '/' + '/'.join(parts + [':source', params['source']]))] +
                    [(params['id']['value'][:8], None)] +
                    [(':changes', '/' + build_url_path({
                        'path': path,
                        'id': params['id'],
                        'source': params['source'],
                        'changes': None,
                    }))]
                )
        else:
            if 'changes' in params:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':source/' + params['source'], '/' + build_url_path({
                        'path': path,
                        'source': params['source'],
                    }))] +
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':source/' + params['source'], None)] +
                    [(':changes', '/' + build_url_path({
                        'path': path,
                        'source': params['source'],
                        'changes': None,
                    }))]
                )
    elif model:
        if 'id' in params:
            loc += (
                [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                [(params['id']['value'][:8], None)] +
                [(':changes', '/' + build_url_path({
                    'path': path,
                    'id': params['id'],
                    'changes': None,
                }))]
            )
        else:
            loc += (
                [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts[:-1], 1)] +
                [(p, None) for p in parts[-1:]] +
                [(':changes', '/' + build_url_path({
                    'path': path,
                    'changes': None,
                }))]
            )
    else:
        loc += (
            [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts[:-1], 1)] +
            [(p, None) for p in parts[-1:]]
        )
    return loc


def get_changes(context, params):
    store = context.get('store')
    manifest = store.manifests['default']
    model = get_model_from_params(manifest.endpoints, params['path'], params)
    backend = model.backend
    rows = commands.changes(
        context, model, backend,
        id=params.get('id', {}).get('value'),
        limit=params.get('limit', 100),
        offset=params.get('changes', -10),
    )

    props = [p for p in model.properties.values() if p.name not in ('id', 'type')]

    yield (
        ['change_id', 'transaction_id', 'datetime', 'action', 'id'] +
        [prop.name for prop in props]
    )

    current = {}
    for data in rows:
        if data['id'] not in current:
            current[data['id']] = {}
        current[data['id']].update(data['change'])
        row = [
            {'color': None, 'value': data['change_id'], 'link': None},
            {'color': None, 'value': data['transaction_id'], 'link': None},
            {'color': None, 'value': data['datetime'].isoformat(), 'link': None},
            {'color': None, 'value': data['action'], 'link': None},
            get_cell(params, model.properties['id'], data['id'], shorten=True),
        ]
        for prop in props:
            if prop.name in data['change']:
                color = 'change'
            elif prop.name not in current:
                color = 'null'
            else:
                color = None
            row.append(get_cell(params, prop, current[data['id']].get(prop.name), shorten=True, color=color))
        yield row


def get_row(context, params):
    store = context.get('store')
    manifest = store.manifests['default']
    model = get_model_from_params(manifest.endpoints, params['path'], params)
    backend = model.backend
    row = commands.get(context, model, backend, params['id']['value'])
    if row is None:
        raise HTTPException(status_code=404)
    for prop in model.properties.values():
        yield prop.name, get_cell(params, prop, row.get(prop.name))


def get_cell(params, prop, value, shorten=False, color=None):
    COLORS = {
        'change': '#B2E2AD',
        'null': '#C1C1C1',
    }

    link = None

    if prop.name == 'id' and value:
        extra = {}
        if 'source' in params:
            extra['source'] = params['source']
        link = '/' + build_url_path({
            'path': params['path'],
            'id': {'value': value, 'type': None},
            **extra,
        })
        if shorten:
            value = value[:8]
    elif hasattr(prop, 'ref') and prop.ref and value:
        extra = {}
        if 'source' in params:
            extra['source'] = params['source']
        link = '/' + build_url_path({
            'path': prop.ref,
            'id': {'value': value, 'type': None},
            **extra,
        })
        if shorten:
            value = value[:8]

    if isinstance(value, datetime.datetime):
        value = value.isoformat()

    max_column_length = 200
    if shorten and isinstance(value, str) and len(value) > max_column_length:
        value = value[:max_column_length] + '...'

    if value is None:
        value = ''
        if color is None:
            color = 'null'

    return {
        'value': value,
        'link': link,
        'color': COLORS[color] if color else None,
    }


def get_data(context, params):
    store = context.get('store')
    manifest = store.manifests['default']
    model = get_model_from_params(manifest.endpoints, params['path'], params)
    backend = model.backend
    rows = commands.getall(
        context, model, backend,
        show=params.get('show'),
        sort=params.get('sort', [{'name': 'id', 'ascending': True}]),
        offset=params.get('offset'),
        limit=params.get('limit', 100),
        count='count' in params,
    )

    if 'count' in params:
        prop = Type()
        prop.name = 'count'
        prop.ref = None
        props = [prop]
    else:
        props = [p for p in model.properties.values() if p.name != 'type']

    yield [prop.name for prop in props]

    for data in rows:
        row = []
        for prop in props:
            row.append(get_cell(params, prop, data.get(prop.name), shorten=True))
        yield row


def get_datasets_by_object(context):
    store = context.get('store')
    datasets = collections.defaultdict(dict)
    for dataset in store.manifests['default'].objects.get('dataset', {}).values():
        for obj in dataset.objects.values():
            datasets[obj.name][dataset.name] = dataset
    return datasets


def get_directory_content(tree, path):
    return [
        (item, ('/' if path else '') + path + '/' + item)
        for item in tree[path]
    ]


def get_directory_datasets(datasets, path):
    if path in datasets:
        for name, dataset in sorted(datasets[path].items(), key=operator.itemgetter(0)):
            link = ('/' if path else '') + path + '/:source/' + name
            yield {
                'name': name,
                'link': link,
                'canonical': dataset.objects[path].canonical,
            }


def get_response_type(context: Context, request: Request, params: dict = None):
    if params is None and 'path' in request.path_params:
        path = request.path_params['path'].strip('/')
        params = parse_url_path(path)
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


def peek_and_stream(stream):
    peek = next(stream, None)

    def _iter():
        if peek is not None:
            for data in itertools.chain([peek], stream):
                yield data

    return _iter()


async def aiter(stream):
    for data in stream:
        yield data
