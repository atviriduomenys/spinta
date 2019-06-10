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
from starlette.responses import FileResponse
from starlette.templating import Jinja2Templates
from starlette.requests import Request

from spinta import commands
from spinta.types.type import Type
from spinta.types.store import get_model_from_params
from spinta.utils.tree import build_path_tree
from spinta.utils.url import build_url_path
from spinta.utils.url import parse_url_path
from spinta.exceptions import NotFound
from spinta.components import Context, Attachment, Action


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
            model = get_model_from_params(manifest, params.model, _params)
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

        datasets_by_object = get_datasets_by_model(context)
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
            model = get_model_from_params(manifest, params.model, _params)
        except NotFound as e:
            raise HTTPException(status_code=404, detail=str(e))

        if request.method == 'POST':
            context.bind('transaction', manifest.backend.transaction, write=True)
            data = await get_request_data(request)
            data = commands.push(context, model, model.backend, data, action=Action.INSERT)
            return JSONResponse(data, status_code=201)

        elif request.method == 'PUT':
            context.bind('transaction', manifest.backend.transaction, write=True)
            if params.properties:
                prop = get_prop_from_params(model, params)
                # FIXME: attachment is a FileSystem backend specific thing and
                #        should not be here.
                #
                #        Probably push command should take request and do all
                #        the needed processing, because some actions use
                #        information in headers, they need to return streaming
                #        responces, consume streaming inputs. So there is no
                #        other way, but to give more control to commands like
                #        push, getone, getall and etc.
                #
                #        [give_request_to_push]
                attachment = await commands.load(context, prop, prop.backend, request)
                data = {
                    'id': params.id,
                    prop.name: attachment,
                }

                # Here we handle subresources in general, but currently only
                # case of subresources is files.
                #
                # So here, first we store a file, which might be stored in a
                # separate backend. Then we updated model with reference to this
                # file. Model also checks if reference is correct and if file
                # really exists.
                #
                # TODO: here we assume, that subresource is always stored in a
                #       separate backend, which is a wrong assumption. We should
                #       check if file has a separate backend and if not, then we
                #       should save file with single push on model.
                commands.push(context, prop, prop.backend, attachment, action=Action.UPDATE)
                data = commands.push(context, model, model.backend, data, action=Action.UPDATE)
                return JSONResponse(data, status_code=200)
            else:
                raise NotImplementedError("PUT is not supported.")

        elif request.method == 'DELETE':
            context.bind('transaction', manifest.backend.transaction, write=True)
            if params.properties:
                prop = get_prop_from_params(model, params)
                data = commands.get(context, model, model.backend, _params['id'])
                value = data.get(prop.name)
                if value is None:
                    raise HTTPException(status_code=404, detail=f"File {prop.name!r} not found in {params.id!r}.")
                # FIXME: see [give_request_to_push]
                attachment = Attachment(
                    # FIXME: content_type is hardcoded here, but it should be
                    #        configured in property node as an parameter with
                    #        default value 'content_type'.
                    #
                    #        [fs_content_type_param]
                    content_type=value['content_type'],
                    filename=value['filename'],
                    data=None,
                )

                data = {
                    'id': params.id,
                    prop.name: None,
                }
                data = commands.push(context, model, model.backend, data, action=Action.UPDATE)
                commands.push(context, prop, prop.backend, attachment, action=Action.DELETE)
                return JSONResponse({'id': params.id}, status_code=200)
            else:
                raise NotImplementedError("DELETE is not supported.")

        context.set('transaction', manifest.backend.transaction())

        if params.changes:
            rows = commands.changes(
                context, model, model.backend,
                id=_params.get('id'),
                limit=_params.get('limit', 100),
                offset=_params.get('changes', -10),
            )
            # TODO: see below (look for peek_and_stream)
            rows = peek_and_stream(rows)

        elif params.id:
            if params.properties:
                data = commands.get(context, model, model.backend, _params['id'])
                prop = get_prop_from_params(model, params)
                value = data.get(prop.name)
                if value is None:
                    raise HTTPException(status_code=404, detail=f"File {prop.name!r} not found in {params.id!r}.")
                # FIXME: this is pretty much hardcoded just for file fields
                #        case, this should be moved to backends.fs
                return FileResponse(prop.backend.path / value['filename'], media_type=value['content_type'])

            else:
                rows = [commands.get(context, model, model.backend, _params['id'])]
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

    if 'rs' in params:
        if 'id' in params:
            if 'changes' in params:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':ds/' + params['ds'] + '/:rs/' + params['rs'], '/' + '/'.join(parts + [':ds', params['ds'], ':rs', params['rs']]))] +
                    [(params['id'][:8], '/' + build_url_path({
                        'path': path,
                        'id': params['id'],
                        'ds': params['ds'],
                        'rs': params['rs'],
                    }))] +
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':ds/' + params['ds'] + '/:rs/' + params['rs'], '/' + '/'.join(parts + [':ds', params['ds'], ':rs', params['rs']]))] +
                    [(params['id'][:8], None)] +
                    [(':changes', '/' + build_url_path({
                        'path': path,
                        'id': params['id'],
                        'ds': params['ds'],
                        'rs': params['rs'],
                        'changes': None,
                    }))]
                )
        else:
            if 'changes' in params:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':ds/' + params['ds'] + '/:rs/' + params['rs'], '/' + build_url_path({
                        'path': path,
                        'ds': params['ds'],
                        'rs': params['rs'],
                    }))] +
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                    [(':ds/' + params['ds'] + '/:rs/' + params['rs'], None)] +
                    [(':changes', '/' + build_url_path({
                        'path': path,
                        'ds': params['ds'],
                        'rs': params['rs'],
                        'changes': None,
                    }))]
                )
    elif model:
        if 'id' in params:
            loc += (
                [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                [(params['id'][:8], None)] +
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
    model = get_model_from_params(manifest, params['path'], params)
    backend = model.backend
    rows = commands.changes(
        context, model, backend,
        id=params.get('id'),
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
    model = get_model_from_params(manifest, params['path'], params)
    backend = model.backend
    row = commands.get(context, model, backend, params['id'])
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
        if 'rs' in params:
            extra['ds'] = params['ds']
            extra['rs'] = params['rs']
        link = '/' + build_url_path({
            'path': params['path'],
            'id': value,
            **extra,
        })
        if shorten:
            value = value[:8]
    elif hasattr(prop, 'ref') and prop.ref and value:
        extra = {}
        if 'rs' in params:
            extra['ds'] = params['ds']
            extra['rs'] = params['rs']
        link = '/' + build_url_path({
            'path': prop.ref,
            'id': value,
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
    model = get_model_from_params(manifest, params['path'], params)
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


def get_datasets_by_model(context):
    store = context.get('store')
    datasets = collections.defaultdict(lambda: collections.defaultdict(dict))
    for dataset in store.manifests['default'].objects.get('dataset', {}).values():
        for resource in dataset.resources.values():
            for model in resource.objects.values():
                datasets[model.name][dataset.name][resource.name] = resource
    return datasets


def get_directory_content(tree, path):
    return [
        (item, ('/' if path else '') + path + '/' + item)
        for item in tree[path]
    ]


def get_directory_datasets(datasets, path):
    if path in datasets:
        for dataset, resources in sorted(datasets[path].items(), key=operator.itemgetter(0)):
            for resource in sorted(resources.values(), key=operator.attrgetter('name')):
                link = ('/' if path else '') + path + '/:ds/' + dataset + '/:rs/' + resource.name
                yield {
                    'name': dataset + '/' + resource.name,
                    'link': link,
                    'canonical': resource.objects[path].canonical,
                }


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


async def get_request_data(request):
    ct = request.headers.get('content-type')
    if ct != 'application/json':
        raise HTTPException(
            status_code=415,
            detail="only 'application/json' content-type is supported",
        )

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

    return data


def get_prop_from_params(model, params):
    if len(params.properties) != 1:
        raise HTTPException(status_code=400, detail=(
            "only one property can be given, now "
            "these properties were given: "
        ) % ', '.join(map(repr, params.properties)))
    prop = params.properties[0]
    if prop not in model.properties:
        raise HTTPException(status_code=404, detail=f"Unknown property {prop}.")
    return model.properties[prop]
