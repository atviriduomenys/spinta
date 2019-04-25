import collections
import datetime
import operator

import pkg_resources as pres

from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.responses import StreamingResponse
from starlette.templating import Jinja2Templates

from spinta import commands
from spinta.types import Type
from spinta.types.store import get_model_from_params
from spinta.utils.tree import build_path_tree
from spinta.utils.url import build_url_path


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
        loc = get_current_location(path, _params)

        context.bind('transaction', manifest.backend.transaction)

        if params.dataset:
            formats = [
                ('CSV', '/' + build_url_path({**_params, 'format': 'csv'})),
                ('JSON', '/' + build_url_path({**_params, 'format': 'json'})),
                ('JSONL', '/' + build_url_path({**_params, 'format': 'jsonl'})),
                ('ASCII', '/' + build_url_path({**_params, 'format': 'asciitable'})),
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

        async def stream(context, rows, exporter, _params):
            with context.enter():
                context.bind('transaction', store.backends['default'].transaction)
                for data in exporter(rows, **_params):
                    yield data

        model = get_model_from_params(manifest, params.model, _params)

        if request.method == 'POST':
            context.bind('transaction', manifest.backend.transaction, write=True)
            data = await request.json()
            id = commands.push(context, model, model.backend, data)
            data = {
                'type': model.name,
                'id': id,
            }
            return JSONResponse(data, status_code=201)

        context.bind('transaction', manifest.backend.transaction)

        if params.changes:
            with context.enter():
                context.bind('transaction', manifest.backend.transaction)
                rows = commands.changes(
                    context, model, model.backend,
                    id=_params.get('id', {}).get('value'),
                    limit=_params.get('limit', 100),
                    offset=_params.get('changes', -10),
                )

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
            )

        exporter = config.exporters[params.format]
        media_type = exporter.content_type
        _params = {
            k: v for k, v in _params.items()
            if k in exporter.params
        }

        g = stream(context, rows, exporter, _params)
        return StreamingResponse(g, media_type=media_type)

    else:
        raise HTTPException(status_code=404)


def get_current_location(path, params):
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
    model = get_model_from_params(manifest, params['path'], params)
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
        link = '/' + build_url_path({
            'path': params['path'],
            'id': {'value': value, 'type': None},
            'source': params['source'],
        })
        if shorten:
            value = value[:8]
    elif prop.ref and value:
        link = '/' + build_url_path({
            'path': prop.ref,
            'id': {'value': value, 'type': None},
            'source': params['source'],
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
