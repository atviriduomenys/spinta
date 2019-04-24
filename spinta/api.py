import collections
import datetime
import operator

import pkg_resources as pres
import uvicorn

from starlette.applications import Starlette
from starlette.templating import Jinja2Templates
from starlette.exceptions import HTTPException
from starlette.responses import StreamingResponse

from spinta.commands import getall, get, changes, prepare
from spinta.types import Type
from spinta.types.store import get_model_from_params
from spinta.urlparams import Version
from spinta.utils.tree import build_path_tree
from spinta.utils.url import build_url_path


templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))

app = Starlette()

context = None

COLORS = {
    'change': '#B2E2AD',
    'null': '#C1C1C1',
}


@app.route('/{path:path}')
async def homepage(request):
    global context

    url_path = request.path_params['path'].strip('/')

    with context.enter():
        config = context.get('config')
        store = context.get('store')
        context.bind('transaction', store.backends['default'].transaction)

        # get UrlParams parser class from configs
        UrlParams = config.components['urlparams']['component']

        url_params = UrlParams()
        url_params.path = url_path

        url_params = prepare(context, url_params, Version())
        params = url_params.params
        path = params['path']

        fmt = params.get('format', 'html')

        if fmt == 'html':
            header = []
            data = []
            formats = []
            items = []
            datasets = []
            row = []
            loc = get_current_location(path, params)

            if 'source' in params:
                formats = [
                    ('CSV', '/' + build_url_path({**params, 'format': 'csv'})),
                    ('JSON', '/' + build_url_path({**params, 'format': 'json'})),
                    ('JSONL', '/' + build_url_path({**params, 'format': 'jsonl'})),
                    ('ASCII', '/' + build_url_path({**params, 'format': 'asciitable'})),
                ]

                if 'changes' in params:
                    data = get_changes(context, params)
                    header = next(data)
                    data = list(reversed(list(data)))
                else:
                    if 'id' in params:
                        row = list(get_row(context, params))
                    else:
                        data = get_data(context, params)
                        header = next(data)
                        data = list(data)

            else:
                datasets_by_object = get_datasets_by_object(context)
                tree = build_path_tree(
                    store.manifests['default'].objects.get('model', {}).keys(),
                    datasets_by_object.keys(),
                )
                items = get_directory_content(tree, path) if path in tree else []
                datasets = list(get_directory_datasets(datasets_by_object, path))

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

        elif fmt in config.exporters:
            async def stream(context, rows, exporter, params):
                with context.enter():
                    context.bind('transaction', store.backends['default'].transaction)
                    for data in exporter(rows, **params):
                        yield data

            manifest = store.manifests['default']
            model = get_model_from_params(manifest, params['path'], params)

            if 'changes' in params:
                rows = changes(
                    context, model, model.backend,
                    id=params.get('id', {}).get('value'),
                    limit=params.get('limit', 100),
                    offset=params.get('changes', -10),
                )

            elif 'id' in params:
                rows = [get(context, model, model.backend, params['id']['value'])]
                params['wrap'] = False

            else:
                rows = getall(
                    context, model, model.backend,
                    show=params.get('show'),
                    sort=params.get('sort', [{'name': 'id', 'ascending': True}]),
                    offset=params.get('offset'),
                    limit=params.get('limit', 100),
                    count='count' in params,
                )

            exporter = config.exporters[fmt]
            media_type = exporter.content_type
            params = {
                k: v for k, v in params.items()
                if k in exporter.params
            }

            g = stream(context, rows, exporter, params)
            return StreamingResponse(g, media_type=media_type)

        else:
            raise HTTPException(status_code=404)


def run(context):
    set_context(context)
    app.debug = context.get('config').debug
    uvicorn.run(app, host='0.0.0.0', port=8000)


def set_context(context_):
    global context
    if context is not None:
        raise Exception("Context was already set.")
    context = context_


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


def get_data(context, params):
    store = context.get('store')
    backend = store.backends['default']
    manifest = store.manifests['default']
    model = get_model_from_params(manifest, params['path'], params)
    rows = getall(
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


def get_row(context, params):
    store = context.get('store')
    backend = store.backends['default']
    manifest = store.manifests['default']
    model = get_model_from_params(manifest, params['path'], params)
    row = get(context, model, backend, params['id']['value'])
    if row is None:
        raise HTTPException(status_code=404)
    for prop in model.properties.values():
        yield prop.name, get_cell(params, prop, row.get(prop.name))


def get_cell(params, prop, value, shorten=False, color=None):
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


def get_changes(context, params):
    store = context.get('store')
    backend = store.backends['default']
    manifest = store.manifests['default']
    model = get_model_from_params(manifest, params['path'], params)
    rows = changes(
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
