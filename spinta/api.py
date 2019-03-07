import collections
import datetime

import pkg_resources as pres
import uvicorn

from starlette.applications import Starlette
from starlette.templating import Jinja2Templates
from starlette.exceptions import HTTPException
from starlette.responses import StreamingResponse

from spinta.utils.url import parse_url_path, build_url_path
from spinta.utils.tree import build_path_tree
from spinta.store import get_model_from_params


templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))

app = Starlette()

store = None

COLORS = {
    'change': '#B2E2AD',
    'null': '#C1C1C1',
}


@app.route('/{path:path}')
async def homepage(request):
    global store

    url_path = request.path_params['path'].strip('/')
    params = parse_url_path(url_path)
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
            ]

            if 'changes' in params:
                data = get_changes(store, params)
                header = next(data)
                data = list(reversed(list(data)))
            else:
                if 'id' in params:
                    row = list(get_row(store, params))
                else:
                    data = get_data(store, params)
                    header = next(data)
                    data = list(data)

        else:
            datasets_by_object = get_datasets_by_object(store)
            tree = build_path_tree(
                store.objects['default'].get('model', {}).keys(),
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

    elif fmt in ('csv', 'json'):
        async def generator():
            for data in store.export(fmt, path, params):
                yield data

        media_types = {
            'csv': 'text/csv',
            'json': 'application/json',
        }

        return StreamingResponse(generator(), media_type=media_types[fmt])

    else:
        raise HTTPException(status_code=404)


def run(store):
    set_store(store)
    app.debug = store.config.debug
    uvicorn.run(app, host='0.0.0.0', port=8000)


def set_store(store_):
    global store
    if store is not None:
        raise Exception("Store was already set.")
    store = store_


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


def get_datasets_by_object(store):
    datasets = collections.defaultdict(dict)
    for dataset in store.objects['default'].get('dataset', {}).values():
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
        for name in sorted(datasets[path].keys()):
            link = ('/' if path else '') + path + '/:source/' + name
            yield name, link


def get_data(store, params):
    model = get_model_from_params(store, 'default', params['path'], params)
    rows = store.getall(params['path'], {'limit': 100, **params})

    props = [p for p in model.properties.values() if p.name != 'type']

    yield [prop.name for prop in props]

    for data in rows:
        row = []
        for prop in props:
            row.append(get_cell(params, prop, data.get(prop.name), shorten=True))
        yield row


def get_row(store, params):
    model = get_model_from_params(store, 'default', params['path'], params)
    row = store.get(params['path'], params['id']['value'], params)
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

    if shorten and isinstance(value, str) and len(value) > 42:
        value = value[:42] + '...'

    if value is None:
        value = ''
        if color is None:
            color = 'null'

    return {
        'value': value,
        'link': link,
        'color': COLORS[color] if color else None,
    }


def get_changes(store, params):
    model = get_model_from_params(store, 'default', params['path'], params)
    rows = store.changes({'limit': 100, 'offset': params['changes'] or -10, **params})

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
