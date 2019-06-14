import collections
import datetime
import operator

import pkg_resources as pres

from starlette.templating import Jinja2Templates

from spinta.types.type import Type
from spinta.commands.formats import Format
from spinta.components import Action
from spinta.utils.tree import build_path_tree
from spinta.utils.url import build_url_path


class Html(Format):
    content_type = 'text/html'
    accept_types = {
        'text/html',
    }
    params = {}

    def __call__(self, data, action: Action):
        config = context.get('config')
        _params = params.params
        path = params.model

        header = []
        data = []
        formats = []
        items = []
        datasets = []
        row = []

        loc = get_current_location(model, path, _params)

        if model:
            formats = [
                ('CSV', '/' + build_url_path({**_params, 'format': 'csv'})),
                ('JSON', '/' + build_url_path({**_params, 'format': 'json'})),
                ('JSONL', '/' + build_url_path({**_params, 'format': 'jsonl'})),
                ('ASCII', '/' + build_url_path({**_params, 'format': 'ascii'})),
            ]

            if action == Action.CHANGES:
                data = get_changes(data)
                header = next(data)
                data = list(reversed(list(data)))
            elif action == Action.GETONE:
                row = list(get_row(data))
            elif action in (Action.GETALL, Action.SEARCH):
                data = get_data(data)
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


def get_changes(rows):
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


def get_data(context, request, params):
    _params = params.params
    store = context.get('store')
    manifest = store.manifests['default']
    model = get_model_from_params(manifest, _params['path'], _params)
    backend = model.backend
    rows = commands.getall(context, request, model, backend, params=params)

    if 'count' in _params:
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
            row.append(get_cell(_params, prop, data.get(prop.name), shorten=True))
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
