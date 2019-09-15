import collections
import datetime
import itertools
import operator

import pkg_resources as pres

from starlette.requests import Request
from starlette.templating import Jinja2Templates
from starlette.exceptions import HTTPException

from spinta.types.datatype import DataType
from spinta.commands.formats import Format
from spinta.components import Context, Action, UrlParams, Node, Property
from spinta.utils.url import build_url_path
from spinta import commands
from spinta.components import Model
from spinta.types import dataset


class Html(Format):
    content_type = 'text/html'
    accept_types = {
        'text/html',
    }
    params = {}


@commands.render.register()
def render(
    context: Context,
    request: Request,
    fmt: Html,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
):
    templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
    return templates.TemplateResponse('base.html', {
        **get_template_context(context, None, params),
        'request': request,
        'header': [],
        'data': [],
        'row': [],
        'formats': [],
    })


@commands.render.register()  # noqa
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Html,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
):
    return _render_model(context, request, model, action, params, data)


@commands.render.register()  # noqa
def render(
    context: Context,
    request: Request,
    model: dataset.Model,
    fmt: Html,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
):
    return _render_model(context, request, model, action, params, data)


def _render_model(
    context: Context,
    request: Request,
    model: Model,
    action: Action,
    params: UrlParams,
    data,
):
    header = []
    row = []

    if action == Action.CHANGES:
        data = get_changes(context, data, model, params)
        header = next(data)
        data = list(reversed(list(data)))
    elif action == Action.GETONE:
        row = list(get_row(context, data, model, params.params))
        data = []
    elif action in (Action.GETALL, Action.SEARCH):
        data = get_data(context, data, model, params)
        header = next(data)
        data = list(data)

    templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
    return templates.TemplateResponse('base.html', {
        **get_template_context(context, model, params),
        'request': request,
        'header': header,
        'data': data,
        'row': row,
        'formats': get_output_formats(params.params),
    })


def get_output_formats(params: dict):
    return [
        ('CSV', '/' + build_url_path({**params, 'format': 'csv'})),
        ('JSON', '/' + build_url_path({**params, 'format': 'json'})),
        ('JSONL', '/' + build_url_path({**params, 'format': 'jsonl'})),
        ('ASCII', '/' + build_url_path({**params, 'format': 'ascii'})),
    ]


def get_template_context(context: Context, model, params):
    loc = get_current_location(model, params.path, params.params)
    store = context.get('store')
    manifest = store.manifests['default']
    datasets_by_object = get_datasets_by_model(context)
    items = get_directory_content(manifest.tree, params.path)
    datasets = list(get_directory_datasets(datasets_by_object, params.path))
    return {
        'location': loc,
        'items': items,
        'datasets': datasets,
    }


def get_current_location(model, path, params):
    parts = path.split('/') if path else []
    loc = [('root', '/')]
    if 'id' in params:
        pk = params['id']
        pks = params['id'][:8]
    if 'dataset' in params:
        name = '/'.join(itertools.chain(*(
            [f':{k}', v]
            for k, v in get_model_link_params(model).items()
            if k in {'dataset', 'resource', 'origin'}
        )))
        loc += [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)]
        if 'id' in params or 'changes' in params:
            loc += [(name, get_model_link(model))]
        if 'id' in params:
            if 'changes' in params:
                loc += (
                    [(pks, get_model_link(model, pk=pk))] +
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(pks, None)] +
                    [(':changes', get_model_link(model, pk=pk, changes=None))]
                )
        else:
            if 'changes' in params:
                loc += (
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(name, None)] +
                    [(':changes', get_model_link(model, changes=None))]
                )
    elif model:
        if 'id' in params:
            loc += (
                [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                [(pks, None)] +
                [(':changes', get_model_link(model, pk=pk, changes=None))]
            )
        else:
            loc += (
                [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts[:-1], 1)] +
                [(p, None) for p in parts[-1:]] +
                [(':changes', get_model_link(model, changes=None))]
            )
    else:
        loc += (
            [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts[:-1], 1)] +
            [(p, None) for p in parts[-1:]]
        )
    return loc


def get_changes(context: Context, rows, model, params: UrlParams):
    props = [p for p in model.properties.values() if p.name not in ('id', 'type')]

    yield (
        ['change_id', 'transaction_id', 'datetime', 'action', 'id'] +
        [prop.name for prop in props if prop.name != 'revision']
    )

    current = {}
    for data in rows:
        if data['id'] not in current:
            current[data['id']] = {}
        current[data['id']].update(data['change'])
        row = [
            {'color': None, 'value': data['change_id'], 'link': None},
            {'color': None, 'value': data['transaction_id'], 'link': None},
            {'color': None, 'value': data['datetime'], 'link': None},
            {'color': None, 'value': data['action'], 'link': None},
            get_cell(context, params.params, model.properties['id'], data['id'], shorten=True),
        ]
        for prop in props:
            if prop.name == 'revision':
                continue
            if prop.name in data['change']:
                color = 'change'
            elif prop.name not in current:
                color = 'null'
            else:
                color = None
            row.append(get_cell(context, params.params, prop, current[data['id']].get(prop.name), shorten=True, color=color))
        yield row


def get_row(context: Context, row, model: Node, params: dict):
    if row is None:
        raise HTTPException(status_code=404)
    for prop in model.properties.values():
        yield prop.name, get_cell(context, params, prop, row.get(prop.name))


def get_cell(context: Context, params: dict, prop, value, shorten=False, color=None):
    COLORS = {
        'change': '#B2E2AD',
        'null': '#C1C1C1',
    }

    link = None
    model = None

    if prop.name == 'id' and value:
        model = prop.model
    elif prop.dtype.name == 'ref' and prop.dtype.object and value:
        model = commands.get_referenced_model(context, prop.model, prop.dtype.object)

    if model:
        link = '/' + build_url_path(get_model_link_params(model, pk=value))

    if prop.dtype.name in ('ref', 'pk') and shorten and isinstance(value, str):
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


def get_data(context: Context, rows, model: Node, params: UrlParams):
    if params.count:
        prop = Property()
        prop.dtype = DataType()
        prop.dtype.name = 'string'
        prop.name = 'count'
        prop.ref = None
        props = [prop]
        rows = [rows]
    else:
        if params.show:
            props = [p for p in model.properties.values() if p.name in params.show]
        else:
            props = [p for p in model.properties.values() if p.name != 'type']

    yield [prop.name for prop in props]

    for data in rows:
        row = []
        for prop in props:
            row.append(get_cell(context, params.params, prop, data.get(prop.name), shorten=True))
        yield row


def get_datasets_by_model(context):
    store = context.get('store')
    datasets = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict)))
    for dataset_ in store.manifests['default'].objects.get('dataset', {}).values():
        for resource in dataset_.resources.values():
            for model in resource.models():
                datasets[model.name][dataset_.name][resource.name][model.origin] = model
    return datasets


def get_directory_content(tree, path):
    return [
        (item, ('/' if path else '') + path + '/' + item)
        for item in tree[path]
    ]


def get_directory_datasets(datasets, path):
    if path in datasets:
        for dataset_, resources in sorted(datasets[path].items(), key=operator.itemgetter(0)):
            for resource, objects in sorted(resources.items(), key=operator.itemgetter(0)):
                for origin, model in sorted(objects.items(), key=operator.itemgetter(0)):
                    name = [dataset_]
                    link = ('/' if path else '') + path + '/:dataset/' + dataset_
                    if resource:
                        name += [resource]
                        link += '/:resource/' + resource
                    if origin:
                        name += [origin]
                        link += '/:origin/' + origin
                    yield {
                        'name': '/'.join(name),
                        'link': link,
                        'canonical': model.canonical,
                    }


def get_model_link_params(model, *, pk=None, **extra):
    params = {
        'path': model.name,
    }

    if isinstance(model, dataset.Model):
        dset = model.parent.parent
        params['dataset'] = dset.name

        resource = model.parent
        if resource.name:
            params['resource'] = resource.name

        if model.origin:
            params['origin'] = model.origin

    if pk is not None:
        params['id'] = pk

    params.update(extra)

    return params


def get_model_link(*args, **kwargs):
    return '/' + build_url_path(get_model_link_params(*args, **kwargs))
