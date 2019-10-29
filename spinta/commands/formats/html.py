from typing import Optional

import datetime
import itertools

import pkg_resources as pres

from starlette.requests import Request
from starlette.templating import Jinja2Templates
from starlette.exceptions import HTTPException

from spinta.types.datatype import DataType
from spinta.commands.formats import Format
from spinta.components import Context, Action, UrlParams, Node, Property
from spinta.utils.url import build_url_path
from spinta import commands
from spinta.components import Namespace, Model
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
    ns: Namespace,
    fmt: Html,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
):
    return _render_model(context, request, ns, action, params, data)


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
        row = list(get_row(context, data, model))
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
        'formats': get_output_formats(params),
    })


def get_output_formats(params: UrlParams):
    return [
        # XXX I don't like that, there should be a better way to build links
        #     from UrlParams instance.
        ('CSV', '/' + build_url_path(params.changed_parsetree({'format': ['csv']}))),
        ('JSON', '/' + build_url_path(params.changed_parsetree({'format': ['json']}))),
        ('JSONL', '/' + build_url_path(params.changed_parsetree({'format': ['jsonl']}))),
        ('ASCII', '/' + build_url_path(params.changed_parsetree({'format': ['ascii']}))),
    ]


def get_template_context(context: Context, model, params):
    return {
        'location': get_current_location(model, params),
    }


def get_current_location(model, params: UrlParams):
    parts = params.path.split('/') if params.path else []
    loc = [('root', '/')]
    if params.pk:
        pk = params.pk
        pks = params.pk[:8]
    if model.type == 'model:ns':
        loc += (
            [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts[:-1], 1)] +
            [(p, None) for p in parts[-1:]]
        )
    elif params.dataset:
        name = '/'.join(itertools.chain(*(
            [f':{p["name"]}', '/'.join(p['args'])]
            for p in get_model_link_params(model)
            if p['name'] in {'dataset', 'resource', 'origin'}
        )))
        loc += [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)]
        if params.pk or params.changes:
            loc += [(name, get_model_link(model))]
        if params.pk:
            if params.changes:
                loc += (
                    [(pks, get_model_link(model, pk=pk))] +
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(pks, None)] +
                    [(':changes', get_model_link(model, pk=pk, changes=[]))]
                )
        else:
            if params.changes:
                loc += (
                    [(':changes', None)]
                )
            else:
                loc += (
                    [(name, None)] +
                    [(':changes', get_model_link(model, changes=[]))]
                )
    else:
        if params.pk:
            loc += (
                [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts, 1)] +
                [(pks, None)] +
                [(':changes', get_model_link(model, pk=pk, changes=[]))]
            )
        else:
            loc += (
                [(p, '/' + '/'.join(parts[:i])) for i, p in enumerate(parts[:-1], 1)] +
                [(p, None) for p in parts[-1:]] +
                [(':changes', get_model_link(model, changes=[]))]
            )
    return loc


def get_changes(context: Context, rows, model, params: UrlParams):
    props = [
        p for p in model.properties.values() if (
            not p.name.startswith('_')
        )
    ]

    yield (
        ['_change', '_revision', '_transaction', '_created', '_op', '_id'] +
        [prop.name for prop in props if prop.name != 'revision']
    )

    # XXX: With large changes sets this will consume a lot of memmory.
    current = {}
    for data in rows:
        id_ = data['_id']
        if id_ not in current:
            current[id_] = {}
        current[id_].update({
            k: v for k, v in data.items() if not k.startswith('_')
        })
        row = [
            {'color': None, 'value': data['_change'], 'link': None},
            {'color': None, 'value': data['_revision'], 'link': None},
            {'color': None, 'value': data['_transaction'], 'link': None},
            {'color': None, 'value': data['_created'], 'link': None},
            {'color': None, 'value': data['_op'], 'link': None},
            get_cell(context, model.properties['_id'], id_, shorten=True),
        ]
        for prop in props:
            if prop.name in data:
                color = 'change'
            elif prop.name not in current:
                color = 'null'
            else:
                color = None
            value = current[id_].get(prop.name)
            cell = get_cell(context, prop, value, shorten=True, color=color)
            row.append(cell)
        yield row


def get_row(context: Context, row, model: Node):
    if row is None:
        raise HTTPException(status_code=404)
    include = {'_type', '_id', '_revision'}
    for prop in model.properties.values():
        if prop.name in include or not prop.name.startswith('_'):
            yield prop.name, get_cell(context, prop, row.get(prop.name))


def get_cell(context: Context, prop, value, shorten=False, color=None):
    COLORS = {
        'change': '#B2E2AD',
        'null': '#C1C1C1',
    }

    link = None
    model = None

    if prop.model.type == 'model:ns' and prop.name == '_id':
        shorten = False
        link = '/' + value
    elif prop.name == '_id' and value:
        model = prop.model
    elif prop.dtype.name == 'ref' and prop.dtype.object and value:
        model = commands.get_referenced_model(context, prop, prop.dtype.object)

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
    # XXX: For things like aggregations, a dynamic model should be created with
    #      all the properties coming from aggregates.
    if params.count:
        prop = Property()
        prop.dtype = DataType()
        prop.dtype.name = 'string'
        prop.name = 'count'
        prop.ref = None
        prop.model = model
        props = [prop]
    else:
        if params.select:
            props = [p for p in model.properties.values() if p.name in params.select]
        else:
            if model.type == 'model:ns':
                include = {'_id', '_type'}
            else:
                include = {'_id'}
            props = [
                p for p in model.properties.values() if (
                    p.name in include or
                    not p.name.startswith('_')
                )
            ]

    yield [prop.name for prop in props]

    for data in rows:
        row = []
        for prop in props:
            row.append(get_cell(context, prop, data.get(prop.name), shorten=True))
        yield row


def get_model_link_params(model: Node, *, pk: Optional[str] = None, **extra):
    ptree = [
        {
            'name': 'path',
            'args': (
                model.name.split('/') +
                ([pk] if pk is not None else [])
            ),
        }
    ]

    if isinstance(model, dataset.Model):
        dset = model.parent.parent
        ptree.append({
            'name': 'dataset',
            'args': [dset.name],
        })

        resource = model.parent
        if resource.name:
            ptree.append({
                'name': 'resource',
                'args': [resource.name],
            })

        if model.origin:
            ptree.append({
                'name': 'origin',
                'args': [model.origin],
            })

    for k, v in extra.items():
        ptree.append({
            'name': k,
            'args': v,
        })

    return ptree


def get_model_link(*args, **kwargs):
    return '/' + build_url_path(get_model_link_params(*args, **kwargs))
