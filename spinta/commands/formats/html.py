from typing import Any
from typing import Dict
from typing import NamedTuple
from typing import Optional, Union, List

import datetime
from typing import Tuple

import pkg_resources as pres

from starlette.requests import Request
from starlette.templating import Jinja2Templates
from starlette.exceptions import HTTPException

from spinta.components import Config
from spinta.manifests.components import Manifest
from spinta.types.datatype import DataType, Ref
from spinta.commands.formats import Format
from spinta.components import Context, Action, UrlParams, Node, Property
from spinta.utils.url import build_url_path
from spinta import commands
from spinta.components import Namespace, Model
from spinta.utils.nestedstruct import flatten
from spinta.auth import authorized


class Html(Format):
    content_type = 'text/html'
    accept_types = {
        'text/html',
    }
    params = {}
    streamable = False


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
    headers: Optional[dict] = None,
):
    return _render_model(context, request, ns, action, params, data, headers)


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
    headers: Optional[dict] = None,
):
    return _render_model(context, request, model, action, params, data, headers)


def _render_model(
    context: Context,
    request: Request,
    model: Union[Model, Namespace],
    action: Action,
    params: UrlParams,
    data,
    http_headers,
):
    header = []
    row = []

    if model.type == 'model:ns' or params.ns:
        data = get_ns_data(data)
        header = next(data)
        data = list(data)
    elif action == Action.CHANGES:
        data = get_changes(context, data, model, params)
        header = next(data)
        data = list(reversed(list(data)))
    elif action == Action.GETONE:
        row = list(get_row(context, data, model))
        data = []
    elif action in (Action.GETALL, Action.SEARCH):
        data = get_data(context, data, model, params, action)
        header = next(data)
        data = list(data)

    templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
    return templates.TemplateResponse(
        'base.html',
        {
            **get_template_context(context, model, params),
            'request': request,
            'header': header,
            'data': data,
            'row': row,
            'formats': get_output_formats(params),
            'limit_enforced': params.limit_enforced,
            'params': params,
        },
        headers=http_headers
    )


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
    config: Config = context.get('config')
    return {
        'location': get_current_location(config, model, params),
    }


CurrentLocation = List[Tuple[
    str,            # Link title
    Optional[str],  # Link URL
]]


class PathInfo(NamedTuple):
    path: str = ''
    name: str = ''
    link: str = ''
    title: str = ''


def _split_path(
    manifest: Manifest,
    base: str,
    orig_path: str,
) -> List[PathInfo]:
    parts = orig_path.split('/') if orig_path else []
    result: List[PathInfo] = []
    last = len(parts)
    base = [base] if base else []
    for i, part in enumerate(parts, 1):
        path = '/'.join(base + parts[:i])
        if i == last and path in manifest.models:
            title = manifest.models[path].title
        elif path in manifest.namespaces:
            title = manifest.namespaces[path].title
        else:
            title = ''
        title = title or part
        result.append(PathInfo(
            path=path,
            name=part,
            link=f'/{path}',
            title=title,
        ))
    return result


def get_current_location(
    config: Config,
    model: Model,
    params: UrlParams,
) -> CurrentLocation:
    # Remove config root
    path = params.path
    if config.root:
        if path.startswith(config.root):
            path = path[len(config.root) + 1:]
        elif config.root.startswith(path):
            path = ''

    parts = _split_path(model.manifest, config.root, path)
    if len(parts) > 0:
        parts, last = parts[:-1], parts[-1]
    else:
        parts, last = [], None

    loc: CurrentLocation = [('🏠', '/')]
    loc += [(p.title, p.link) for p in parts]

    if model.type == 'model:ns':
        if last is not None:
            loc += [(last.title, None)]

    else:
        pk = params.pk
        changes = params.changes

        if last is not None:
            if pk or changes:
                loc += [(last.title, get_model_link(model))]
            else:
                loc += [(last.title, None)]

        if pk:
            pks = params.pk[:8]  # short version of primary key
            if changes:
                loc += [(pks, get_model_link(model, pk=pk))]
            else:
                loc += [(pks, None)]

        if changes:
            loc += [('Changes', None)]
        else:
            loc += [('Changes', get_model_link(model, pk=pk, changes=[]))]

    return loc


def get_changes(context: Context, rows, model, params: UrlParams):
    props = [
        p for p in model.properties.values() if (
            not p.name.startswith('_')
        )
    ]

    yield (
        ['_id', '_revision', '_txn', '_created', '_op', '_rid'] +
        [prop.name for prop in props if prop.name != 'revision']
    )

    # XXX: With large changes sets this will consume a lot of memmory.
    current = {}
    for data in rows:
        id_ = data['_rid']
        if id_ not in current:
            current[id_] = {}
        current[id_].update({
            k: v for k, v in data.items() if not k.startswith('_')
        })
        row = [
            {'color': None, 'value': data['_id'], 'link': None},
            {'color': None, 'value': data['_revision'], 'link': None},
            {'color': None, 'value': data['_txn'], 'link': None},
            {'color': None, 'value': data['_created'], 'link': None},
            {'color': None, 'value': data['_op'], 'link': None},
            get_cell(context, model.properties['_id'], data, '_rid', shorten=True),
        ]
        for prop in props:
            if prop.name in data:
                color = 'change'
            elif prop.name not in current:
                color = 'null'
            else:
                color = None
            cell = get_cell(
                context,
                prop,
                current[id_],
                prop.name,
                shorten=True,
                color=color,
            )
            row.append(cell)
        yield row


def get_row(context: Context, row, model: Node):
    if row is None:
        raise HTTPException(status_code=404)
    include = {'_type', '_id', '_revision'}
    for prop in model.properties.values():
        if (
            not prop.hidden and
            # TODO: object and array are not supported yet
            prop.dtype.name not in ('object', 'array') and
            (prop.name in include or not prop.name.startswith('_'))
        ):
            yield prop.name, get_cell(context, prop, row, prop.name)


def get_cell(
    context: Context,
    prop: Property,
    row: Dict[str, Any],
    name: Any,
    shorten=False,
    color=None,
):
    COLORS = {
        'change': '#B2E2AD',
        'null': '#C1C1C1',
    }

    link = None
    model = None
    value = row.get(name)

    if prop.name == '_id' and value:
        model = prop.model
    elif prop.dtype.name == 'ref' and prop.dtype.model and value:
        model = prop.dtype.model

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


def get_ns_data(rows):
    yield ['title', 'description']
    for row in rows:
        if row['title']:
            title = row['title']
        else:
            parts = row['_id'].split('/')
            if row['_type'] == 'ns':
                title = parts[-2]
            else:
                title = parts[-1]

        if row['_type'] == 'ns':
            icon = '📁'
            suffix = '/'
        else:
            icon = '📄'
            suffix = ''

        yield [
            {
                'value': f'{icon} {title}{suffix}',
                'link': '/' + row['_id'],
                'color': None,
            },
            {
                'value': row['description'],
                'link': None,
                'color': None,
            },
        ]


def get_data(
    context: Context,
    rows,
    model: Model,
    params: UrlParams,
    action: Action,
):
    # XXX: For things like aggregations, a dynamic model should be created with
    #      all the properties coming from aggregates.
    if params.count:
        prop = Property()
        prop.dtype = DataType()
        prop.dtype.name = 'string'
        prop.name = 'count()'
        prop.ref = None
        prop.model = model
        props = [prop]
        header = ['count()']
    else:
        if params.select:
            header = [_expr_to_name(x) for x in params.select]
            props = _get_props_from_select(context, model, header)
        else:
            include = {'_id'}
            props = [
                p for p in model.properties.values() if (
                    authorized(context, p, action) and
                    not p.hidden and
                    # TODO: object and array are not supported yet
                    p.dtype.name not in ('object', 'array') and
                    (p.name in include or not p.name.startswith('_'))
                )
            ]
            header = [p.name for p in props]

    yield header

    for data in flatten(rows):
        row = []
        for name, prop in zip(header, props):
            row.append(get_cell(context, prop, data, name, shorten=True))
        yield row


def _get_props_from_select(
    context: Context,
    model: Model,
    select: List[str],
) -> List[Property]:
    props = []
    for node in select:
        name = _expr_to_name(node)
        if name in model.flatprops:
            prop = model.flatprops[name]
        else:
            parts = name.split('.')
            prop = _find_linked_prop(context, model, parts[0], parts[1:])
        props.append(prop)
    return props


def _expr_to_name(node) -> str:
    if not isinstance(node, dict):
        return node

    name = node['name']

    if name == 'bind':
        return node['args'][0]

    if name == 'getattr':
        obj, key = node['args']
        return _expr_to_name(obj) + '.' + _expr_to_name(key)

    raise Exception(f"Unknown node {node!r}.")


def _find_linked_prop(
    context: Context,
    model: Model,
    name: str,
    parts: List[str],
) -> Union[Property, None]:
    # TODO: Add support for nested properties, now only references are
    #       supported.
    prop = model.properties.get(name)
    if parts and prop and isinstance(prop.dtype, Ref):
        model = prop.dtype.model
        return _find_linked_prop(context, model, parts[0], parts[1:])
    else:
        return prop


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

    for k, v in extra.items():
        ptree.append({
            'name': k,
            'args': v,
        })

    return ptree


def get_model_link(*args, **kwargs):
    return '/' + build_url_path(get_model_link_params(*args, **kwargs))
