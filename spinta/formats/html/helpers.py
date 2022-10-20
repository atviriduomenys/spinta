import datetime
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import TypedDict

from spinta.components import Config
from spinta.components import Context
from spinta.components import Model
from spinta.components import Node
from spinta.components import Property
from spinta.components import UrlParams
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Color
from spinta.manifests.components import Manifest
from spinta.types.datatype import File
from spinta.types.datatype import Ref
from spinta.utils.url import build_url_path

CurrentLocation = List[Tuple[
    str,            # Link title
    Optional[str],  # Link URL
]]


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

    if model.name == '_ns':
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
            loc += [('Changes', get_model_link(model, pk=pk, changes=[-10]))]

    return loc


def short_id(value: Optional[str]) -> Optional[str]:
    if value is not None:
        return value[:8]


def get_cell(
    context: Context,
    prop: Property,
    pk: Optional[str],
    row: Dict[str, Any],
    name: Any,
    *,
    shorten=False,
    color: Optional[Color] = None,
) -> Cell:
    link = None
    model = None
    if prop.dtype.name == 'ref':
        value = row.get(f'{name}._id')
    elif isinstance(prop.dtype, File):
        # XXX: In listing, row is flattened, in single object view row is
        #      nested, because of that, we need to check both cases here.
        value = row.get(f'{name}._id') or row.get(name, {}).get('_id')
        if pk:
            # Primary key might not be given in select, for example
            # select(count()).
            link = '/' + build_url_path(
                get_model_link_params(prop.model, pk=pk, prop=prop.place)
            )
    else:
        value = row.get(name)

    if prop.name == '_id' and value:
        model = prop.model
    elif isinstance(prop.dtype, Ref) and prop.dtype.model and value:
        model = prop.dtype.model

    if model:
        link = '/' + build_url_path(get_model_link_params(model, pk=value))

    if prop.dtype.name in ('ref', 'pk') and shorten and isinstance(value, str):
        value = short_id(value)

    if isinstance(value, datetime.datetime):
        value = value.isoformat()

    max_column_length = 200
    if shorten and isinstance(value, str) and len(value) > max_column_length:
        value = value[:max_column_length] + '...'

    if value is None:
        value = ''
        if color is None:
            color = Color.null

    return Cell(value, link, color)


def get_ns_data(rows) -> Iterator[List[Cell]]:
    yield [
        Cell('title'),
        Cell('description'),
    ]
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
            Cell(f'{icon} {title}{suffix}', link='/' + row['_id']),
            Cell(row['description']),
        ]


class _ParsedNode(TypedDict):
    name: str
    args: List[Any]


def get_model_link_params(
    model: Node,
    *,
    pk: Optional[str] = None,
    prop: Optional[str] = None,
    **extra,
) -> List[_ParsedNode]:
    assert prop is None or (prop and pk), (
        "If prop is given, pk must be given too."
    )

    ptree = [
        {
            'name': 'path',
            'args': (
                model.name.split('/') +
                ([pk] if pk is not None else []) +
                ([prop] if prop is not None else [])
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


def get_template_context(context: Context, model, params: UrlParams):
    config: Config = context.get('config')
    return {
        'location': get_current_location(config, model, params),
    }


def _get_output_format_link(fmt: str, params: UrlParams) -> str:
    query = ''
    change = {'format': [fmt]}

    if params.changes:
        params.changes_offset = -10
        query = 'limit(100)'

    params = params.changed_parsetree(change)
    link = '/' + build_url_path(params)
    if query:
        link = link + '?' + query
    return link


def get_output_formats(params: UrlParams):
    links = [
        ('CSV', 'csv'),
        ('JSON', 'json'),
        ('JSONL', 'jsonl'),
        ('ASCII', 'ascii'),
    ]
    return [
        (label, _get_output_format_link(fmt, params))
        for label, fmt in links
    ]
