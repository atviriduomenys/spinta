import dataclasses
from typing import Any
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import TypeVar
from typing import TypedDict

import pkg_resources as pres
from itertools import chain
from itertools import count
from starlette.requests import Request
from starlette.templating import Jinja2Templates

from spinta import commands
from spinta.backends.components import SelectTree
from spinta.backends.helpers import get_model_reserved_props
from spinta.backends.helpers import get_ns_reserved_props
from spinta.backends.helpers import select_model_props
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import UrlParams
from spinta.formats.components import Format
from spinta.formats.helpers import get_model_tabular_header
from spinta.formats.html.components import Cell
from spinta.formats.html.components import Color
from spinta.formats.html.components import ComplexCell
from spinta.formats.html.components import Html
from spinta.formats.html.helpers import get_model_link
from spinta.formats.html.helpers import get_output_formats
from spinta.formats.html.helpers import get_template_context
from spinta.formats.html.helpers import short_id
from spinta.types.datatype import Array
from spinta.types.datatype import DataType
from spinta.types.datatype import File
from spinta.types.datatype import Object
from spinta.types.datatype import Ref
from spinta.types.datatype import String
from spinta.utils.nestedstruct import flatten


def _get_model_reserved_props(action: Action) -> List[str]:
    if action in (Action.GETALL, Action.SEARCH):
        return ['_id']
    else:
        return get_model_reserved_props(action)


def _render_check(request: Request, data: Dict[str, Any] = None):
    if data:
        if 'errors' in data:
            result = {
                'message': "Yra klaidų",
                'errors': [
                    err['message']
                    for err in data['errors']
                ],
            }
        else:
            result = {
                'message': "Klaidų nerasta.",
                'errors': None,
            }
    else:
        result = data

    templates = Jinja2Templates(directory=pres.resource_filename('spinta', 'templates'))
    return templates.TemplateResponse('form.html', {
        'request': request,
        'title': "Duomenų struktūros aprašo tikrinimas",
        'description': (
            "Ši priemonė leidžia patikrinti ar "
            "<a href=\"https://atviriduomenys.readthedocs.io/dsa/index.html\">"
            "duomenų struktūros apraše</a> nėra klaidų."
        ),
        'name': 'check',
        'fields': [
            {
                'label': "Duomenų struktūros aprašas",
                'help': "Pateikite duomenų struktūros aprašo failą.",
                'input': '<input type="file" name="manifest" accept=".csv">'
            },
        ],
        'submit': "Tikrinti",
        'result': result,
    })


@commands.render.register(Context, Request, Namespace, Html)
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
    if action == Action.CHECK:
        return _render_check(request, data)
    else:
        return _render_model(context, request, ns, action, params, data, headers)


@commands.render.register(Context, Request, Model, Html)
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Html,
    *,
    action: Action,
    params: UrlParams,
    data: Iterator[ComplexCell],
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    if action == Action.GETONE:
        data = [data]
    return _render_model(context, request, model, action, params, data, headers)


T = TypeVar('T')


class _LimitIter(Generic[T]):
    _iterator: Iterator[T]
    limit: Optional[int]
    exhausted: bool = False

    def __init__(self, limit: Optional[int], it: Iterable[T]) -> None:
        self._iterator = iter(it)
        self._counter = count(1)
        self.limit = limit

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        try:
            value = next(self._iterator)
        except StopIteration:
            self.exhausted = True
            raise
        if self.limit is not None and next(self._counter) > self.limit:
            raise StopIteration
        return value


def _is_empty(rows: Iterator[T]) -> Iterator[T]:
    try:
        row = next(rows)
    except StopIteration:
        empty = True
    else:
        empty = False
        rows = chain([row], rows)
    return rows, empty


def _iter_values(
    header: List[str],
    rows: Iterator[Dict[str, T]]
) -> Iterator[List[T]]:
    na = Cell('', color=Color.null)
    for row in rows:
        yield [row.get(h, na) for h in header]


def _get_model_tabular_header(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
) -> List[str]:
    if model.name == '_ns':
        reserved = get_ns_reserved_props(action)
    else:
        reserved = _get_model_reserved_props(action)
    return get_model_tabular_header(
        context,
        model,
        action,
        params,
        reserved=reserved,
    )


def _render_model(
    context: Context,
    request: Request,
    model: Model,
    action: Action,
    params: UrlParams,
    rows: Iterable[Dict[str, Cell]],
    http_headers,
):
    rows = flatten(rows)

    rows, empty = _is_empty(rows)
    header = _get_model_tabular_header(context, model, action, params)
    rows = _iter_values(header, rows)

    if model.name.startswith('_'):
        rows = _LimitIter(None, rows)
    else:
        rows = _LimitIter(params.limit_enforced_to, rows)

    # Preserve response data for tests.
    if request.url.hostname == 'testserver':
        rows = list(rows)

    templates = Jinja2Templates(
        directory=pres.resource_filename('spinta', 'templates')
    )
    return templates.TemplateResponse(
        'data.html',
        {
            **get_template_context(context, model, params),
            'request': request,
            'header': header,
            'empty': empty,
            'data': rows,
            'formats': get_output_formats(params),
            'params': params,
            'zip': zip,
        },
        headers=http_headers
    )


@dataclasses.dataclass
class _NamespaceName:
    name: str

    def render(self):
        name = self.name.replace('/:ns', '/').rstrip('/').split('/')[-1]
        return f"📁 {name}/"


@dataclasses.dataclass
class _ModelName:
    name: str

    def render(self):
        name = self.name.split('/')[-1]
        return f"📄 {name}"


@commands.prepare_data_for_response.register(Context, Model, Html, dict)
def prepare_data_for_response(
    context: Context,
    model: Model,
    fmt: Html,
    value: dict,
    *,
    action: Action,
    select: SelectTree,
    prop_names: List[str],
) -> dict:
    if model.name == '_ns':
        value = value.copy()
        if value['_type'] == 'ns':
            value['name'] = _NamespaceName(value['name'])
        else:
            value['name'] = _ModelName(value['name'])

    reserved = _get_model_reserved_props(action)

    data = {
        prop.name: commands.prepare_dtype_for_response(
            context,
            prop.dtype,
            fmt,
            val,
            data=value,
            action=action,
            select=sel,
        )
        for prop, val, sel in select_model_props(
            model,
            prop_names,
            value,
            select,
            reserved,
        )
    }

    return data


@commands.prepare_dtype_for_response.register(Context, String, Html, _NamespaceName)
def prepare_dtype_for_response(
    context: Context,
    dtype: String,
    fmt: Html,
    value: _NamespaceName,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value.render(), link=get_model_link(value))


@commands.prepare_dtype_for_response.register(Context, String, Html, _ModelName)
def prepare_dtype_for_response(
    context: Context,
    dtype: String,
    fmt: Html,
    value: _ModelName,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value.render(), link=get_model_link(value))


@commands.prepare_dtype_for_response.register(Context, DataType, Html, type(None))
def prepare_dtype_for_response(
    context: Context,
    dtype: DataType,
    fmt: Html,
    value: None,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell('', color=Color.null)


@commands.prepare_dtype_for_response.register(Context, DataType, Html, object)
def prepare_dtype_for_response(
    context: Context,
    dtype: DataType,
    fmt: Html,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if dtype.prop.name == '_id':
        return Cell(short_id(value), link=get_model_link(
            dtype.prop.model,
            pk=value,
        ))
    return Cell(value)


class _RefValue(TypedDict):
    _id: str


@commands.prepare_dtype_for_response.register(Context, Ref, Html, dict)
def prepare_dtype_for_response(
    context: Context,
    dtype: Ref,
    fmt: Html,
    value: _RefValue,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(short_id(value['_id']), link=get_model_link(
        dtype.model,
        pk=value['_id'],
    ))


@commands.prepare_dtype_for_response.register(Context, File, Html, dict)
def prepare_dtype_for_response(
    context: Context,
    dtype: File,
    fmt: Html,
    value: Dict[str, Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value['_id'], link=get_model_link(
        dtype.prop.model,
        pk=data['_id'],
        prop=dtype.prop.name,
    ))


@commands.prepare_dtype_for_response.register(Context, Object, Html, dict)
def prepare_dtype_for_response(
    context: Context,
    dtype: Object,
    fmt: Html,
    value: Dict[str, Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return commands.prepare_dtype_for_response[Context, Object, Format, dict](
        context,
        dtype,
        fmt,
        value,
        data=data,
        action=action,
        select=select,
    )


@commands.prepare_dtype_for_response.register(Context, Array, Html, list)
def prepare_dtype_for_response(
    context: Context,
    dtype: Array,
    fmt: Html,
    value: List[Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return commands.prepare_dtype_for_response[Context, Array, Format, list](
        context,
        dtype,
        fmt,
        value,
        data=data,
        action=action,
        select=select,
    )
