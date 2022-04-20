import dataclasses
import datetime
from decimal import Decimal
from typing import Any
from typing import Dict
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import TypeVar

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
from spinta.types.datatype import Date
from spinta.types.datatype import Time
from spinta.types.datatype import DateTime
from spinta.types.datatype import Number
from spinta.types.datatype import Binary
from spinta.types.datatype import JSON
from spinta.utils.nestedstruct import flatten
from spinta.utils.schema import NotAvailable


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


def _build_template_context(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    rows: Iterable[Dict[str, Cell]],
):
    rows = flatten(rows)

    rows, empty = _is_empty(rows)
    header = _get_model_tabular_header(context, model, action, params)
    rows = _iter_values(header, rows)

    if model.name.startswith('_'):
        rows = _LimitIter(None, rows)
    else:
        rows = _LimitIter(params.limit_enforced_to, rows)

    return {
        'header': header,
        'empty': empty,
        'data': rows,
        'params': params,
    }


def _render_model(
    context: Context,
    request: Request,
    model: Model,
    action: Action,
    params: UrlParams,
    rows: Iterable[Dict[str, Cell]],
    http_headers,
):
    ctx = _build_template_context(
        context,
        model,
        action,
        params,
        rows,
    )
    ctx.update(get_template_context(context, model, params))
    ctx['request'] = request
    ctx['formats'] = get_output_formats(params)

    # Pass function references
    ctx['zip'] = zip

    # Preserve response data for tests.
    if request.url.hostname == 'testserver':
        ctx['data'] = list(ctx['data'])

    templates = Jinja2Templates(
        directory=pres.resource_filename('spinta', 'templates')
    )
    return templates.TemplateResponse('data.html', ctx, headers=http_headers)


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
            fmt,
            prop.dtype,
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


@commands.prepare_dtype_for_response.register(Context, Html, String, _NamespaceName)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: String,
    value: _NamespaceName,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value.render(), link=get_model_link(value))


@commands.prepare_dtype_for_response.register(Context, Html, String, _ModelName)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: String,
    value: _ModelName,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value.render(), link=get_model_link(value))


@commands.prepare_dtype_for_response.register(Context, Html, DataType, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: DataType,
    value: None,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell('', color=Color.null)


@commands.prepare_dtype_for_response.register(Context, Html, DataType, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: DataType,
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


@commands.prepare_dtype_for_response.register(Context, Html, Date, datetime.date)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Date,
    value: datetime.date,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value.isoformat())


@commands.prepare_dtype_for_response.register(Context, Html, Time, datetime.time)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Time,
    value: datetime.time,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value.isoformat())


@commands.prepare_dtype_for_response.register(Context, Html, DateTime, datetime.datetime)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: DateTime,
    value: datetime.datetime,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value.isoformat())


@commands.prepare_dtype_for_response.register(Context, Html, Number, Decimal)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Number,
    value: Decimal,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value)


@commands.prepare_dtype_for_response.register(Context, Html, Binary, bytes)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Binary,
    value: bytes,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value)


@commands.prepare_dtype_for_response.register(Context, Html, JSON, (object, NotAvailable, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: JSON,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell(value)


@commands.prepare_dtype_for_response.register(Context, Html, Array, tuple)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Array,
    value: tuple,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, Array, tuple]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, Array, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Array,
    value: None,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, Array, type(None)]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, Ref, (dict, str, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: File,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, Ref, dict]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, File, NotAvailable)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: File,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, File, NotAvailable]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, File, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: File,
    value: Dict[str, Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if value['_id'] is None:
        _id = Cell('', color=Color.null)
    else:
        _id = Cell(value['_id'], link=get_model_link(
            dtype.prop.model,
            pk=data['_id'],
            prop=dtype.prop.name,
        ))
    if value['_content_type'] is None:
        _content_type = Cell('', color=Color.null)
    else:
        _content_type = Cell(value['_content_type'])
    return {
        '_id': _id,
        '_content_type': _content_type,
    }


@commands.prepare_dtype_for_response.register(Context, Html, Object, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Object,
    value: Dict[str, Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return commands.prepare_dtype_for_response[Context, Format, Object, dict](
        context,
        fmt,
        dtype,
        value,
        data=data,
        action=action,
        select=select,
    )


@commands.prepare_dtype_for_response.register(Context, Html, Array, list)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Array,
    value: List[Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return commands.prepare_dtype_for_response[Context, Format, Array, list](
        context,
        fmt,
        dtype,
        value,
        data=data,
        action=action,
        select=select,
    )
