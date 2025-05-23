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

from itertools import chain
from itertools import count
from starlette.requests import Request
from starlette.templating import Jinja2Templates

from shapely.geometry.base import BaseGeometry

from spinta import commands
from spinta.backends.components import SelectTree
from spinta.backends.helpers import get_model_reserved_props, get_select_prop_names, select_props
from spinta.backends.helpers import get_ns_reserved_props
from spinta.backends.helpers import select_model_props
from spinta.backends.postgresql.types.geometry.helpers import get_display_value, get_osm_link
from spinta.components import pagination_enabled, page_in_data
from spinta.core.enums import Action
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
from spinta.types.datatype import Array, ExternalRef, PageType, BackRef, ArrayBackRef, Denorm
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
from spinta.types.datatype import Inherit
from spinta.types.datatype import UUID
from spinta.types.geometry.components import Geometry
from spinta.types.text.components import Text
from spinta.utils.encoding import is_url_safe_base64, encode_page_values
from spinta.utils.nestedstruct import flatten, sepgetter
from spinta.utils.path import resource_filename
from spinta.utils.schema import NotAvailable
from spinta.utils.url import build_url_path


def _get_model_reserved_props(action: Action, include_page: bool) -> List[str]:
    if action == Action.GETALL:
        reserved = ['_id']
    elif action == action.SEARCH:
        reserved = ['_id', '_base']
    else:
        return get_model_reserved_props(action, include_page)
    if include_page:
        reserved.append('_page')
    return reserved


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

    templates = Jinja2Templates(directory=str(resource_filename('spinta', 'templates')))
    return templates.TemplateResponse( 'form.html', {
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
    last_page: Any = None
    header_page_id: int = None
    params: UrlParams = None

    def __init__(self, limit: Optional[int], it: Iterable[T], header: list = None, params: UrlParams = None) -> None:
        self._iterator = iter(it)
        self._counter = count(1)
        self.limit = limit
        self.params = params
        if header and '_page' in header:
            self.header_page_id = header.index('_page')

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
        if self.header_page_id is not None and self.params is not None:
            page = value[self.header_page_id]
            self.last_page = '/' + build_url_path(
                self.params.changed_parsetree({
                    "page": [page.value]
                })
            )
            value.remove(page)
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
        reserved = _get_model_reserved_props(action, pagination_enabled(model, params))
    return get_model_tabular_header(
        context,
        model,
        action,
        params,
        reserved=reserved,
    )


def build_template_context(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    rows: Iterable[Dict[str, Cell]],
):
    rows = flatten(rows, sepgetter(model))

    rows, empty = _is_empty(rows)
    header = _get_model_tabular_header(context, model, action, params)
    rows = _iter_values(header, rows)

    if model.name.startswith('_'):
        rows = _LimitIter(None, rows, header, params)
    else:
        rows = _LimitIter(params.limit_enforced_to, rows, header, params)

    # Remove page from header, since it now works as next page link
    if '_page' in header:
        header = header.copy()
        header.remove('_page')

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
    ctx = build_template_context(
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
        directory=str(resource_filename('spinta', 'templates'))
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

    reserved = _get_model_reserved_props(action, page_in_data(value))

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
    link = data.pop('_link', True)
    if dtype.prop.name == '_id' and link:
        return Cell(short_id(value), link=get_model_link(
            dtype.prop.model,
            pk=value,
        ))
    return Cell(value)

@commands.prepare_dtype_for_response.register(Context, Html, UUID, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: UUID,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    link = data.pop('_link', True)
    if dtype.prop.name == '_id' and link:
        return Cell(short_id(str(value)), link=get_model_link(
            dtype.prop.model,
            pk=value,
        ))
    return Cell(str(value))


@commands.prepare_dtype_for_response.register(Context, Html, UUID, (NotAvailable, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: UUID,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if value is None:
        return Cell('', color=Color.null)

    super_ = commands.prepare_dtype_for_response[Context, Format, UUID, NotAvailable]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


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
    if is_url_safe_base64(value):
        value = value.decode('ascii')
    return Cell(value)


@commands.prepare_dtype_for_response.register(Context, Html, PageType, list)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: PageType,
    value: list,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    value = encode_page_values(value).decode('ascii')
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
    super_ = commands.prepare_dtype_for_response[Context, Format, Ref, type(value)]
    value = super_(context, fmt, dtype, value, data=data, action=action, select=select)
    if value is None:
        return Cell('', color=Color.null)
    return value


@commands.prepare_dtype_for_response.register(Context, Html, ExternalRef, (dict, str, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: ExternalRef,
    value: Optional[Dict[str, Any]],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if value is None:
        return Cell('', color=Color.null)

    props = dtype.properties.copy()
    props.update(dtype.model.properties)

    if select and select != {'*': {}}:
        names = get_select_prop_names(
            context,
            dtype,
            props,
            action,
            select,
        )
    else:
        names = list(value.keys())
    if not isinstance(names, list):
        names = list(names)
    data = {}
    for prop, val, sel in select_props(
        dtype.model,
        names,
        props,
        value,
        select,
    ):
        if '_id' in value:
            value.update({
                '_link': False
            })
        data[prop.name] = commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=value,
            action=action,
            select=sel,
        )
    return data


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


@commands.prepare_dtype_for_response.register(Context, Html, Inherit, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Inherit,
    value: List[Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    res = commands.prepare_dtype_for_response[Context, Format, Inherit, dict](
        context,
        fmt,
        dtype,
        value,
        data=data,
        action=action,
        select=select,
    )
    return res


@commands.prepare_dtype_for_response.register(Context, Html, Inherit, (NotAvailable, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Inherit,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if value is None:
        return Cell('', color=Color.null)

    super_ = commands.prepare_dtype_for_response[Context, Format, File, NotAvailable]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, ArrayBackRef, tuple)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: ArrayBackRef,
    value: tuple,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, ArrayBackRef, tuple]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, ArrayBackRef, list)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: ArrayBackRef,
    value: list,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, ArrayBackRef, list]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, ArrayBackRef, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: ArrayBackRef,
    value: type(None),
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, ArrayBackRef, type(None)]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Html, BackRef, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: BackRef,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, BackRef, dict]
    value = super_(context, fmt, dtype, value, data=data, action=action, select=select)
    if value is None:
        return Cell('', color=Color.null)
    return value


@commands.prepare_dtype_for_response.register(Context, Html, BackRef, str)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: BackRef,
    value: str,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, BackRef, str]
    value = super_(context, fmt, dtype, value, data=data, action=action, select=select)
    return value


@commands.prepare_dtype_for_response.register(Context, Html, BackRef, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: BackRef,
    value: type(None),
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return Cell('', color=Color.null)


@commands.prepare_dtype_for_response.register(Context, Html, Text, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Text,
    value: Dict[str, str],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if len(value) == 1 and select is not None:
        for key, data in value.items():
            if key not in select.keys():
                return _value_or_null(data)

    return {
        k: _value_or_null(v)
        for k, v in value.items()
    }


@commands.prepare_dtype_for_response.register(Context, Html, Geometry, BaseGeometry)
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Geometry,
    value: BaseGeometry,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    display_value = get_display_value(value)
    osm_link = get_osm_link(value, dtype)
    return Cell(display_value, link=osm_link)


@commands.prepare_dtype_for_response.register(Context, Html, Denorm, (object, type(None), NotAvailable))
def prepare_dtype_for_response(
    context: Context,
    fmt: Html,
    dtype: Denorm,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return commands.prepare_dtype_for_response(
        context,
        fmt,
        dtype.rel_prop,
        value,
        data=data,
        action=action,
        select=select
    )


def _value_or_null(value):
    return Cell(value) if value is not None else Cell('', color=Color.null)
