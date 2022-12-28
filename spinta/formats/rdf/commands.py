import dataclasses
import datetime
from decimal import Decimal
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from lxml import etree
from lxml.etree import Element, QName, SubElement
from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta import commands
from spinta.backends.components import SelectTree
from spinta.backends.helpers import get_model_reserved_props
from spinta.backends.helpers import select_model_props
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.components import UrlParams
from spinta.dimensions.prefix.components import UriPrefix
from spinta.formats.components import Format
from spinta.formats.html.helpers import get_model_link_params
from spinta.formats.rdf.components import Rdf, Cell
from spinta.types.datatype import DataType
from spinta.types.datatype import File
from spinta.types.datatype import Ref
from spinta.types.datatype import String
from spinta.types.datatype import Date
from spinta.types.datatype import Time
from spinta.types.datatype import DateTime
from spinta.types.datatype import Number
from spinta.utils.schema import NotAvailable
from spinta.utils.url import build_url_path

RDF = "rdf"


def _get_model_link(*args, **kwargs):
    return build_url_path(get_model_link_params(*args, **kwargs))


def _add_tree_elem(
    request: Request,
    elem,
    parent: Element,
    name: str,
    prefix: str,
    available_prefixes: dict
):
    if prefix and available_prefixes.get(prefix):
        tree_elem = SubElement(parent, QName(available_prefixes.get(prefix), name))
    else:
        tree_elem = SubElement(parent, name)

    if isinstance(elem, Cell):
        tree_elem.text = str(elem.value)
        if elem.about:
            if available_prefixes.get(RDF):
                about = QName(available_prefixes.get(RDF), 'about')
            else:
                about = 'about'
            tree_elem.attrib[about] = f'{request.base_url}{elem.about}'
    else:
        for key, val in elem.items():
            if isinstance(val, Cell):
                prefix = val.prefix
            else:
                prefix = val.pop('prefix', None)
            _add_tree_elem(request, val, tree_elem, key, prefix, available_prefixes)


def _get_prefix(uri):
    prefix = None
    if uri and ':' in uri:
        prefix = uri.split(':')[0]
    return prefix


@commands.render.register(Context, Request, Model, Rdf)
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Rdf,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    headers = headers or {}
    headers['Content-Disposition'] = f'attachment; filename="{model.basename}.rdf"'

    if model.manifest.datasets.get(model.ns.name):
        prefixes = model.manifest.datasets.get(model.ns.name).prefixes
        for key, val in prefixes.items():
            if isinstance(val, UriPrefix):
                prefixes[key] = val.uri
    else:
        prefixes = {}

    if prefixes.get(RDF):
        root = Element(QName(prefixes.get(RDF), RDF.capitalize()), nsmap=prefixes)
    else:
        root = Element(RDF.capitalize(), nsmap=prefixes)

    if action == Action.GETONE:
        _add_tree_elem(
            request,
            data,
            root,
            model.basename,
            _get_prefix(model.uri),
            prefixes
        )
    else:
        for elem in data:
            _add_tree_elem(
                request,
                elem,
                root,
                model.basename,
                _get_prefix(model.uri),
                prefixes
            )

    return StreamingResponse(
        _stream(root),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )


async def _stream(root):
    yield etree.tostring(root, pretty_print=True)


@dataclasses.dataclass
class _NamespaceName:
    name: str

    def render(self):
        name = self.name.replace('/:ns', '/').rstrip('/').split('/')[-1]
        return f"{name}"


@dataclasses.dataclass
class _ModelName:
    name: str

    def render(self):
        name = self.name.split('/')[-1]
        return f"{name}"


@commands.prepare_data_for_response.register(Context, Model, Rdf, dict)
def prepare_data_for_response(
    context: Context,
    model: Model,
    fmt: Rdf,
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

    reserved = get_model_reserved_props(action)

    data = {
        prop.name: commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=value,
            action=action,
            select=sel,
            prefix=_get_prefix(prop.uri)
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


@commands.prepare_dtype_for_response.register(Context, Rdf, String, _NamespaceName)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: String,
    value: _NamespaceName,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    return Cell(value.render(), about=_get_model_link(value), prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, String, _ModelName)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: String,
    value: _ModelName,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    return Cell(value.render(), about=_get_model_link(value), prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, DataType, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: DataType,
    value: None,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    return Cell('', prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, DataType, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: DataType,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    if dtype.prop.name == '_id':
        return Cell(value, about=_get_model_link(
            dtype.prop.model,
            pk=value,
        ))
    return Cell(value, prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, Date, datetime.date)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Date,
    value: datetime.date,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    return Cell(value.isoformat(), prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, Time, datetime.time)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Time,
    value: datetime.time,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    return Cell(value.isoformat(), prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, DateTime, datetime.datetime)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: DateTime,
    value: datetime.datetime,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    return Cell(value.isoformat(), prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, Number, Decimal)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Number,
    value: Decimal,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    return Cell(value, prefix=prefix)


@commands.prepare_dtype_for_response.register(Context, Rdf, Ref, (dict, str, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: File,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    super_ = commands.prepare_dtype_for_response[Context, Format, Ref, dict]
    data = super_(context, fmt, dtype, value, data=data, action=action, select=select)
    data['prefix'] = prefix
    return data


@commands.prepare_dtype_for_response.register(Context, Rdf, File, NotAvailable)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: File,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    super_ = commands.prepare_dtype_for_response[Context, Format, File, NotAvailable]
    return super_(context, fmt, dtype, value, data=data, action=action, select=select)


@commands.prepare_dtype_for_response.register(Context, Rdf, File, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: File,
    value: Dict[str, Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
    prefix: str = None
):
    if value['_id'] is None:
        _id = Cell('', prefix=prefix)
    else:
        _id = Cell(value['_id'], about=_get_model_link(
            dtype.prop.model,
            pk=data['_id'],
            prop=dtype.prop.name,
        ), prefix=prefix)
    if value['_content_type'] is None:
        _content_type = Cell('', prefix=prefix)
    else:
        _content_type = Cell(value['_content_type'], prefix=prefix)
    return {
        '_id': _id,
        '_content_type': _content_type,
        'prefix': prefix
    }
