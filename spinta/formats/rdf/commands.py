import base64
import datetime

from decimal import Decimal
from typing import Any, Union, Tuple
from typing import Dict
from typing import List
from typing import Optional

from lxml import etree
from lxml.etree import Element, QName
from starlette.requests import Request
from starlette.responses import StreamingResponse

from shapely.geometry.base import BaseGeometry

from geoalchemy2.elements import WKBElement

from spinta import commands
from spinta.backends.components import SelectTree
from spinta.backends.helpers import get_model_reserved_props
from spinta.backends.helpers import select_model_props
from spinta.backends.helpers import get_select_prop_names, select_props
from spinta.components import Action, Namespace, page_in_data
from spinta.components import Context
from spinta.components import Model
from spinta.components import UrlParams
from spinta.dimensions.prefix.components import UriPrefix
from spinta.exceptions import DuplicateRdfPrefixMissmatch
from spinta.formats.components import Format
from spinta.formats.html.helpers import get_model_link
from spinta.formats.rdf.components import Rdf
from spinta.types.datatype import DataType, PageType
from spinta.types.datatype import File
from spinta.types.datatype import Ref
from spinta.types.datatype import Date
from spinta.types.datatype import Time
from spinta.types.datatype import DateTime
from spinta.types.datatype import Number
from spinta.types.datatype import ExternalRef, Array, BackRef, ArrayBackRef, Inherit, JSON, UUID, Object, Binary
from spinta.types.geometry.components import Geometry
from spinta.types.text.components import Text
from spinta.utils.encoding import encode_page_values
from spinta.utils.schema import NotAvailable

RDF = "rdf"
PAV = "pav"
XML = "xml"
DESCRIPTION = "Description"

DEFAULT_PREFIXES = {
    RDF: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    PAV: "http://purl.org/pav/",
    XML: "http://www.w3.org/XML/1998/namespace"
}


def _get_available_prefixes(context: Context, model: Model) -> dict:
    prefixes = DEFAULT_PREFIXES.copy()
    if commands.has_dataset(context, model.manifest, model.ns.name):
        manifest_prefixes = commands.get_dataset(context, model.manifest, model.ns.name).prefixes
        for key, val in manifest_prefixes.items():
            if isinstance(val, UriPrefix):
                prefixes[key] = val.uri
    return prefixes


def _get_update_prefixes(prefixes: dict, used_datasets: list, model: Model) -> dict:
    if model.external and model.external.dataset and model.external.dataset.name not in used_datasets:
        manifest_prefixes = model.external.dataset.prefixes
        for key, val in manifest_prefixes.items():
            if isinstance(val, UriPrefix):
                if key in prefixes and prefixes[key] != val.uri:
                    raise DuplicateRdfPrefixMissmatch(model.external.dataset, prefix=key, old_value=prefixes[key], new_value=val.uri)
                prefixes[key] = val.uri
        used_datasets.append(model.external.dataset.name)
    return prefixes


def _update_required_prefixes(prefixes: dict) -> dict:
    updated_prefixes = DEFAULT_PREFIXES.copy()
    updated_prefixes.update(prefixes)
    return updated_prefixes


def _get_attribute_name(
    name: str,
    prefix: str,
    available_prefixes: dict
) -> Union[str, QName]:
    if prefix and available_prefixes.get(prefix):
        attribute_name = QName(available_prefixes.get(prefix), name)
    else:
        attribute_name = name
    return attribute_name


def _get_attributes(
    model: Model,
    data: dict,
    about_name: Union[str, QName],
    type_name: Union[str, QName],
    revision_name: Union[str, QName],
) -> Tuple[dict, dict]:
    attributes = {}
    uri_prop = model.uri_prop
    about_uri = False
    if uri_prop is not None:
        if uri_prop.name in data:
            uri = data.pop(uri_prop.name)
            if uri is not None and uri.text:
                attributes[about_name] = uri.text
                about_uri = True
    if '_id' in data:
        _id = data.pop('_id')
        if not about_uri and _id is not None and _id.text:
            attributes[about_name] = get_model_link(model, pk=_id.text)
    if '_type' in data:
        _type = data.pop('_type')
        if _type is not None and _type.text:
            attributes[type_name] = _type.text
    if '_revision' in data:
        _revision = data.pop('_revision')
        if _revision is not None and _revision.text:
            attributes[revision_name] = _revision.text
    return data, attributes


def _get_prefix_and_name(uri: str) -> Tuple[str, str]:
    prefix = name = None
    if uri and ':' in uri:
        prefix = uri.split(':')[0]
        name = uri.split(':')[1]
    elif uri:
        name = uri
    return prefix, name


def _create_element(
    name: Union[str, QName],
    text: str = None,
    base: str = None,
    nsmap: dict = None,
    attributes: dict = None,
    children: List[Element] = None,
) -> Element:
    if attributes is None:
        attributes = {}
    if children is None:
        children = []

    if nsmap is not None:
        elem = Element(name, nsmap=nsmap)
    else:
        elem = Element(name)

    if base is not None:
        elem.base = base
    if text is not None:
        elem.text = text
    for key, value in attributes.items():
        elem.attrib[key] = value
    for child in children:
        if child is not None:
            if isinstance(child, list):
                for item in child:
                    if item is not None:
                        elem.append(item)
            else:
                elem.append(child)
    return elem


def _create_model_element(
    model: Model,
    prefixes: dict,
    data: dict
) -> Element:
    about_name = _get_attribute_name('about', RDF, prefixes)
    type_name = _get_attribute_name('type', RDF, prefixes)
    revision_name = _get_attribute_name('version', PAV, prefixes)
    model_prefix, model_name = _get_prefix_and_name(model.uri)

    data, attributes = _get_attributes(
        model,
        data,
        about_name,
        type_name,
        revision_name
    )
    if model_name:
        name = _get_attribute_name(model_name, model_prefix, prefixes)
    else:
        name = _get_attribute_name(model.basename, model_prefix, prefixes)
    if not isinstance(name, QName):
        name = _get_attribute_name(DESCRIPTION, RDF, prefixes)
        attributes[type_name] = model.model_type()

    return _create_element(
        name=name,
        attributes=attributes,
        children=list(data.values()),
        nsmap=prefixes
    )


def _prepare_for_print(
    model: Model,
    prefixes: dict,
    data: dict
) -> str:
    elem = _create_model_element(model, prefixes, data)
    res = etree.tostring(elem, encoding="unicode", pretty_print=True)
    for key, val in elem.nsmap.items():
        res = res.replace(f'xmlns:{key}="{val}" ', "")
    return res


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

    return StreamingResponse(
        _stream(
            context,
            request,
            model,
            action,
            data
        ),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )


@commands.render.register(Context, Request, Namespace, Rdf)
def render(
    context: Context,
    request: Request,
    ns: Namespace,
    fmt: Rdf,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    headers = headers or {}
    headers['Content-Disposition'] = f'attachment; filename="{ns.basename}.rdf"'
    return StreamingResponse(
        _stream_namespace(
            context,
            request,
            ns,
            action,
            data
        ),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )


async def _stream(
    context: Context,
    request: Request,
    model: Model,
    action: Action,
    data
):
    namespaces = []
    prefixes = _get_available_prefixes(context, model)
    root_name = _get_attribute_name(RDF.upper(), RDF, prefixes)
    for key, val in prefixes.items():
        namespaces.append(f'xmlns:{key}="{val}"')
    if request.base_url:
        namespaces.append(f'xmlns="{str(request.base_url)}"')
    if isinstance(root_name, QName):
        root_name = f"{RDF}:{root_name.localname}"
    namespaces = "\n ".join(namespaces)

    yield f'<?xml version="1.0" encoding="UTF-8"?>\n<{root_name}\n {namespaces}>\n'
    if action == Action.GETONE:
        yield _prepare_for_print(model, prefixes, data)
    else:
        for row in data:
            yield _prepare_for_print(model, prefixes, row)
    yield f"</{root_name}>\n"


async def _stream_namespace(
    context: Context,
    request: Request,
    ns: Namespace,
    action: Action,
    data
):
    namespaces = []
    models = commands.traverse_ns_models(
        context,
        ns,
        ns.manifest,
        action,
        internal=True,
    )
    prefixes = {}
    used_datasets = []
    model_mapper = {}
    for model in models:
        prefixes = _get_update_prefixes(prefixes, used_datasets, model)
        model_mapper[model.name] = model
    prefixes = _update_required_prefixes(prefixes)

    root_name = _get_attribute_name(RDF.upper(), RDF, prefixes)
    for key, val in prefixes.items():
        namespaces.append(f'xmlns:{key}="{val}"')
    if request.base_url:
        namespaces.append(f'xmlns="{str(request.base_url)}"')
    if isinstance(root_name, QName):
        root_name = f"{RDF}:{root_name.localname}"
    namespaces = "\n ".join(namespaces)

    yield f'<?xml version="1.0" encoding="UTF-8"?>\n<{root_name}\n {namespaces}>\n'

    # TODO: Temporarily disabled page return, because page does not work with namespace/:all
    if action == Action.GETONE:
        model = model_mapper[data['_type'].text]
        if '_page' in data:
            data.pop('_page')
        yield _prepare_for_print(model, prefixes, data)
    else:
        for row in data:
            if '_page' in row:
                row.pop('_page')
            model = model_mapper[row['_type'].text]
            yield _prepare_for_print(model, prefixes, row)
    yield f"</{root_name}>\n"


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
    value = value.copy()
    reserved = get_model_reserved_props(action, page_in_data(value))

    available_prefixes = _get_available_prefixes(context, model)

    value['_available_prefixes'] = available_prefixes
    value['_about_name'] = _get_attribute_name('about', RDF, available_prefixes)
    value['_resource_name'] = _get_attribute_name('resource', RDF, available_prefixes)
    value['_type_name'] = _get_attribute_name('type', RDF, available_prefixes)
    value['_revision_name'] = _get_attribute_name('version', PAV, available_prefixes)

    data = {}
    for prop, val, sel in select_model_props(
        model,
        prop_names,
        value,
        select,
        reserved,
    ):
        prefix, name = _get_prefix_and_name(prop.uri)
        if name:
            value['_elem_name'] = _get_attribute_name(name, prefix, available_prefixes)
        else:
            value['_elem_name'] = _get_attribute_name(prop.name, prefix, available_prefixes)

        elem = commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=value,
            action=action,
            select=sel
        )
        data[prop.name] = elem
    return data


@commands.prepare_dtype_for_response.register(Context, Rdf, DataType, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: DataType,
    value: None,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return None


@commands.prepare_dtype_for_response.register(Context, Rdf, DataType, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: DataType,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, PageType, list)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: PageType,
    value: list,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=encode_page_values(value).decode('ascii')
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Date, datetime.date)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Date,
    value: datetime.date,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=value.isoformat()
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Time, datetime.time)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Time,
    value: datetime.time,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=value.isoformat()
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, DateTime, datetime.datetime)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: DateTime,
    value: datetime.datetime,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=value.isoformat()
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Number, Decimal)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Number,
    value: Decimal,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Ref, (dict, str, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Ref,
    value,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    super_ = commands.prepare_dtype_for_response[Context, Format, Ref, type(value)]
    data_dict = super_(context, fmt, dtype, value, data=data, action=action, select=select)
    prefixes = data['_available_prefixes']
    attributes = {}
    children = []
    if data_dict is None:
        return None

    if len(data_dict) == 1 and '_id' in data_dict or '_uri' in data_dict:
        if '_uri' in data_dict:
            attributes[data['_resource_name']] = data_dict['_uri'].text
        else:
            attributes[data['_resource_name']] = get_model_link(
                dtype.model,
                pk=data_dict['_id'].text
            )
    else:
        # TODO: write tests for denormalized form after task #216 is done

        if isinstance(data['_elem_name'], QName):
            prefix, name = _get_prefix_and_name(dtype.model.uri)
            if name:
                ref_model_name = _get_attribute_name(name, prefix, prefixes)
            else:
                ref_model_name = _get_attribute_name(dtype.model.basename, prefix, prefixes)
            data_dict, ref_model_attrs = _get_attributes(
                dtype.model,
                data_dict,
                data['_about_name'],
                data['_type_name'],
                data['_revision_name'],
            )
            ref_model_elem = _create_element(
                name=ref_model_name,
                attributes=ref_model_attrs,
                children=list(data_dict.values())
            )
            children.append(ref_model_elem)
        else:
            data_dict['_type'] = _create_element(
                name='_type',
                text=dtype.model.model_type()
            )
            data_dict, description_attrs = _get_attributes(
                dtype.model,
                data_dict,
                data['_about_name'],
                data['_type_name'],
                data['_revision_name'],
            )
            description_elem = _create_element(
                name=_get_attribute_name(DESCRIPTION, RDF, prefixes),
                attributes=description_attrs,
                children=list(data_dict.values())
            )
            children.append(description_elem)

    if attributes or children:
        return _create_element(
            name=data['_elem_name'],
            attributes=attributes,
            children=children
        )
    else:
        return None


@commands.prepare_dtype_for_response.register(Context, Rdf, File, NotAvailable)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: File,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return None


@commands.prepare_dtype_for_response.register(Context, Rdf, File, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: File,
    value: Dict[str, Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    if value['_id'] is not None:
        attributes = {
            data['_about_name']: get_model_link(
                dtype.prop.model,
                pk=data['_id'],
                prop=dtype.prop.name
            )
        }
        return _create_element(
            name=data['_elem_name'],
            attributes=attributes
        )
    else:
        return None


@commands.prepare_dtype_for_response.register(Context, Rdf, Text, str)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Text,
    value: str,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Text, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Text,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    result = []
    for key, item in value.items():
        if item is not None:
            prefixes = data['_available_prefixes']
            attributes = None
            if key != "C":
                attributes = {
                    _get_attribute_name("lang", XML, prefixes): key
                }
            result.append(_create_element(
                name=data['_elem_name'],
                attributes=attributes,
                text=str(item)
            ))
    return result


@commands.prepare_dtype_for_response.register(Context, Rdf, ExternalRef, (dict, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: ExternalRef,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    super_ = commands.prepare_dtype_for_response[Context, Format, Ref, type(value)]
    data_dict = super_(context, fmt, dtype, value, data=data, action=action, select=select)
    prefixes = data['_available_prefixes']
    attributes = {}
    children = []
    if data_dict is None:
        return None

    if len(data_dict) == 1 and '_id' in data_dict or '_uri' in data_dict:
        if '_uri' in data_dict:
            attributes[data['_resource_name']] = data_dict['_uri'].text
        else:
            attributes[data['_resource_name']] = get_model_link(
                dtype.model,
                pk=data_dict['_id'].text
            )
    else:
        if isinstance(data['_elem_name'], QName):
            prefix, name = _get_prefix_and_name(dtype.model.uri)
            if name:
                ref_model_name = _get_attribute_name(name, prefix, prefixes)
            else:
                ref_model_name = _get_attribute_name(dtype.model.basename, prefix, prefixes)
            data_dict, ref_model_attrs = _get_attributes(
                dtype.model,
                data_dict,
                data['_about_name'],
                data['_type_name'],
                data['_revision_name'],
            )
            ref_model_elem = _create_element(
                name=ref_model_name,
                attributes=ref_model_attrs,
                children=list(data_dict.values())
            )
            children.append(ref_model_elem)
        else:
            data_dict['_type'] = _create_element(
                name='_type',
                text=dtype.model.model_type()
            )
            data_dict, description_attrs = _get_attributes(
                dtype.model,
                data_dict,
                data['_about_name'],
                data['_type_name'],
                data['_revision_name'],
            )
            description_elem = _create_element(
                name=_get_attribute_name(DESCRIPTION, RDF, prefixes),
                attributes=description_attrs,
                children=list(data_dict.values())
            )
            children.append(description_elem)

    if attributes or children:
        return _create_element(
            name=data['_elem_name'],
            attributes=attributes,
            children=children
        )
    else:
        return None


@commands.prepare_dtype_for_response.register(Context, Rdf, Geometry, BaseGeometry)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Geometry,
    value: BaseGeometry,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    # TODO: implement appropriate prepare support to handle Geometry dtype with BaseGeometry value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Geometry, WKBElement)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Geometry,
    value: WKBElement,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    # TODO: implement appropriate prepare support to handle Geometry dtype with WKBElement value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, BackRef, (dict, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: BackRef,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    if value is None:
        return None

    # TODO: implement appropriate prepare support to handle BackRef dtype with dict value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, BackRef, str)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: BackRef,
    value: str,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    # TODO: implement appropriate prepare support to handle BackRef dtype with str value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, ArrayBackRef, (tuple, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: ArrayBackRef,
    value: tuple,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    if value is None:
        return None

    # TODO: implement appropriate prepare support to handle ArrayBackRef dtype with tuple value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, ArrayBackRef, list)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: ArrayBackRef,
    value: list,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    # TODO: implement appropriate prepare support to handle ArrayBackRef dtype with list value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, JSON, (object, type(None), NotAvailable))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: JSON,
    value: object,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    if value is None or isinstance(value, NotAvailable):
        return None

    # TODO: implement appropriate prepare support to handle JSON dtype with object value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Object, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Object,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    # TODO: implement appropriate prepare support to handle Object dtype with dict value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Binary, bytes)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Binary,
    value: bytes,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    return _create_element(
        name=data['_elem_name'],
        text=base64.b64encode(value).decode('ascii')
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Array, (list, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Array,
    value: List[Any],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    if value is None:
        return None

    # TODO: implement appropriate prepare support to handle Array dtype with list value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Array, tuple)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Array,
    value: tuple,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    # TODO: implement appropriate prepare support to handle Array dtype with tuple value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Inherit, (object, type(None), NotAvailable))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Inherit,
    value: object,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    if value is None or isinstance(value, NotAvailable):
        return None

    # TODO: implement appropriate prepare support to handle Inherit dtype with object value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, Inherit, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: Inherit,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    # TODO: implement appropriate prepare support to handle Inherit dtype with dict value
    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )


@commands.prepare_dtype_for_response.register(Context, Rdf, UUID, (object, type(None), NotAvailable))
def prepare_dtype_for_response(
    context: Context,
    fmt: Rdf,
    dtype: UUID,
    value: object,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None
):
    if value is None or isinstance(value, NotAvailable):
        return None

    return _create_element(
        name=data['_elem_name'],
        text=str(value)
    )
