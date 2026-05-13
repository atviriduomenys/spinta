from typing import Any, Dict
from shapely.geometry.base import BaseGeometry

from spinta import commands
from spinta.components import Context
from spinta.core.enums import Action
from spinta.formats.components import Format
from spinta.formats.html.components import Cell, Html
from spinta.types.geometry.components import Geometry
from spinta.types.geometry.helpers import get_display_value, get_osm_link


@commands.prepare_dtype_for_response.register(Context, Format, Geometry, BaseGeometry)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Geometry,
    value: BaseGeometry,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return value.wkt


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
