from typing import Any
from typing import Dict
from typing import Optional

from spinta import commands
from spinta.components import Context
from spinta.exceptions import InvalidParameterValue
from spinta.exceptions import TooManyParameters
from spinta.manifests.components import Manifest
from spinta.types.datatype import DataType
from spinta.types.geometry.components import Geometry


GEOMETRY_TYPES = {
    'point',
    'linestring',
    'polygon',
}
GEOMETRY_TYPES |= {
    f'multi{x}'
    for x in GEOMETRY_TYPES
}
GEOMETRY_TYPES |= {
    x + d
    for d in ('z', 'm', 'zm')
    for x in GEOMETRY_TYPES
}


@commands.load.register(Context, Geometry, dict, Manifest)
def load(
    context: Context,
    dtype: Geometry,
    data: Dict[str, Any],
    manifest: Manifest,
) -> Geometry:
    _load = commands.load[Context, DataType, dict, Manifest]
    dtype: Geometry = _load(context, dtype, data, manifest)

    srid: Optional[int] = None
    geometry_type: Optional[str] = None
    if dtype.type_args:
        if len(dtype.type_args) == 1:
            arg = dtype.type_args[0]
            if arg.isdigit():
                srid = int(arg)
            else:
                geometry_type = arg
        elif len(dtype.type_args) == 2:
            geometry_type, _srid = dtype.type_args
            if _srid.isdigit():
                srid = int(_srid)
            else:
                raise InvalidParameterValue(dtype, parameter='srid')
        else:
            raise TooManyParameters(dtype, max_params=2)

    if geometry_type:
        geometry_type = geometry_type.lower()
        if geometry_type not in GEOMETRY_TYPES:
            raise InvalidParameterValue(dtype, parameter='geometry_type')

    dtype.srid = srid
    dtype.geometry_type = geometry_type

    return dtype
