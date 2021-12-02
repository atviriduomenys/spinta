import base64

from spinta import commands
from spinta.utils.schema import NA
from spinta.components import Context
from spinta.types.datatype import File
from spinta.types.helpers import check_no_extra_keys
from spinta.formats.components import Format
from spinta.formats.json.components import Json
from spinta.backends.components import Backend


@commands.load.register(Context, File, object)
def load(context: Context, dtype: File, value: object) -> object:
    if value is NA:
        return value

    assert isinstance(value, dict)

    # check that given obj does not have more keys, than dtype's schema
    check_no_extra_keys(dtype, dtype.schema, value)

    if '_content' in value and isinstance(value['_content'], str):
        value['_content'] = base64.b64decode(value['_content'])

    return commands.decode(context, Json(), dtype.backend, dtype, value)


@commands.decode.register(Context, Format, Backend, File, dict)
def decode(context: Context, source: Format, target: Backend, dtype: File, value: dict):
    return value
