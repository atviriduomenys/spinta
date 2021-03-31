from typing import Optional, Union

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import File
from spinta.utils.schema import NotAvailable, NA


@commands.build_data_patch_for_write.register(Context, File)
def build_data_patch_for_write(
    context: Context,
    dtype: File,
    *,
    given: Optional[dict],
    saved: Optional[dict],
    insert_action: bool = False,
    update_action: bool = False,
) -> Union[dict, NotAvailable]:
    if insert_action or update_action:
        given = {
            '_id': given.get('_id', None) if given else None,
            '_content_type': given.get('_content_type', None) if given else None,
            '_content': given.get('_content', NA) if given else NA,
        }
    else:
        given = {
            '_id': given.get('_id', NA) if given else NA,
            '_content_type': given.get('_content_type', NA) if given else NA,
            '_content': given.get('_content', NA) if given else NA,
        }
    saved = {
        '_id': saved.get('_id', NA) if saved else NA,
        '_content_type': saved.get('_content_type', NA) if saved else NA,
        '_content': saved.get('_content', NA) if saved else NA,
    }
    given = {
        k: v for k, v in given.items()
        if v != saved[k]
    }
    given = {k: v for k, v in given.items() if v is not NA}
    return given or NA
