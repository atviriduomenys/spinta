import logging

from typing import Optional

from spinta import exceptions
from spinta.typing import ObjectData

log = logging.getLogger(__name__)


def report_error(
    exc: Exception,
    data: Optional[ObjectData] = None,
    *,
    stop_on_error: bool = True,
) -> Exception:
    if data and '_id' in data and isinstance(exc, exceptions.BaseError):
        exc.context['id'] = data['_id']
    if stop_on_error:
        raise exc
    else:
        log.error(str(exc))
    return exc
