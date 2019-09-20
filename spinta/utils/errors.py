import logging

log = logging.getLogger(__name__)


def report_error(exc: Exception, stop_on_error: bool = True):
    if stop_on_error:
        raise exc
    else:
        log.error(str(exc))
