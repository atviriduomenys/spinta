import logging

from spinta.api import app  # noqa

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)
