import contextlib

import requests

from spinta.datasets.components import ExternalBackend


class Csv(ExternalBackend):

    @contextlib.contextmanager
    def begin(self):
        yield requests.Session()
