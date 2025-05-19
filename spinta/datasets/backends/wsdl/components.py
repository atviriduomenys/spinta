from __future__ import annotations
import contextlib
import logging

from requests import RequestException
from zeep import Client as ZeepClient
from zeep.exceptions import Error as ZeepError

from spinta.datasets.components import ExternalBackend

log = logging.getLogger(__name__)


class WsdlBackend(ExternalBackend):
    type: str = "wsdl"
    client: ZeepClient | None

    @contextlib.contextmanager
    def begin(self):
        self.client = self.get_client()
        yield

    def get_client(self) -> ZeepClient | None:
        client = None
        wsdl_url = self.config.get("dsn")
        try:
            client = ZeepClient(wsdl=wsdl_url)
        except (RequestException, ZeepError) as err:
            log.error(f"Zeep client initialization with url %s failed. Error: %s", wsdl_url,  err)

        return client
