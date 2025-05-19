from __future__ import annotations
import contextlib

from requests import RequestException
from zeep import Client as ZeepClient
from zeep.exceptions import Error as ZeepError

from spinta.datasets.components import ExternalBackend
from spinta.exceptions import WsdlClientError


class WsdlBackend(ExternalBackend):
    type: str = "wsdl"
    client: ZeepClient

    @contextlib.contextmanager
    def begin(self):
        self.client = self.get_client()
        yield

    def get_client(self) -> ZeepClient:
        wsdl_url = self.config.get("dsn")
        try:
            return ZeepClient(wsdl=wsdl_url)
        except (RequestException, ZeepError) as err:
            raise WsdlClientError(f"Zeep client initialization with url {wsdl_url} failed. Error: {err}")
