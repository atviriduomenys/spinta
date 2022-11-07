from typing import Optional

from zeep import Client

from spinta.datasets.components import ExternalBackend


class Soap(ExternalBackend):
    type: str = 'soap'
    client: Optional[Client] = None
