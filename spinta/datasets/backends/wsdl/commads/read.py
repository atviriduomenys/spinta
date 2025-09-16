from typing import Any

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.wsdl.components import WsdlBackend
from spinta.manifests.components import Manifest


@commands.load.register(Context, WsdlBackend, dict)
def load(context: Context, backend: WsdlBackend, config: dict[str, Any]) -> None:
    pass


@commands.prepare.register(Context, WsdlBackend, Manifest)
def prepare(context: Context, backend: WsdlBackend, manifest: Manifest, **kwargs) -> None:
    pass


@commands.bootstrap.register(Context, WsdlBackend)
def bootstrap(context: Context, backend: WsdlBackend) -> None:
    pass


@commands.wait.register(Context, WsdlBackend)
def wait(context: Context, backend: WsdlBackend, *, fail: bool = False) -> bool:
    return True
