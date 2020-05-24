from __future__ import annotations

from typing import TYPE_CHECKING, Dict

import pathlib

from spinta.core.enums import Access
from spinta.components import Component, Store, MetaData

if TYPE_CHECKING:
    from spinta.backends.components import Backend
    from spinta.datasets.keymaps.components import KeyMap


class Manifest(Component):
    type: str = None
    name: str = None
    keymap: KeyMap = None
    backend: Backend = None
    parent: Component = None
    store: Store = None
    objects: Dict[str, MetaData] = None
    path: pathlib.Path = None
    access: Access = Access.protected

    # {<endpoint>: <model.name>} mapping. There can be multiple model types, but
    # name and endpoint for all of them should match.
    endpoints: Dict[str, str] = None

    def __repr__(self):
        return (
            f'<{type(self).__module__}.{type(self).__name__}'
            f'(name={self.name!r})>'
        )

    def add_model_endpoint(self, model):
        endpoint = model.endpoint
        if endpoint:
            if endpoint not in self.endpoints:
                self.endpoints[endpoint] = model.name
            elif self.endpoints[endpoint] != model.name:
                raise Exception(
                    f"Same endpoint, but different model name, "
                    f"endpoint={endpoint!r}, model.name={model.name!r}."
                )

    @property
    def models(self):
        return self.objects['model']
