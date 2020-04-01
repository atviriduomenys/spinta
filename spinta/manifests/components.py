from typing import Dict

import pathlib

from spinta.components import Component, Config, Store


class Manifest(Component):
    type: str = None
    name: str = None
    parent: Component = None
    store: Store = None
    objects: Dict[str, 'Node'] = None
    path: pathlib.Path = None

    # {<endpoint>: <model.name>} mapping. There can be multiple model types, but
    # name and endpoint for all of them should match.
    endpoints: Dict[str, str] = None

    def __repr__(self):
        return f'<{self.__class__.__module__}.{self.__class__.__name__}(name={self.name!r})>'

    def load(self, config: Config):
        pass

    def add_model_endpoint(self, model):
        endpoint = model.endpoint
        if endpoint:
            if endpoint not in self.endpoints:
                self.endpoints[endpoint] = model.name
            elif self.endpoints[endpoint] != model.name:
                raise Exception(f"Same endpoint, but different model name, endpoint={endpoint!r}, model.name={model.name!r}.")

    @property
    def models(self):
        return self.objects['model']
