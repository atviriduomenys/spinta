from typing import Dict

import pathlib

from spinta.core.components import Component
from spinta.components.core import Config


class Node(Component):
    name: str = None
    parent: 'Node' = None

    def __repr__(self) -> str:
        return f'<{type(self).__module__}.{type(self).__name__}(name={self.name!r})>'

    def __hash__(self) -> int:
        # This is a recursive hash, goes down to all parents. There can be nodes
        # with same type and name on different parts of nodes tree, that is why
        # we have to also include parents.
        return hash((self.type, self.name, self.parent))

    def node_type(self):
        """Return node type.

        Usually node type is same as `Node.type` attribute, but in some cases,
        there can be same `Node.type`, but with a different behaviour. For
        example there are two types of `model` nodes, one is
        `spinta.types.dataset.Model` and other is `spinta.components.Model`.
        Both these nodes have `model` type, but `Node.node_type()` returns
        different types, `model:dataset` and `model`.

        In other words, there can be several types of model nodes, they mostly
        act like normal models, but they are implemented differently.
        """
        return self.type

    def model_type(self):
        """Return model name and specifier.

        This is a full and unique model type, used to identify a specific model.
        """
        specifier = self.model_specifier()
        if specifier:
            return f'{self.name}/{specifier}'
        else:
            return self.name

    def model_specifier(self):
        """Return model specifier.

        There can by sever different kinds of models. For example
        `model:dataset` always has a specifier, that looks like this
        `:dataset/dsname`, also, `ns` models have `:ns` specifier.
        """
        raise NotImplementedError


class Manifest(Node):
    nodes: Dict[str, Node]
    endpoints: Dict[str, str]

    def load(self, config: Config):
        pass

    @property
    def models(self):
        return self.nodes['model']


class MetaData(Node):
    """Manifest metadata node.

    This is top level manifest node, like model or dataset. On YAML manifests
    MetaData usually represents.one YAML file in manifest directory.
    """

    # Optional, path to metadata file in manifest directory, if manifest is
    # store on a filesystem.
    path: pathlib.Path = None
    parent: Manifest
    title: str
    desription: str
    version: dict

    schema = {
        'type': {'type': 'string', 'required': 'load'},
        'name': {'type': 'string', 'required': 'load'},
        'title': {'type': 'string'},
        'description': {'type': 'string'},
        'version': {
            'type': 'object',
            'factory': dict,
            'items': {
                'id': {'type': 'integer', 'required': True},
                'date': {'type': 'datetime', 'required': True},
            },
        },
    }
