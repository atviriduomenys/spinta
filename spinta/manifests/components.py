from __future__ import annotations

import dataclasses
from builtins import staticmethod
from typing import Any

from typing import Dict
from typing import IO
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Tuple
from typing import TypedDict
from typing import Union

from spinta.components import Component
from spinta.components import Mode
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Store
from spinta.core.enums import Access
from spinta.dimensions.enum.components import Enums
from spinta.dimensions.prefix.components import UriPrefix

if TYPE_CHECKING:
    from spinta.backends.components import Backend
    from spinta.datasets.components import Dataset
    from spinta.datasets.keymaps.components import KeyMap


class MetaDataContainer(TypedDict):
    ns: Dict[str, Namespace]
    dataset: Dict[str, Dataset]
    model: Dict[str, Model]


class ManifestGiven:
    access: str = None


class Manifest(Component):
    type: str = None
    name: str = None
    keymap: KeyMap = None
    backend: Backend = None
    parent: Component = None
    store: Store = None
    objects: MetaDataContainer = None
    path: str = None
    access: Access = Access.protected
    prefixes: Dict[str, UriPrefix]
    enums: Enums

    # {<endpoint>: <model.name>} mapping. There can be multiple model types,
    # but name and endpoint for all of them should match.
    endpoints: Dict[str, str] = None

    # Backends defined in the manifest.
    backends: Dict[str, Backend] = None

    # List of other source manifests used to populate nodes into the main
    # manifest.
    sync: List[Manifest]

    mode: Mode = Mode.internal

    given: ManifestGiven

    @staticmethod
    def detect_from_path(path: str) -> bool:
        return False

    def __init__(self):
        self.given = ManifestGiven()

    def __repr__(self):
        return (
            f'<{type(self).__module__}.{type(self).__name__}'
            f'(name={self.name!r})>'
        )

    def __eq__(self, other: Union[Manifest, str]):
        if isinstance(other, str):
            # A hack for tests, to make possible things like:
            #     assert manifest == """d | r | m | ..."""
            # This uses pytest_assertrepr_compare hook and compare_manifest to
            # eventually compare manifests in ascii table form.
            from spinta.testing.manifest import compare_manifest
            left, right = compare_manifest(self, other)
            return left == right
        else:
            super().__eq__(other)

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
    def models(self) -> Dict[str, Model]:
        return self.objects['model']

    @property
    def datasets(self) -> Dict[str, Dataset]:
        return self.objects['dataset']

    @property
    def namespaces(self) -> Dict[str, Namespace]:
        return self.objects['ns']


NodeSchema = Optional[Dict[str, Any]]
ManifestSchema = Tuple[Any, NodeSchema]


@dataclasses.dataclass
class ManifestPath:
    type: str = 'tabular'
    name: str = None
    path: str = None
    file: IO = None
