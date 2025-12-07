from __future__ import annotations

import contextlib
from typing import Any, Type
from typing import Dict
from typing import Optional
from typing import Set

from spinta.backends.constants import BackendOrigin, BackendFeatures
from spinta.core.ufuncs import Env
from spinta.ufuncs.resultbuilder.components import ResultBuilder


class Backend:
    metadata = {
        "name": "backend",
    }

    type: str
    name: str
    origin: BackendOrigin
    features: Set[BackendFeatures] = set()

    # Original configuration values given in manifest, this is used to restore
    # manifest back to its original form.
    config: Dict[str, Any]

    available: bool = True

    # Query builder's type in config.components['querybuilders']
    # by default, '' is QueryBuilder
    query_builder_type: str = ""
    # Later on type should be changed to `QueryBuilder`
    query_builder_class: Type[Env]

    # Result builder's type in config.components['resultbuilders']
    # by default, '' is ResultBuilder
    result_builder_type: str = ""
    result_builder_class: Type[ResultBuilder]

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__}(name={self.name!r}) at 0x{id(self):02x}>"

    @contextlib.contextmanager
    def transaction(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def begin(self):
        raise NotImplementedError

    def bootstrapped(self):
        raise NotImplementedError

    # Checks if backend supports specific feature
    def supports(self, feature: BackendFeatures) -> bool:
        return feature in self.features


SelectTree = Optional[Dict[str, "SelectTree"]]
