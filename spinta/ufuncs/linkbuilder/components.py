from typing import Any

from spinta.components import Context
from spinta.core.ufuncs import Env
from spinta.datasets.components import Dataset, Param, Resource


class LinkBuilder(Env):
    resource: Resource
    dataset: Dataset
    params: dict[str, Param]

    def __init__(
        self,
        context: Context,
        resource: Resource,
        dataset: Dataset,
        *,
        this: Any,
        resolvers=None,
        executors=None,
        scope=None,
    ) -> None:
        # `this` is required at construction time, so a missing value fails
        # here, not later when a formula is resolved. Use `this=None` for
        # resource-level formulas that have no current property.
        super().__init__(context, resolvers, executors, {**(scope or {}), "this": this})
        self.resource = resource
        self.dataset = dataset
        self.params = {param.name: param for param in resource.params}

    def __call__(self, *, this: Any, **scope):
        return LinkBuilder(
            self.context,
            self.resource,
            self.dataset,
            this=this,
            resolvers=self._resolvers,
            executors=self._executors,
            scope={**self._scope, **scope},
        )
