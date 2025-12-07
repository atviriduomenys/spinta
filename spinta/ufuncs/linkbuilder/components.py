from spinta.components import Context
from spinta.core.ufuncs import Env
from spinta.datasets.components import Resource, Dataset, Param


class LinkBuilder(Env):
    resource: Resource
    dataset: Dataset
    params: dict[str, Param]

    def __init__(self, context: Context, resource: Resource, dataset: Dataset, **kwargs) -> None:
        super().__init__(context, **kwargs)
        self.resource = resource
        self.dataset = dataset
        self.params = {param.name: param for param in resource.params}
