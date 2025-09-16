from spinta.components import Context
from spinta.core.ufuncs import Env
from spinta.datasets.components import Resource, Dataset


class LinkBuilder(Env):
    resource: Resource
    dataset: Dataset

    def __init__(self, context: Context, resource: Resource, dataset: Dataset, **kwargs) -> None:
        super().__init__(context, **kwargs)
        self.resource = resource
        self.dataset = dataset
