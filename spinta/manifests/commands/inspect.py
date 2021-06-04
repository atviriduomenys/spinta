from typing import Iterator

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context
from spinta.datasets.components import Dataset
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestSchema
from spinta.manifests.helpers import dataset_to_schema
from spinta.manifests.helpers import resource_to_schema


@commands.inspect.register(Context, Backend, Manifest, type(None))
def inspect(
    context: Context,
    backend: Backend,
    manifest: Manifest,
    source: None,
) -> Iterator[ManifestSchema]:
    for dataset in manifest.datasets.values():
        yield from commands.inspect(context, manifest.backend, dataset, None)


@commands.inspect.register(Context, Backend, Dataset, type(None))
def inspect(
    context: Context,
    backend: Backend,
    dataset: Dataset,
    source: None,
) -> Iterator[ManifestSchema]:
    eid, schema = dataset_to_schema(dataset)
    schema['resources'] = {
        resource.name: resource_to_schema(resource)[1]
        for resource in dataset.resources.values()
    }
    yield eid, schema
    for resource in dataset.resources.values():
        yield from commands.inspect(context, resource.backend, resource, None)
