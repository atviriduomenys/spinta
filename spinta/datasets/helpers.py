from typing import Optional

from spinta import exceptions
from spinta.backends import Backend
from spinta.backends.components import BackendOrigin
from spinta.backends.helpers import load_backend
from spinta.components import Context
from spinta.datasets.components import Resource
from spinta.utils.naming import to_code_name


def load_resource_backend(
    context: Context,
    resource: Resource,
    name: Optional[str],  # Backend name given in `ref` column.
) -> Backend:
    if name:
        # If backend name is given, just use an exiting backend.
        store = resource.dataset.manifest.store
        possible_backends = [
            # Backend defined in manifest.
            resource.dataset.manifest.backends,
            # Backend defined via configuration.
            store.backends,
        ]
        for backends in possible_backends:
            if name in backends:
                return backends[name]
        raise exceptions.BackendNotFound(resource, name=name)

    elif resource.type or resource.external:
        # If backend is defined inline on resource itself, try to load it.
        name = f'{resource.dataset.name}/{resource.name}'
        return load_backend(context, resource, name, BackendOrigin.resource, {
            'type': resource.type,
            'name': name,
            'dsn': resource.external,
        })

    else:
        # If backend was not given, try to check configuration with resource and
        # dataset names as backend names or fallback to manifest backend.
        store = resource.dataset.manifest.store
        possible_backends = [
            # Dataset and resource name combination.
            f'{resource.dataset.name}/{resource.name}',
            # Just dataset name.
            resource.dataset.name,
            # Just resource type.
            # XXX: I don't think that using resource type as a backend name is a
            #      good idea. Probably should be removed.
            resource.type,
        ]
        for name in filter(None, possible_backends):
            name = to_code_name(name)
            if name in store.backends:
                return store.backends[name]
        return resource.dataset.manifest.backend
