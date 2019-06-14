import contextlib
import typing

from spinta import commands
from spinta.components import Node
from spinta.auth import AdminToken
from spinta.types.store import get_model_by_name


class ContextForTests:

    def _get_model(self, model: typing.Union[str, Node], dataset: str, resource: str):
        if isinstance(model, str):
            store = self.get('store')
            if resource:
                return store.manifests['default'].objects['dataset'][dataset].resources[resource].objects[model]
            else:
                return store.manifests['default'].objects['model'][model]
        else:
            return model

    @contextlib.contextmanager
    def transaction(self, *, write=False):
        store = self.get('store')
        if self.has('transaction'):
            yield self.get('transaction')
        else:
            with self.enter():
                self.set('auth.token', AdminToken())
                yield self.set('transaction', store.manifests['default'].backend.transaction(write=write))

    def pull(self, dataset: str, *, models: list = None, push: bool = True):
        store = self.get('store')
        dataset = store.manifests['default'].objects['dataset'][dataset]
        models = models or []
        try:
            with self.transaction(write=push):
                data = commands.pull(self, dataset, models=models)
                if push:
                    yield from commands.push(self, store, data)
                else:
                    yield from data
        except Exception:
            raise Exception(f"Error while processing '{dataset.path}'.")

    def push(self, data):
        result = []
        store = self.get('store')
        manifest = store.manifests['default']
        with self.transaction(write=True):
            for d in data:
                d = dict(d)
                model_name = d.pop('type', None)
                assert model_name is not None, d
                model = get_model_by_name(self, manifest, model_name)
                if 'id' in d:
                    id_ = d.pop('id')
                    commands.patch(self, model, model.backend, id_=id_, data=d)
                else:
                    id_ = commands.insert(self, model, model.backend, data=d)
                result.append(id_)
        return result

    def getone(self, model: str, id_, *, dataset: str = None, resource: str = None):
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            return commands.getone(self, model, model.backend, id_=id_)

    def getall(self, model: str, *, dataset: str = None, resource: str = None, **kwargs):
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            return list(commands.getall(self, model, model.backend, **kwargs))

    def changes(self, model: str, *, dataset: str = None, resource: str = None, **kwargs):
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            return list(commands.changes(self, model, model.backend, **kwargs))

    def wipe(self, model: str, *, dataset: str = None, resource: str = None):
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            commands.wipe(self, model, model.backend)
