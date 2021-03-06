import contextlib
import typing

from spinta import commands
from spinta.components import Node
from spinta.auth import AdminToken


class ContextForTests:

    def _get_model(self, model: typing.Union[str, Node], dataset: str):
        if isinstance(model, str):
            store = self.get('store')
            if dataset:
                return store.manifests['default'].objects['dataset'][dataset].objects[model]
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
        store = self.get('store')
        with self.transaction(write=True):
            yield from commands.push(self, store, data)

    def getone(self, model: str, id, *, dataset: str = None):
        model = self._get_model(model, dataset)
        with self.transaction():
            return commands.get(self, model, model.backend, id)

    def getall(self, model: str, *, dataset: str = None, **kwargs):
        model = self._get_model(model, dataset)
        with self.transaction():
            return list(commands.getall(self, model, model.backend, **kwargs))

    def changes(self, model: str, *, dataset: str = None, **kwargs):
        model = self._get_model(model, dataset)
        with self.transaction():
            return list(commands.changes(self, model, model.backend, **kwargs))

    def wipe(self, model: str, *, dataset: str = None):
        model = self._get_model(model, dataset)
        with self.transaction():
            commands.wipe(self, model, model.backend)
