import contextlib
import typing
import collections

from toposort import toposort

from spinta import commands
from spinta.components import Node
from spinta.auth import AdminToken
from spinta.types.store import get_model_by_name
from spinta import components
from spinta.utils.commands import load_commands
from spinta.auth import AuthorizationServer, ResourceProtector, BearerTokenValidator


class ContextForTests:

    def __init__(self):
        super().__init__()
        self.loaded = False

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
        self.load_if_not_loaded()
        store = self.get('store')
        dataset = store.manifests['default'].objects['dataset'][dataset]
        models = models or []
        with self.transaction(write=push):
            try:
                data = list(commands.pull(self, dataset, models=models))
            except Exception:
                raise Exception(f"Error while processing '{dataset.path}'.")
            data = self.push(data) if push else data
        return data

    def push(self, data):
        self.load_if_not_loaded()
        result = []
        store = self.get('store')
        manifest = store.manifests['default']
        with self.transaction(write=True):
            for d in data:
                d = dict(d)
                type_ = d.pop('type', None)
                assert type_ is not None, d
                model = get_model_by_name(self, manifest, type_)
                if 'id' in d:
                    id_ = commands.upsert(self, model, model.backend, key=['id'], data=d)
                else:
                    id_ = commands.insert(self, model, model.backend, data=d)
                if id_ is not None:
                    result.append({
                        'type': type_,
                        'id': id_,
                    })
        return result

    def getone(self, model: str, id_, *, dataset: str = None, resource: str = None):
        self.load_if_not_loaded()
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            return commands.getone(self, model, model.backend, id_=id_)

    def getall(self, model: str, *, dataset: str = None, resource: str = None, **kwargs):
        self.load_if_not_loaded()
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            return list(commands.getall(self, model, model.backend, **kwargs))

    def changes(self, model: str, *, dataset: str = None, resource: str = None, **kwargs):
        self.load_if_not_loaded()
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            return list(commands.changes(self, model, model.backend, **kwargs))

    def wipe(self, model: str, *, dataset: str = None, resource: str = None):
        self.load_if_not_loaded()
        model = self._get_model(model, dataset, resource)
        with self.transaction():
            commands.wipe(self, model, model.backend)

    def wipe_all(self):
        with self.transaction():
            store = self.get('store')

            # Remove all data after each test run.
            graph = collections.defaultdict(set)
            for model in store.manifests['default'].objects['model'].values():
                if model.name not in graph:
                    graph[model.name] = set()
                for prop in model.properties.values():
                    if prop.type.name == 'ref':
                        graph[prop.type.object].add(model.name)

            for models in toposort(graph):
                for name in models:
                    self.wipe(name)

            # Datasets does not have foreign kei constraints, so there is no need to
            # topologically sort them. At least for now.
            for dataset in store.manifests['default'].objects['dataset'].values():
                for resource in dataset.resources.values():
                    for model in resource.objects.values():
                        self.wipe(model)

            self.wipe(store.internal.objects['model']['transaction'])

    def load(self, overrides=None):
        if self.loaded:
            raise Exception("test context is already loaded")

        config = self.get('config.raw')

        if overrides:
            config.hardset(overrides)

        self.set('config', components.Config())
        store = self.set('store', components.Store())

        load_commands(config.get('commands', 'modules', cast=list))
        commands.load(self, self.get('config'), config)
        commands.check(self, self.get('config'))
        commands.load(self, store, config)
        commands.check(self, store)
        commands.prepare(self, store.internal)
        commands.migrate(self, store)
        commands.prepare(self, store)
        commands.migrate(self, store)

        self.bind('auth.server', AuthorizationServer, self)
        self.bind('auth.resource_protector', ResourceProtector, self, BearerTokenValidator)

        self.enter_stack()

        self.loaded = True

        return self

    def load_if_not_loaded(self):
        if not self.loaded:
            self.load()
