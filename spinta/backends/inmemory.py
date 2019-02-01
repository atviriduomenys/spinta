from spinta.backends.base import Backend
from spinta.backends.base import BackendError


class InMemory(Backend):

    def __init__(self):
        self._schema = {}
        self._storage = {}

    def create_table(self, model):
        self._schema[model.name] = model
        self._storage[model.name] = {}

    def insert(self, model, data):
        self._check_table_exist(model)
        self._check_properties_exist(model, data)
        pk = model.get_primary_key(data)
        self._check_primary_key_not_exists(model, data)
        self._storage[model.name][pk] = data

    def _check_table_exist(self, model):
        if model.name not in self._schema:
            raise BackendError("Table {model.name} does not exist.")

    def _check_properties_exist(self, model, data):
        for name in data.keys():
            if not model.has_property(name):
                raise BackendError("Table {model.name} doens not have property named {name}.")

    def _check_primary_key_not_exists(self, model, pk):
        if pk in self._storage[model.name]:
            raise BackendError("Table primary key {model.primary_key} with value {pk} already exist in {model.name}.")
