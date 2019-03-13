from spinta.commands import Command
from spinta.types import Type
from spinta.types.object import Object
from spinta.utils.refs import get_ref_id


class Dataset(Type):
    metadata = {
        'name': 'dataset',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'source': {'type': 'command'},
            'objects': {'type': 'object', 'default': {}},
            'version': {'type': 'integer', 'required': True},
            'date': {'type': 'date', 'required': True},
            'owner': {'type': 'string'},
            'stars': {'type': 'integer'},
            'parent': {'type': 'manifest'},
            'backend': {'type': 'string', 'default': 'default'},
        },
    }


class Model(Object):
    metadata = {
        'name': 'dataset.model',
        'properties': {
            'source': {'type': 'command_list'},
            'identity': {'type': 'array', 'required': False},
            'properties': {'type': 'object', 'default': {}},
            'stars': {'type': 'integer'},
            'local': {'type': 'boolean'},
            'parent': {'type': 'dataset'},
            'backend': {'type': 'string', 'default': 'default', 'inherit': True},
            'dependencies': {'type': 'object'},
            'extends': {'type': 'string'},
            'canonical': {'type': 'boolean'},
        },
    }

    property_type = 'dataset.property'

    def get_type_value(self):
        return f'{self.name}/:source/{self.parent.name}'


class Property(Type):
    metadata = {
        'name': 'dataset.property',
        'properties': {
            'source': {'type': 'string'},
            'local': {'type': 'boolean'},
            'stars': {'type': 'integer'},
            'const': {'type': 'any'},
            'enum': {'type': 'array'},
            'parent': {'type': 'dataset.model'},
            'replace': {'type': 'object'},
            'ref': {'type': 'string'},
            'dependency': {'type': 'boolean'},
        },
    }


class LoadDataset(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'dataset',
    }

    def execute(self):
        super().execute()
        assert isinstance(self.obj.objects, dict)
        for name, obj in self.obj.objects.items():
            self.obj.objects[name] = self.load({
                'type': 'dataset.model',
                'name': name,
                'parent': self.obj,
                **(obj or {}),
            })


class PrepareDataset(Command):
    metadata = {
        'name': 'prepare.type',
        'type': 'dataset',
    }

    def execute(self):
        super().execute()
        for model in self.obj.objects.values():
            self.run(model, {'prepare.type': None})


class CheckDataset(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'dataset',
    }

    def execute(self):
        self.check_owner()
        for model in self.obj.objects.values():
            self.check_extends(model)
            for prop in model.properties.values():
                self.check_ref(prop)

    def check_owner(self):
        if self.obj.owner and self.obj.owner not in self.store.objects[self.ns]['owner']:
            self.error(f"Can't find owner {self.obj.owner!r}.")

    def check_extends(self, model):
        if not model.extends:
            return

        if model.extends in self.obj.objects:
            return

        if model.extends in self.store.objects[self.ns]['model']:
            return

        self.error(f"Can't find model {model.name!r} specified in 'extends'.")

    def check_ref(self, prop):
        if prop.ref and prop.ref not in self.obj.objects and prop.ref not in self.store.objects[self.ns]['model']:
            self.error(f"{prop.parent.name}.{prop.name} referenced an unknown object {prop.ref!r}.")


class Pull(Command):
    metadata = {
        'name': 'pull',
        'type': 'dataset',
    }

    def execute(self):
        for model in self.obj.objects.values():
            if model.source is None:
                continue

            if self.args.models and model.name not in self.args.models:
                continue

            for dependency in self.dependencies(model.dependencies):
                for source in model.source:
                    command, args = next(iter(source.items()), None)
                    rows = self.run(model, {command: {**args, 'dependency': dependency}})
                    for row in rows:
                        data = {'type': f'{model.name}/:source/{self.obj.name}'}
                        for prop in model.properties.values():
                            if isinstance(prop.source, list):
                                data[prop.name] = [
                                    self.get_value_from_source(prop, command, prop_source, row, dependency)
                                    for prop_source in prop.source
                                ]

                            elif prop.source:
                                data[prop.name] = self.get_value_from_source(prop, command, prop.source, row, dependency)

                            if prop.ref and prop.name in data:
                                data[prop.name] = get_ref_id(data[prop.name])

                        if self.check_key(data.get('id')):
                            yield data

    def dependencies(self, deps):
        if deps:
            model_names = set()
            prop_names = []
            prop_name_mapping = {}
            for name, dep in deps.items():
                if '.' not in dep:
                    self.error(f"Dependency must be in 'object/name.property' form, got: {dep}.")
                model_name, prop_name = dep.split('.', 1)
                model_names.add(model_name)
                prop_names.append(prop_name)
                prop_name_mapping[prop_name] = name
            if len(model_names) > 1:
                names = ', '.join(sorted(model_names))
                self.error(f"Dependencies are allowed only from single model, but more than one model found: {names}.")
            model_name = list(model_names)[0]
            for row in self.store.getall(model_name, {'show': prop_names, 'source': self.obj.name}):
                yield {
                    prop_name_mapping[k]: v
                    for k, v in row.items()
                }
        else:
            yield {}

    def check_key(self, key):
        if isinstance(key, list):
            for k in key:
                if k is None:
                    return False
        elif key is None:
            return False
        return True

    def get_value_from_source(self, prop, command, source, value, dependency):
        if prop.dependency:
            return dependency.get(source)
        else:
            return self.run(prop, {command: {'source': source, 'value': value}})
