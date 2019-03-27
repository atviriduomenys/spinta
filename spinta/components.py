import importlib

components = {
    'protocols': {
        'http': {
            'component': 'spinta.protocols.http:HttpProtocol',
        }
    }
}


class Components:

    def __init__(self, components):
        self._components = components
        self._loaded = {}
        self._instances = {
            'session': {},
            'request': {},
            'command': {},
        }

    def get(self, name, scope='request'):
        if name not in self._instances[scope]:
            self._instances[scope][name] = self.load(name)
        return self._instances[scope][name]

    def load(self, name):
        if name not in self._loaded:
            component = self._components
            for key in name.split('.'):
                if key not in component:
                    raise Exception(f"Unknown component {name}.")
                component = component[key]

            if 'component' not in component:
                raise Exception(f"Unknown component {name}.")

            module, klass = component['component'].split(':')
            module = importlib.import_module(module)
            klass = getattr(module, klass)
            self._loaded[name] = {**component, 'class': klass}

        component = self._loaded[name]
        return component['class']()
