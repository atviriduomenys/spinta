import copy
import pathlib

from spinta.types import NA, Type
from spinta.commands import Command


class ManifestLoad(Command):
    metadata = {
        'name': 'manifest.load',
    }

    def execute(self, data: dict):
        assert isinstance(data, dict)

        for name, params in self.obj.metadata.properties.items():
            if name in data and data[name] is not NA:
                value = data[name]
            else:
                # Get default value.
                default = params.get('default', NA)
                if isinstance(default, (list, dict)):
                    value = copy.deepcopy(default)
                else:
                    value = default

                # Check if value is required.
                if params.get('required', False) and value is NA:
                    self.error(f"Parameter {name} is required.")

                # If value is not given, set it to None.
                if value is NA:
                    value = None

            if params.get('type') == 'path' and isinstance(value, str):
                value = pathlib.Path(value)

            # Set parameter on the spec object.
            self.obj[name] = value

        unknown_keys = set(data.keys()) - set(self.obj.metadata.properties.keys())
        if unknown_keys:
            keys = ', '.join(unknown_keys)
            self.error(f"{self.obj} does not have following parameters: {keys}.")

    def get_object(self, data):
        type = data.get('type')

        if 'const' in data and type is None:
            if isinstance(data['const'], str):
                type = 'string'
            else:
                self.error(f"Unknown data type of {data['const']!r} constant.")

        if type is None:
            self.error(f"Required parameter 'type' is not set.")

        return self.store.get_object(type)


class Serialize(Command):
    metadata = {
        'name': 'serialize',
    }

    def execute(self):
        output = {}
        for k, v in self.obj.metadata.properties.items():
            v = getattr(self.obj, k, v)
            if v is NA:
                continue
            if isinstance(v, Type):
                v = self.run(v, {'serialize': NA})
            if isinstance(v, pathlib.Path):
                v = str(v)
            output[k] = v
        return output
