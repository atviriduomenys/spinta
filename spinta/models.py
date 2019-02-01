class Property:

    def __init__(self, name):
        self.name = name


class Model:

    def __init__(self, spec: dict):
        self.spec = self.prepare_spec()
        self.backend = None

    def prepare_spec(self, spec: dict):
        return spec

    @property
    def name(self):
        return self.spec['name']

    def create_table(self):
        if self.name in self.schema:
            raise Exception(f"Table {self.name} already exist in database.")
        self.backend.create_table()

    def insert(self, data: dict):
        self.check_table_exists()
        self.check_properties_exist()

    def update(self, data: dict):
        pass

    def delete(self, data: dict):
        pass

    def has_property(self, name: str):
        return name in self.properties

    def get_primary_key(self, data: dict):
        if self.primary_key is None:
            raise Exception(f"Primary key is not set for {self.name} model.")

        if isinstance(self.primary_key, tuple):
            primary_key = []
            for name in self.primary_key:
                if name not in data or data[name] is None:
                    raise Exception(f"Primary key {name} is not given for {self.name} model.")
                primary_key.append(data[name])
            return tuple(primary_key)

        else:
            if self.primary_key not in data or data[self.primary_key] is None:
                raise Exception(f"Primary key {self.primary_key} is not given for {self.name} model.")
            return data[self.primary_key]
