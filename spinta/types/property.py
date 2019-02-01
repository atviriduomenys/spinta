from spinta.types import Type, Function, NA


class Property(Type):
    name = 'property'


class Date(Property):
    name = 'date'


class String(Property):
    name = 'string'


class Integer(Property):
    name = 'integer'


class BackRef(Property):
    name = 'backref'


class Required(Function):
    name = 'required'

    def execute(self, value, required):
        if required and value is NA:
            self.error(f"Value for this property is required.")
        return value


class Default(Function):
    name = 'default'

    def execute(self, value, default):
        if value is NA:
            if callable(default):
                return default()
            else:
                return default
        else:
            return value
