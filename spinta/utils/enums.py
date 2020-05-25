from spinta import exceptions


def get_enum_by_name(enum, value):
    for item in enum:
        if item.name == value:
            return item
    raise Exception(f"Unknown value {value!r}.")


def enum_by_name(component, param, enum, name):
    if name is None or name == '':
        return None
    for item in enum:
        if item.name == name:
            return item
    raise exceptions.InvalidValue(component, param=param, given=name)
