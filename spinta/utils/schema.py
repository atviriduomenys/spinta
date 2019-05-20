na = object()


def resolve_schema(obj, Base):
    # Emulates inheritance for `schema` class attribute for given `obj` and
    # one of the `obj's` base classes `Base`.
    #
    # e.g.:
    #
    # class Base:
    #     schema = { 'required': {'type': 'bool', 'default': False} }
    #
    # class Obj(Base):
    #     schema = { 'items': {} }
    #
    # obj = Obj()
    # full_schema = resolve_schema(obj, Base)
    # print(full_schema)
    # { 'required': {'type': 'bool', 'default': False}, 'items': {} }

    bases = []
    for cls in obj.__class__.mro():
        bases.append(cls)
        if cls is Base:
            break
    else:
        raise Exception(f"Could not find specified base {Base!r} on {obj.__clas__}.")
    bases = reversed(bases)
    schema = {}
    for cls in bases:
        if hasattr(cls, 'schema'):
            schema.update(cls.schema)
    return schema


def load_from_schema(Base: type, obj: object, schema: dict, params: dict, check_unknowns=True):
    schema = resolve_schema(obj, Base)
    for name in set(schema) | set(params):
        if name not in schema:
            if check_unknowns:
                continue
                # TODO: temporarily commented this out.
                # raise Exception(f"Unknown param {name!r} of {obj!r}.")
            else:
                continue
        value = params.get(name, na)
        value = _get_value(obj, schema[name], name, value)
        setattr(obj, name, value)
    return obj


def _get_value(obj: object, schema: dict, name: str, value: object):
    if schema.get('required', False) and value is na:
        raise Exception(f"Missing required param {name!r}.")
    if value is na:
        value = schema.get('default')
    return value
