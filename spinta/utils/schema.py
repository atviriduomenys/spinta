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
