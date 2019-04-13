def resolve_schema(obj, Base):
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
