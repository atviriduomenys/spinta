from typing import Union, List, Type

from spinta import exceptions

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
        raise Exception(f"Could not find specified base {Base!r} on {obj.__class__}.")
    bases = reversed(bases)
    schema = {}
    for cls in bases:
        if hasattr(cls, 'schema'):
            schema.update(cls.schema)
    return schema


def load_from_schema(Base: type, obj: object, params: dict, check_unknowns=True):
    schema = resolve_schema(obj, Base)
    for name in set(schema) | set(params):
        if name not in schema:
            if check_unknowns:
                raise exceptions.UnknownParameter(obj, param=name)
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


def check_unkown_params(
    schema: Union[List[dict], dict],
    data: dict, node,
):
    schemas = schema if isinstance(schema, list) else [schema]
    known_params = set.union(*(set(s.keys()) for s in schemas))
    given_params = set(data.keys())
    unknown_params = given_params - known_params
    if unknown_params:
        raise exceptions.MultipleErrors(
            exceptions.UnknownParameter(node, param=param)
            for param in sorted(unknown_params)
        )
