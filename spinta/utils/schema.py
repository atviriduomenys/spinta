from typing import Union, List

from spinta import exceptions


class NotAvailable:

    def __repr__(self):
        return "<NA>"

    def __bool__(self):
        return False


NA = NotAvailable()


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
        value = params.get(name, NA)
        value = _get_value(obj, schema[name], name, value)
        setattr(obj, name, value)
    return obj


def _get_value(obj: object, schema: dict, name: str, value: object):
    if schema.get('required', False) and value is NA:
        raise Exception(f"Missing required param {name!r}.")
    if value is NA:
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


def is_valid_sort_key(key, model):
    # sort key must be a leaf node in the model schema.
    # we cannot sort using intermediate node, because it's type would
    # be `array` or `object`.
    #
    # is_valid_sort_key('certificates', report_model) == False
    # is_valid_sort_key('certificates.notes.note_type', report_model) == True
    leaf_key = key.split('.')[-1]
    if leaf_key not in model.leafprops:
        return False
    return True
