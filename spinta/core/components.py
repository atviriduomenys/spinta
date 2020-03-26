from typing import Dict, Container, Any, Union, Callable, Set, Tuple

import dataclasses
import enum

from dateutil.parser import parse as parsedate

from spinta.utils.imports import importstr
from spinta import commands
from spinta.dispatcher import Command
from spinta.core import spyna
from spinta.core.schema import NA
from spinta.core.ufuncs import asttoexpr
from spinta.core.data import getval
from spinta.component.context import Context
from spinta.errors.nodes import InvalidComponentData
from spinta.errors.nodes import UnknownParameter
from spinta.errors.components import MissingParam
from spinta.errors.components import UnknownReference
from spinta.errors.components import UnknownValue
from spinta.errors.components import InvalidType
from spinta.errors.components import UnknownKeys


SchemaType = enum.Enum('SchemaType', [
    'string',
    'integer',
    'object',
    'array',
    'ref',
    'date',
    'datetime',
    'comp',
    'bool',
    'spyna',
    'uri',
    'url',
])


@dataclasses.dataclass
class Schema:
    type: SchemaType

    # If True, value is required on 'check', if 'load', then value is required
    # on 'load'.
    required: Union[bool, str]  # bool or 'load'

    # Value must be one of given choices, can be Enum.
    choices: Container

    # Dot separated name relative to component, but can start with 'context.',
    # to resolve a value from context.
    ref: str

    default: Any = NA
    factory: Callable = None

    # Schema for each dictionary item.
    items: Dict[str, dict] = None

    # Schema for all dictionary values.
    values: dict = None

    # Forcefully convert value, for example if expected list, but received
    # scalar, then simply put scalar into list.
    force: bool = False

    # Set value on a given component attribute name, rather than using schema
    # name.
    attr: str = None

    # For type=comp, set component name attribute from dict key.
    name: bool = False

    # Set parent object to this attribute.
    parent: bool = False

    # Dotthed object path where value should be inherited from.
    inherit: str = None

    # Do not update resolved reverreces as value, just check if ref exists.
    resolve: bool = True


class Component:
    ctype: str
    schema: Dict[str, Union[dict, Schema]] = {}


def resolve_schema(obj, Base) -> Dict[str, Schema]:
    """Build component schema by merging schemas of component and its parents.

    Emulates inheritance for `schema` class attribute for given `obj` and
    one of the `obj's` base classes `Base`.

    e.g.:

    class Base:
        schema = { 'required': {'type': 'bool', 'default': False} }

    class Obj(Base):
        schema = { 'items': {} }

    obj = Obj()
    full_schema = resolve_schema(obj, Base)
    print(full_schema)
    {
        'required': {'type': 'bool', 'default': False},
        'items': {},
    }
    """

    bases = []
    for cls in obj.__class__.mro():
        bases.append(cls)
        if cls is Base:
            break
    else:
        raise Exception(
            f"Could not find specified base {Base!r} on {obj.__class__}."
        )

    data = {}
    for cls in reversed(bases):
        if hasattr(cls, 'schema'):
            data.update(cls.schema)

    known = {f.name for f in dataclasses.fields(Schema)}
    return {
        k: _load_schema(known, k, v)
        for k, v in data.items()
    }


def _load_schema(known: Set[str], name: str, data: dict) -> Dict[str, Schema]:
    unknown = set(data) - known
    if unknown:
        unknown = ', '.join(sorted(unknown))
        raise Exception(f"Unknown schema params: {unknown}.")

    data = {
        **data,
        'type': SchemaType[data.get('type', 'string')],
    }
    schema = Schema(**data)

    if schema.type == SchemaType.ref and not schema.ref:
        raise Exception(f"Missing 'ref' param for {name!r}")

    if schema.type == SchemaType.object:
        if 'items' in data:
            schema.items = {
                k: _load_schema(known, f'{name}.{k}', v)
                for k, v in data.items()
            }
        if 'values' in data:
            schema.values = _load_schema(known, name, data['values'])

    if schema.type == SchemaType.array:
        if 'items' in data:
            schema.items = _load_schema(known, name, data['items'])

    return schema


def load(
    context: Context,
    parent: Component = None,
    data: dict = None,
    *,
    group: str,
    ctype: str = None,
    mixed=False,
) -> Component:
    if ctype is None:
        if not isinstance(data, dict):
            raise InvalidComponentData(parent, error=(
                f"Expected dict got {type(data).__name__}."
            ))
        if 'type' not in data:
            raise InvalidComponentData(parent, error=(
                f"Required parameter 'type' is not defined."
            ))
        ctype = data['type']

    rc = context.get('rc')
    Comp = rc.get('components', group, ctype, cast=importstr)

    if Comp is None:
        raise InvalidComponentData(parent, error=(
            f"Unknown {group} component type {ctype!r}."
        ))

    comp = Comp()
    comp.schema = resolve_schema(comp, Comp)
    remainder = {}
    for name in set(comp.schema) | set(data):
        if name not in comp.schema:
            if mixed:
                remainder[name] = data[name]
                continue
            raise UnknownParameter(comp, param=name)
        schema = comp.schema[name]
        attr = schema.attr or name
        if name == 'type' and ctype:
            value = ctype
        elif schema.parent:
            value = parent
        else:
            value = data.get(name, NA)
            value = _load_param(context, comp, schema, (name,), value)
        setattr(value, attr, value)

    # Do custom component loading if needed.
    commands.load(context, comp)

    if mixed:
        return comp, remainder
    else:
        return comp


def _load_param(
    context: Context,
    comp: Component,
    schema: Schema,
    name: Tuple[str],
    value: Any,
):
    if value is NA and schema.required == 'load':
        raise MissingParam(comp, param='.'.join(name))

    if value is NA:
        if schema.factory:
            value = schema.factory()
        else:
            value = schema.default
    else:
        value = _to_native(schema, value)

    if value is NA:
        return value

    if schema.type == SchemaType.comp:
        value = load(
            context, comp, value,
            group=schema.group,
            ctype=schema.ctype,
            mixed=schema.mixed,
        )
        if schema.name:
            setattr(comp, 'name', name[-1])
        if schema.mixed:
            value, params = value
            mixed = load(
                context, value, params,
                group=schema.mixed.group,
                ctype=schema.mixed.ctype,
                mixed=schema.mixed.mixed,
            )
            setattr(value, schema.mixed.attr, mixed)

    elif schema.type == SchemaType.object:
        if not isinstance(value, dict):
            raise InvalidType(comp, prop='.'.join(name), expected='object')
        if schema.values:
            value = {
                k: _load_param(context, comp, schema.values, name + (k,), v)
                for k, v in value.items()
            }

    return value


def _to_native(schema: Schema, value: Any) -> Any:
    if schema.type in (SchemaType.date, SchemaType.datetime):
        if isinstance(value, str):
            value = parsedate(value)

    if schema.type == SchemaType.array:
        if schema.force and not isinstance(value, list):
            value = [value]

    if schema.type == SchemaType.spyna:
        if isinstance(value, str):
            value = spyna.parse()
        if isinstance(value, dict):
            value = asttoexpr(value)

    return value


def link(context: Context, comp: Component):
    for name, schema in comp.schema:
        attr = schema.attr or name
        value = getattr(comp, attr)
        value = _link(context, comp, schema, value)
        if value is not NA:
            setattr(comp, attr, value)


def _link(context: Context, comp: Component, schema: Schema, value: Any) -> None:
    if schema.inherit and value is NA:
        return getval(context, comp, schema.inherit.split('.'))

    if schema.type == SchemaType.ref:
        if schema.resolve:
            value = getval(context, comp, schema.ref.split('.') + [value])

    elif schema.type == SchemaType.object:
        if not isinstance(value, dict):
            return value
        if schema.items:
            for k, s in schema.items.items():
                if k in value:
                    _link(context, comp, s, value[k])
        if schema.values:
            for k, v in value.items():
                _link(context, comp, schema.values, v)

    elif schema.type == SchemaType.comp:
        link(context, value)

    return NA


def check(context: Context, comp: Component) -> None:
    for name, schema in comp.schema:
        _check(context, schema, comp, name, getattr(comp, name))


def _check(
    context: Context,
    schema: Schema,
    comp: Component,
    name: str,
    value: Any,
) -> None:
    if schema.required and value is NA:
        raise MissingParam(comp, param=name)

    if value is NA:
        return

    if schema.choices:
        if isinstance(schema.choices, enum.IntEnum):
            choices = [c.value for c in schema.choices]
        elif isinstance(schema.choices, enum.Enum):
            choices = schema.choices.names
        else:
            choices = schema.choices
        if value not in choices:
            raise UnknownValue(comp, param=name, choices=choices)

    if schema.type == SchemaType.ref:
        if not schema.resolve:
            container = getval(context, comp, schema.ref.split('.'))
            if value not in container:
                raise UnknownReference(comp, value=value, ref=schema.ref)

    if schema.type == SchemaType.object:
        if not isinstance(value, dict):
            raise InvalidType(comp, prop=name, expected='object')
        if schema.items:
            unknown = set(value) - set(schema.items)
            if unknown:
                unknown = ', '.join(sorted(unknown))
                raise UnknownKeys(comp, param=name, unknown=unknown)
            for k, s in schema.items.items():
                _check(context, comp, s, f'{name}.{k}', value.get(k, NA))
        elif schema.values:
            for k, v in value.items():
                _check(context, comp, schema.values, f'{name}.{k}', v)

    if schema.type == SchemaType.array:
        if not isinstance(value, list):
            raise InvalidType(comp, prop=name, expected='array')
        if schema.items:
            for v in value:
                _check(context, comp, schema.items, name, v)


def traverse(context: Context, comp: Component, command: Command):
    for name, schema in comp.schema:
        if schema.type == SchemaType.comp:
            attr = schema.attr or name
            value = getattr(comp, attr)
            link(context, value)
