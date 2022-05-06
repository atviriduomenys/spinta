from typing import Any
from typing import Dict
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union
from typing import overload

from spinta import commands
from spinta import exceptions
from spinta.components import Component
from spinta.components import Config
from spinta.components import Context
from spinta.components import EntryId
from spinta.components import Node
from spinta.manifests.components import Manifest
from spinta.utils.schema import NA
from spinta.utils.schema import resolve_schema


def get_node(
    config: Config,
    manifest: Manifest,
    # MetaData entry ID, for yaml manifests it's filename, for backend manifests
    # it's UUID, for CSV tables it's row number.
    eid: EntryId,
    data: dict = None,
    *,
    # Component group from config.components.
    group: str = 'nodes',
    # If None component name will be taken from data['type'].
    ctype: str = None,
    # If parent is None, then parent is assumed to be manifest.
    parent: Component = None,
    check: bool = True,
):
    if data is not None and not isinstance(data, dict):
        raise exceptions.InvalidManifestFile(
            parent or manifest,
            manifest=manifest.name,
            eid=eid,
            error=f"Expected dict got {type(data).__name__}.",
        )

    if ctype is None:
        if 'type' not in data:
            raise exceptions.InvalidManifestFile(
                parent or manifest,
                manifest=manifest.name,
                eid=eid,
                error=f"Required parameter 'type' is not defined.",
            )

        ctype = data['type']

    if parent is None:
        # If parent is given, that means we are loading a node whose parent is
        # not manifest, that means we can't do checks on manifest.objects.

        if ctype not in manifest.objects:
            raise exceptions.InvalidManifestFile(
                manifest=manifest.name,
                eid=eid,
                error=f"Unknown type {ctype!r}.",
            )

        if check:
            if 'name' not in data:
                raise exceptions.MissingRequiredProperty(
                    manifest,
                    schema=data['path'],
                    prop='name',
                )

            if data['name'] in manifest.objects[ctype]:
                name = data['name']
                other = manifest.objects[ctype][name].eid
                raise exceptions.InvalidManifestFile(
                    manifest=manifest.name,
                    eid=eid,
                    error=(
                        f"{ctype!r} with name {name!r} already defined in "
                        f"{other}."
                    ),
                )

    if ctype not in config.components[group]:
        from spinta.components import Property
        if group == 'types' and isinstance(parent, Property):
            if ctype:
                error = (
                    f"Unknown {ctype!r} type of {parent.place!r} property "
                    f"in {parent.model.name!r} model."
                )
            else:
                error = (
                    f"Type is not given for {parent.place!r} property "
                    f"in {parent.model.name!r} model."
                )
        else:
            error = f"Unknown component {ctype!r} in {group!r}."
        raise exceptions.InvalidManifestFile(
            parent,
            manifest=manifest.name,
            eid=eid,
            error=error,
        )

    Node_ = config.components[group][ctype]
    return Node_()


@overload
def load_node(
    context: Context,
    node: Union[Node, Component],
    data: dict,
    *,
    mixed: Literal[True] = False,
    parent: Node = None,
) -> Tuple[Node, dict]:
    ...


@overload
def load_node(
    context: Context,
    node: Union[Node, Component],
    data: dict,
    *,
    mixed: Literal[False] = False,
    parent: Node = None,
) -> Node:
    ...


def load_node(
    context: Context,
    node: Union[Node, Component],
    data: dict,
    *,
    mixed: bool = False,
    parent: Union[Node, Manifest] = None,
) -> Union[
    Node,
    Tuple[Node, Dict[str, Any]]
]:
    remainder = {}
    node_schema = resolve_schema(node, Component)
    for name in set(node_schema) | set(data):
        if name not in node_schema:
            if mixed:
                remainder[name] = data[name]
                continue
            else:
                raise exceptions.UnknownParameter(node, param=name)
        schema = node_schema[name]
        if schema.get('parent'):
            attr = schema.get('attr', name)
            assert parent is not None, node
            setattr(node, attr, parent)
            continue
        value = data.get(name, NA)
        if schema.get('inherit', False) and value is NA:
            if node.parent and hasattr(node.parent, name):
                value = getattr(node.parent, name)
            else:
                value = None
        if schema.get('required', False) and (value is NA or value is None):
            raise exceptions.MissingRequiredProperty(node, prop=name)
        if value is NA:
            if 'factory' in schema:
                value = schema['factory']()
            else:
                value = schema.get('default')
        elif schema.get('type') == 'array':
            if not isinstance(value, list) and schema.get('force'):
                value = [value]
        attr = schema.get('attr', name)
        setattr(node, attr, value)
    if mixed:
        return node, remainder
    else:
        return node


def load_model_properties(
    context: Context,
    model: Node,
    Prop: Type[Node],
    data: Optional[dict],
) -> None:
    data = data or {}

    # Add build-in properties.
    data = {
        '_op': {'type': 'string'},
        '_type': {'type': 'string'},
        '_id': {'type': 'pk', 'unique': True},
        '_revision': {'type': 'string'},
        '_txn': {'type': 'string'},
        '_cid': {'type': 'integer'},
        '_created': {'type': 'datetime'},
        '_where': {'type': 'rql'},
        **data,
    }

    model.flatprops = {}
    model.leafprops = {}
    model.properties = {}
    for name, params in data.items():
        prop = Prop()
        prop.name = name
        prop.place = name
        prop.path = model.path
        prop.model = model
        prop = commands.load(context, prop, params, model.manifest)
        model.properties[name] = prop
        model.flatprops[name] = prop
