# Domain-driven design (DDD)

Concepts used in Spinta explained using DDD vocabulary.

## Component

In Spinta *Entities* are called *Components* and are defiend in `compoents.py` files.

Components are initialized using command methods, for example:

```python
load[Context, Node, dict, Manifest] -> None
```

Here `dict` argument is a *Value object*, which is used to initialize various components. You can think of it like this:

```python
class Node:
    def __init__(self, context: Context, manifest: Manifest, data: Dict[str, Any]):
        load(context, self, data, manifest)
```

All components go throug different loading phases, and are processed using different commands:

- `load` - initialize component isntance.
- `link` - add references between components, after all components are initialized.
- `check` - run data validation and check for user errors.
- `prepare` - initialized backend if needed, for example prepare `sa.Metadata` schemas.

When all components are initialized, then they go through different set of commands, once an HTTP request is received.

## Manifest

In Spinta `Manifest` is an *Aggregate Root*, which holds all the information aboult currently loaded DSA metadata table.

## Node

*Node* is an *Entity*, which represents single metadata entry in Manifest table.

## Command

*Domain* or *Application* services in Spinta are called *Commands*. Commands use *multipledispatch* framework, to distribute commands to command methods.

Here is and example of a Command definition:

```python

@command()
def load(context: Context, node: Node, data: dict, manifest: Manifest) -> None
    ...
```

## Command method


