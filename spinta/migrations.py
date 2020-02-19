from collections import defaultdict
from typing import Iterable, List

import datetime
import uuid

import jsonpatch

from spinta.exceptions import MultipleParentsError


def build_schema_graph(versions: Iterable[dict]) -> dict:
    # Builds schema graph using dict, where keys are schema version id and
    # values are a set of ids of the schema version children.
    # Root node id is `None`.
    graph = defaultdict(set)
    for version in versions:
        id_ = version['version']['id']
        parents = version['version']['parents']
        if not parents:
            graph[None].add(id_)
        else:
            for parent in parents:
                graph[parent].add(id_)
    return graph


def find_schema_leaf_ids(schema_graph: dict) -> List[str]:
    # DFS through directed schema graph, finds all leaf node ids
    leaf_ids = []
    # root node key is `None`
    stack = [None]
    discovered = set()
    while stack:
        node = stack.pop()
        if node not in discovered:
            discovered.add(node)
            leafs = schema_graph.get(node, set())

            # if no leafs - it means we are at a leaf
            # though there's no need to add `None` to leaf_ids if there's only
            # one schema version
            if not leafs and node is not None:
                leaf_ids.append(node)
            else:
                stack.extend(list(leafs))
    return leaf_ids


def find_parents(versions: Iterable[dict]) -> List[str]:
    schema_graph = build_schema_graph(versions[1:])
    leaf_ids = find_schema_leaf_ids(schema_graph)
    return leaf_ids


def get_schema_from_changes(versions: Iterable[dict]) -> dict:
    new = {}
    old = {}
    for i, version in enumerate(versions):
        if i == 0:
            new = version
            continue
        patch = version.get('changes')
        if patch:
            patch = jsonpatch.JsonPatch(patch)
            old = patch.apply(old)
    parents = find_parents(versions)
    if len(parents) > 1:
        raise MultipleParentsError()
    return old, new, parents


def get_schema_changes(old: dict, new: dict) -> List[dict]:
    return [
        change
        for change in jsonpatch.make_patch(old, new)
        if not change['path'].startswith('/version/')
    ]


def get_new_schema_version(
    old: dict,
    changes: List[dict],
    migrate: dict,
    parents: List[str],
) -> dict:
    return {
        'version': {
            'id': str(uuid.uuid4()),
            'date': datetime.datetime.now(datetime.timezone.utc).astimezone(),
            'parents': parents,
        },
        'changes': changes,
        'migrate': {
            'schema': migrate,
        },
    }
