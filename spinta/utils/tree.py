from typing import List

import itertools


def build_path_tree(*paths):
    tree = {}
    for path in itertools.chain(*paths):
        add_path_to_tree(tree, path)
    return tree


def add_path_to_tree(tree: dict, path: str):
    parent: List[str] = []
    for part in path.split('/'):
        name = '/'.join(parent)
        parent.append(part)

        if name not in tree:
            tree[name] = [part]
        elif part not in tree[name]:
            tree[name].append(part)
            tree[name].sort()

    name = '/'.join(parent)
    if name not in tree:
        tree[name] = []
