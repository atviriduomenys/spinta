import collections
import itertools


def build_path_tree(*paths):
    tree = collections.defaultdict(set)
    for path in itertools.chain(*paths):
        parent = []
        for part in path.split('/'):
            tree['/'.join(parent)].add(part)
            parent.append(part)
    return {k: sorted(v) for k, v in tree.items()}
