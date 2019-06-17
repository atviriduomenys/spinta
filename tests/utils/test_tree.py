from spinta.utils.tree import build_path_tree


def test_build_path_tree():
    paths = [
        'a',
        'a/b',
        'a/b/c',
        'a/b/c/d',
        'z/x',
        'z/x/y',
    ]
    assert build_path_tree(paths) == {
        '': ['a', 'z'],
        'a': ['b'],
        'a/b': ['c'],
        'a/b/c': ['d'],
        'a/b/c/d': [],
        'z': ['x'],
        'z/x': ['y'],
        'z/x/y': [],
    }
