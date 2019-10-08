from spinta.utils.nestedstruct import flatten, build_select_tree


def test_flatten():
    assert list(flatten([{'a': 1}])) == [{'a': 1}]
    assert list(flatten([{'a': {'b': 1}}])) == [{'a.b': 1}]
    assert list(flatten([{'a': {'b': [1, 2, 3]}}])) == [
        {'a.b': 1},
        {'a.b': 2},
        {'a.b': 3},
    ]


def test_build_select_tree():
    assert build_select_tree(['a']) == {
        'a': set(),
    }

    assert build_select_tree(['a.b.c']) == {
        'a': {'b'},
        'a.b': {'c'},
        'a.b.c': set(),
    }

    assert build_select_tree(['a.b.c', 'a.b.c.d']) == {
        'a': {'b'},
        'a.b': {'c'},
        'a.b.c': {'d'},
        'a.b.c.d': set(),
    }

    assert build_select_tree(['a.b.c', 'a.b.d']) == {
        'a': {'b'},
        'a.b': {'c', 'd'},
        'a.b.c': set(),
        'a.b.d': set()
    }
