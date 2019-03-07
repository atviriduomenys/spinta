from spinta.utils.nestedstruct import flatten


def test_flatten():
    assert list(flatten([{'a': 1}])) == [{'a': 1}]
    assert list(flatten([{'a': {'b': 1}}])) == [{'a.b': 1}]
    assert list(flatten([{'a': {'b': [1, 2, 3]}}])) == [
        {'a.b': 1},
        {'a.b': 2},
        {'a.b': 3},
    ]
