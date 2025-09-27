import pytest

from spinta.components import Model, Property
from spinta.types.text.components import Text
from spinta.utils.nestedstruct import flatten, build_select_tree, sepgetter, flat_dicts_to_nested


def test_flatten():
    assert list(flatten([{"a": 1}])) == [{"a": 1}]
    assert list(flatten([{"a": {"b": 1}}])) == [{"a.b": 1}]
    assert list(flatten([{"a": {"b": [1, 2, 3]}}])) == [
        {"a.b[]": 1},
        {"a.b[]": 2},
        {"a.b[]": 3},
    ]


def test_flatten_empty_lists():
    lst = [{"a": 1, "b": [], "c": []}]
    assert list(flatten(lst)) == [{"a": 1}]


def test_flatten_dict():
    assert list(flatten({"a": 1})) == [{"a": 1}]


def test_flatten_nested_dict():
    assert list(flatten({"a": {"b": {"c": 1}}})) == [{"a.b.c": 1}]


def test_flatten_list_in_list():
    assert list(flatten({"a": [[1], [2, [3, [4]]]]})) == [
        {"a[][]": 1},
        {"a[][]": 2},
        {"a[][][]": 3},
        {"a[][][][]": 4},
    ]


def test_flatten_two_lists():
    lst = [
        {"a": 1, "b": [2, 3], "c": [4, 5]},
        {"a": 6, "b": [7], "c": [8, 9]},
    ]
    assert list(flatten(lst)) == [
        {"a": 1, "b[]": 2, "c[]": 4},
        {"a": 1, "b[]": 2, "c[]": 5},
        {"a": 1, "b[]": 3, "c[]": 4},
        {"a": 1, "b[]": 3, "c[]": 5},
        {"a": 6, "b[]": 7, "c[]": 8},
        {"a": 6, "b[]": 7, "c[]": 9},
    ]


def test_flatten_text_type():
    prop = Property()
    text_dtype = Text()
    text_dtype.langs = {"en": Property()}
    prop.dtype = text_dtype
    model = Model()
    model.properties["name"] = prop
    assert list(flatten({"name": {"en": "Test"}}, sep_getter=sepgetter(model))) == [{"name@en": "Test"}]
    assert list(flatten({"name": {"en": "Test"}})) == [{"name.en": "Test"}]


def test_flatten_list_dict():
    lst = [{"a": [{"b": 1, "c": [1]}, {"b": 2, "c": [2]}]}]
    assert list(flatten(lst)) == [
        {"a[].b": 1, "a[].c[]": 1},
        {"a[].b": 2, "a[].c[]": 2},
    ]


def test_flatten_memory_usage():
    i = 0
    data = [
        {"a": 1},
        {"a": 2},
        {"a": 3},
        {"a": 4},
    ]

    def it():
        nonlocal i
        for item in data:
            i += 1
            yield item

    res = flatten(it())
    assert i == 0
    next(res)
    assert i == 1
    next(res)
    assert i == 2
    next(res)
    assert i == 3


def test_build_select_tree():
    assert build_select_tree(["a"]) == {
        "a": set(),
    }

    assert build_select_tree(["a.b.c"]) == {
        "a": {"b"},
        "a.b": {"c"},
        "a.b.c": set(),
    }

    assert build_select_tree(["a.b.c", "a.b.c.d"]) == {
        "a": {"b"},
        "a.b": {"c"},
        "a.b.c": {"d"},
        "a.b.c.d": set(),
    }

    assert build_select_tree(["a.b.c", "a.b.d"]) == {"a": {"b"}, "a.b": {"c", "d"}, "a.b.c": set(), "a.b.d": set()}


@pytest.mark.parametrize(
    "data, list_keys, result",
    [
        ({"notes.note": [0, 1, 2]}, [], {"notes": {"note": [0, 1, 2]}}),
        ({"notes.note": [0, 1, 2]}, ["notes"], {"notes": [{"note": 0}, {"note": 1}, {"note": 2}]}),
        ({"notes.note": [0, 1, 2]}, ["notes.note"], {"notes": {"note": [0, 1, 2]}}),
        ({"notes.note": [[0, 1], [2]]}, ["notes", "notes.note"], {"notes": [{"note": [0, 1]}, {"note": [2]}]}),
    ],
    ids=["d0", "d1", "d2", "d3"],
)
def test_flat_to_nested(data: dict, list_keys: list, result: dict):
    assert flat_dicts_to_nested(data, list_keys) == result
