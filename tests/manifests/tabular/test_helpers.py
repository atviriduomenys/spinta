import pytest

from spinta.exceptions import PropertyNotFound
from spinta.manifests.tabular.helpers import normalizes_columns, _parse_dtype_string


def test_normalizes_columns_short_names():
    assert normalizes_columns(["d", "r", "b", "m"]) == [
        "dataset",
        "resource",
        "base",
        "model",
    ]


def test_normalizes_columns_strip_unknown_columns():
    assert normalizes_columns(["m", "property", "unknown"]) == [
        "model",
        "property",
    ]


def test_normalizes_columns_strip_unknown_columns_2():
    assert normalizes_columns(["unknown"]) == []


def test_normalizes_columns_strip_unknown_columns_3():
    assert normalizes_columns(["id", None, None]) == ["id"]


def test_normalizes_columns_check_unknown_columns():
    with pytest.raises(PropertyNotFound):
        normalizes_columns(["unknown", "model"])


def test_parse_dtype_string():
    res = _parse_dtype_string("geometry")
    assert res == {
        "type": "geometry",
        "type_args": [],
        "required": False,
        "unique": False,
        "error": None,
    }


def test_parse_dtype_string_with_args():
    res = _parse_dtype_string("geometry(linestringm, 3345)")
    assert res == {
        "type": "geometry",
        "type_args": ["linestringm", "3345"],
        "required": False,
        "unique": False,
        "error": None,
    }


def test_parse_dtype_string_with_required():
    res = _parse_dtype_string("geometry required")
    assert res == {
        "type": "geometry",
        "type_args": [],
        "required": True,
        "unique": False,
        "error": None,
    }


def test_parse_dtype_string_with_args_and_required():
    res = _parse_dtype_string("geometry(linestringm, 3345) required")
    assert res == {
        "type": "geometry",
        "type_args": ["linestringm", "3345"],
        "required": True,
        "unique": False,
        "error": None,
    }


def test_parse_dtype_string_with_unique():
    res = _parse_dtype_string("geometry unique")
    assert res == {
        "type": "geometry",
        "type_args": [],
        "required": False,
        "unique": True,
        "error": None,
    }


def test_parse_dtype_string_with_args_and_unique():
    res = _parse_dtype_string("geometry(linestringm, 3345) unique")
    assert res == {
        "type": "geometry",
        "type_args": ["linestringm", "3345"],
        "required": False,
        "unique": True,
        "error": None,
    }


def test_parse_dtype_string_with_unique_and_required():
    res = _parse_dtype_string("geometry unique required")
    assert res == {
        "type": "geometry",
        "type_args": [],
        "required": True,
        "unique": True,
        "error": None,
    }


def test_parse_dtype_string_with_args_unique_and_required():
    res = _parse_dtype_string("geometry(linestringm, 3345) required unique")
    assert res == {
        "type": "geometry",
        "type_args": ["linestringm", "3345"],
        "required": True,
        "unique": True,
        "error": None,
    }


def test_parse_dtype_string_with_error():
    res = _parse_dtype_string("geometry wrong_arg")
    assert res == {
        "type": "geometry",
        "type_args": [],
        "required": False,
        "unique": False,
        "error": "Invalid type arguments: wrong_arg.",
    }


def test_parse_dtype_string_with_multipe_error():
    res = _parse_dtype_string("geometry(linestringm, 3345) wrong_arg1 wrong_arg2")
    assert res == {
        "type": "geometry",
        "type_args": ["linestringm", "3345"],
        "required": False,
        "unique": False,
        "error": "Invalid type arguments: wrong_arg1, wrong_arg2.",
    }


def test_parse_dtype_string_with_multiple_spaces():
    res = _parse_dtype_string("geometry  required ")
    assert res == {
        "type": "geometry",
        "type_args": [],
        "required": True,
        "unique": False,
        "error": None,
    }
