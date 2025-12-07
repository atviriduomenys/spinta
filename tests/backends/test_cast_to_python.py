import datetime
from typing import Any

import pytest
import shapely.geometry

from spinta import commands
from spinta.backends import Backend
from spinta.components import Context, Property
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.types.datatype import Number, Integer, Boolean, Time, Date, DateTime, Binary, DataType, Object, Array
from spinta.types.geometry.components import Geometry

number_data = {
    "valid": ("5.5", 5.5),
    "valid negative": ("-5.5", -5.5),
    "valid integer": ("1", 1.0),
    "valid zero": ("0", 0),
    "valid comma": ("5,5", 5.5),
    "valid comma separated": ("505,550.6", 505550.6),
    "valid number given": (5.5, 5.5),
    "invalid": ("5.5.5", "5.5.5"),
    "invalid text": ("text", "text"),
    "invalid type": (True, True),
    "nan": (float("nan"), None),
}


integer_data = {
    "valid": ("5", 5),
    "valid negative": ("-5", -5),
    "valid zero": ("0", 0),
    "valid comma separated": ("505,550,000", 505550000),
    "valid integer given": (5, 5),
    "invalid": ("5.5.5", "5.5.5"),
    "invalid text": ("text", "text"),
    "invalid number": ("1.5", "1.5"),
    "invalid comma": ("5,5", "5,5"),
    "invalid type": (True, True),
    "nan": (float("nan"), None),
}


boolean_data = {
    "valid true": ("true", True),
    "valid 1": ("1", True),
    "valid on": ("on", True),
    "valid yes": ("yes", True),
    "valid false": ("false", False),
    "valid 0": ("0", False),
    "valid off": ("off", False),
    "valid no": ("no", False),
    "valid empty": ("", False),
    "valid": (True, True),
    "valid uppercase": ("TRUE", True),
    "invalid": ("5.5.5", "5.5.5"),
    "invalid text": ("text", "text"),
    "invalid number": ("1.5", "1.5"),
    "nan": (float("nan"), None),
}


time_data = {
    "valid": ("10:10:05", datetime.time(10, 10, 5)),
    "valid microseconds": ("10:10:05.0005", datetime.time(10, 10, 5, 500)),
    "invalid": ("25:01:01", "25:01:01"),
    "invalid text": ("text", "text"),
    "invalid number": ("1", "1"),
    "invalid datetime": ("2020-01-02 10:10:05", "2020-01-02 10:10:05"),
    "invalid date": ("2020-01-02", "2020-01-02"),
    "nan": (float("nan"), None),
}


date_data = {
    "valid": ("2020-01-10", datetime.date(2020, 1, 10)),
    "invalid": ("2020-01-40", "2020-01-40"),
    "invalid date backwards": ("10-01-2020", "10-01-2020"),
    "invalid datetime": ("2020-01-10T10:10:10", "2020-01-10T10:10:10"),
    "invalid text": ("text", "text"),
    "invalid number": ("1", "1"),
    "nan": (float("nan"), None),
}


datetime_data = {
    "valid": ("2020-01-10T10:10:10", datetime.datetime(2020, 1, 10, 10, 10, 10)),
    "valid space": ("2020-01-10 10:10:10", datetime.datetime(2020, 1, 10, 10, 10, 10)),
    "valid microseconds": ("2020-01-10 10:10:10.0005", datetime.datetime(2020, 1, 10, 10, 10, 10, 500)),
    "invalid": ("2020-01-40", "2020-01-40"),
    "invalid time": ("10:10:10", "10:10:10"),
    "invalid date backwards": ("10-01-2020", "10-01-2020"),
    "invalid datetime": ("2020-01-10T40:10:10", "2020-01-10T40:10:10"),
    "invalid text": ("text", "text"),
    "invalid number": ("1", "1"),
    "nan": (float("nan"), None),
}


binary_data = {
    "valid": ("dGV4dA==", b"text"),
    "valid integer": ("MTA=", b"10"),
    "valid number": ("NjQuNQ==", b"64.5"),
    "valid zero": ("MA==", b"0"),
    "invalid": ("10", "10"),
    "invalid number": ("1.0", "1.0"),
    "nan": (float("nan"), None),
}


geometry_data = {
    "valid point": ("POINT(10 10)", shapely.geometry.Point(10, 10)),
    "valid linestring": ("LINESTRING(10 10, 20 30)", shapely.geometry.LineString([[10, 10], [20, 30]])),
    "valid polygon": ("POLYGON((0 0, 0 1, 1 1, 0 0))", shapely.geometry.Polygon([[0, 0], [0, 1], [1, 1], [0, 0]])),
    "invalid": ("POINT(10, 10)", "POINT(10, 10)"),
    "invalid text": ("text", "text"),
    "invalid number": ("1.5", "1.5"),
    "nan": (float("nan"), None),
}


@pytest.mark.parametrize("given, expected", number_data.values(), ids=number_data.keys())
def test_cast_backend_to_python_number(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), Number(), Backend(), given) == expected


@pytest.mark.parametrize("given, expected", integer_data.values(), ids=integer_data.keys())
def test_cast_backend_to_python_integer(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), Integer(), Backend(), given) == expected


@pytest.mark.parametrize("given, expected", boolean_data.values(), ids=boolean_data.keys())
def test_cast_backend_to_python_boolean(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), Boolean(), Backend(), given) == expected


@pytest.mark.parametrize("given, expected", time_data.values(), ids=time_data.keys())
def test_cast_backend_to_python_time(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), Time(), Backend(), given) == expected


@pytest.mark.parametrize("given, expected", date_data.values(), ids=date_data.keys())
def test_cast_backend_to_python_date(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), Date(), Backend(), given) == expected


@pytest.mark.parametrize("given, expected", datetime_data.values(), ids=datetime_data.keys())
def test_cast_backend_to_python_datetime(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), DateTime(), Backend(), given) == expected


@pytest.mark.parametrize("given, expected", binary_data.values(), ids=binary_data.keys())
def test_cast_backend_to_python_binary(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), Binary(), Backend(), given) == expected


@pytest.mark.parametrize("given, expected", geometry_data.values(), ids=geometry_data.keys())
def test_cast_backend_to_python_geometry(given: Any, expected: Any):
    assert commands.cast_backend_to_python(Context("empty"), Geometry(), Backend(), given) == expected


def test_cast_backend_to_python_ref(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property     | type    | ref                    | source  | prepare | access
    example                      |         |                        |         |         |
      |   |   | Country          |         | int_prop, boolean_prop | COUNTRY |         |
      |   |   |   | int_prop     | integer |                        | INT     |         | open
      |   |   |   | boolean_prop | boolean |                        | BOOLEAN |         | open
      |   |   | City             |         | name                   | CITY    |         |
      |   |   |   | name         | string  |                        | NAME    |         | open
      |   |   |   | country      | ref     | Country                | COUNTRY |         | open
    """,
    )
    model = commands.get_model(context, manifest, "example/City")

    given = {"int_prop": "5", "boolean_prop": "TRUE"}
    assert commands.cast_backend_to_python(context, model.properties.get("country"), Backend(), given) == {
        "int_prop": 5,
        "boolean_prop": True,
    }


def test_cast_backend_to_python_denorm(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property             | type    | ref      | source  | prepare | access
    example                              |         |          |         |         |
      |   |   | Country                  |         | int_prop | COUNTRY |         |
      |   |   |   | int_prop             | integer |          | INT     |         | open
      |   |   |   | boolean_prop         | boolean |          | BOOLEAN |         | open
      |   |   | City                     |         | name     | CITY    |         |
      |   |   |   | name                 | string  |          | NAME    |         | open
      |   |   |   | country              | ref     | Country  | COUNTRY |         | open
      |   |   |   | country.boolean_prop |         |          |         |         | open
    """,
    )
    model = commands.get_model(context, manifest, "example/City")

    given = {"int_prop": "5", "boolean_prop": "TRUE"}
    assert commands.cast_backend_to_python(context, model.properties.get("country"), Backend(), given) == {
        "int_prop": 5,
        "boolean_prop": True,
    }


def test_cast_backend_to_python_external_denorm(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | b | m | property             | type    | ref      | source  | prepare | access | level
    example                              |         |          |         |         |        |
      |   |   | Country                  |         | int_prop | COUNTRY |         |        |
      |   |   |   | int_prop             | integer |          | INT     |         | open   |
      |   |   |   | boolean_prop         | boolean |          | BOOLEAN |         | open   |
      |   |   | City                     |         | name     | CITY    |         |        |
      |   |   |   | name                 | string  |          | NAME    |         | open   |
      |   |   |   | country              | ref     | Country  | COUNTRY |         | open   | 3
      |   |   |   | country.boolean_prop |         |          |         |         | open   |
    """,
    )
    model = commands.get_model(context, manifest, "example/City")

    given = {"int_prop": "5", "boolean_prop": "TRUE"}
    assert commands.cast_backend_to_python(context, model.properties.get("country"), Backend(), given) == {
        "int_prop": 5,
        "boolean_prop": True,
    }


def test_cast_backend_to_python_object():
    obj = Object()
    obj.properties = {
        "int_prop": _create_property("int_prop", Integer()),
        "boolean_prop": _create_property("boolean_prop", Boolean()),
    }
    given = {"int_prop": "5", "boolean_prop": "TRUE"}
    assert commands.cast_backend_to_python(Context("empty"), obj, Backend(), given) == {
        "int_prop": 5,
        "boolean_prop": True,
    }


def test_cast_backend_to_python_array():
    array = Array()
    array.items = _create_property("int_prop", Integer())
    given = [0, "5", "5.5", "text"]
    assert commands.cast_backend_to_python(Context("empty"), array, Backend(), given) == [0, 5, "5.5", "text"]


def _create_property(name: str, dtype: DataType):
    prop = Property()
    prop.name = name
    prop.dtype = dtype
    return prop
