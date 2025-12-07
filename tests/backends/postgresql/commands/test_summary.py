import pytest

from spinta.backends.postgresql.commands.summary import extract_uuid
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.manifest import bootstrap_manifest
from _pytest.fixtures import FixtureRequest


def float_equals(a: float, b: float, epsilon=1e-9):
    return abs(a - b) < epsilon


def dict_equals(dict1: dict, dict2: dict, epsilon=1e-9):
    if len(dict1) != len(dict2):
        return False

    for key in dict1:
        if key not in dict2:
            return False

        value1 = dict1[key]
        value2 = dict2[key]

        if isinstance(value1, float) and isinstance(value2, float):
            if not float_equals(value1, value2, epsilon):
                return False
        elif value1 != value2:
            return False

    return True


time_units_data = {
    "minutes": ("5T", "10:00:00", "10:02:30", "18:17:30"),
    "seconds": ("15S", "10:00:00", "10:00:07.500000", "10:24:52.500000"),
    "milliseconds": ("200L", "10:00:00", "10:00:00.100000", "10:00:19.900000"),
    "microseconds": ("400U", "10:00:00", "2023-01-08 00:00:00", "2026-10-25 00:00:00"),
}

time_ignore_units_data = {
    "years": ("2Y", "10:00:00", "10:00:30", "11:39:30"),
    "months": ("2M", "10:00:00", "10:00:30", "11:39:30"),
    "quarters": ("2Q", "10:00:00", "10:00:30", "11:39:30"),
    "weeks": ("2W", "10:00:00", "10:00:30", "11:39:30"),
    "days": ("2D", "10:00:00", "10:00:30", "11:39:30"),
    "hours": ("1H", "10:00:00", "10:00:30", "11:39:30"),
    "minutes out of bounds": ("5T", "22:00:00", "22:00:30", "23:39:30"),
    "seconds out of bounds": ("15S", "23:50:00", "23:50:00.500000", "23:51:39.500000"),
    "milliseconds out of bounds": ("200L", "23:59:59", "23:59:59.000500", "23:59:59.099500"),
    "microseconds out of bounds": ("90000U", "23:59:59", "23:59:59.000500", "23:59:59.099500"),
    "nanoseconds (not implemented)": ("800N", "23:59:59", "23:59:59.000500", "23:59:59.099500"),
}

datetime_units_data = {
    "years": ("10Y", "2023-01-01T00:00:00", "2028-01-01 05:02:24", "3017-12-31 18:57:36"),
    "months": ("2M", "2023-01-01T00:00:00", "2023-01-31 10:26:24", "2039-08-01 13:33:36"),
    "quarters": ("3Q", "2023-01-01T00:00:00", "2023-05-17 23:16:48", "2097-08-17 00:43:12"),
    "weeks": ("2W", "2023-01-01T00:00:00", "2023-01-08 00:00:00", "2026-10-25 00:00:00"),
    "days": ("14D", "2023-01-01T00:00:00", "2023-01-08 00:00:00", "2026-10-25 00:00:00"),
    "hours": ("60H", "2023-01-01T00:00:00", "2023-01-02 06:00:00", "2023-09-06 18:00:00"),
    "minutes": ("15T", "2023-01-01T00:00:00", "2023-01-01 00:07:30", "2023-01-02 00:52:30"),
    "seconds": ("20S", "2023-01-01T00:00:00", "2023-01-01 00:00:10", "2023-01-01 00:33:10"),
    "milliseconds": ("450L", "2023-01-01T00:00:00", "2023-01-01 00:00:00.225000", "2023-01-01 00:00:44.775000"),
    "microseconds": ("800U", "2023-01-01T00:00:00", "2023-01-01 00:00:00.000400", "2023-01-01 00:00:00.079600"),
}

datetime_ignore_units_data = {
    "nanoseconds (not implemented)": ("10N", "2023-01-01T00:00:00", "2023-01-01 12:00:00", "2023-04-10 12:00:00"),
}

date_units_data = {
    "years": ("10Y", "2023-01-01", "2028-01-01 05:02:24", "3017-12-31 18:57:36"),
    "months": ("2M", "2023-01-01", "2023-01-31 10:26:24", "2039-08-01 13:33:36"),
    "quarters": ("3Q", "2023-01-01", "2023-05-17 23:16:48", "2097-08-17 00:43:12"),
    "weeks": ("2W", "2023-01-01", "2023-01-08 00:00:00", "2026-10-25 00:00:00"),
    "days": ("14D", "2023-01-01", "2023-01-08 00:00:00", "2026-10-25 00:00:00"),
}

date_ignore_units_data = {
    "hours": ("60H", "2023-01-01", "2023-01-01 12:00:00", "2023-04-10 12:00:00"),
    "minutes": ("15T", "2023-01-01", "2023-01-01 12:00:00", "2023-04-10 12:00:00"),
    "seconds": ("20S", "2023-01-01", "2023-01-01 12:00:00", "2023-04-10 12:00:00"),
    "milliseconds": ("450L", "2023-01-01", "2023-01-01 12:00:00", "2023-04-10 12:00:00"),
    "microseconds": ("800U", "2023-01-01", "2023-01-01 12:00:00", "2023-04-10 12:00:00"),
    "nanoseconds (not implemented)": ("10N", "2023-01-01", "2023-01-01 12:00:00", "2023-04-10 12:00:00"),
}


def test_summary_integer_no_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/integer", ["insert", "getall", "search"])
    app.post("/example/summary/integer/Test", json={"value": 0})
    app.post("/example/summary/integer/Test", json={"value": 3})
    resp_410 = app.post("/example/summary/integer/Test", json={"value": 410})
    resp_707 = app.post("/example/summary/integer/Test", json={"value": 707})
    resp_1000 = app.post("/example/summary/integer/Test", json={"value": 1000})
    response = app.get("/example/summary/integer/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {"bin": 5, "count": 2, "_type": "example/summary/integer/Test"})
    assert dict_equals(
        json_response["_data"][40],
        {
            "bin": 405,
            "count": 1,
            "_type": "example/summary/integer/Test",
            "_id": resp_410.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][70],
        {
            "bin": 705,
            "count": 1,
            "_type": "example/summary/integer/Test",
            "_id": resp_707.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": 995,
            "count": 1,
            "_type": "example/summary/integer/Test",
            "_id": resp_1000.json()["_id"],
        },
    )


def test_summary_integer_with_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/integer", ["insert", "getall", "search"])
    resp_0 = app.post("/example/summary/integer/Test", json={"value": 0})
    resp_3 = app.post("/example/summary/integer/Test", json={"value": 3})
    resp_21 = app.post("/example/summary/integer/Test", json={"value": 21})
    resp_55 = app.post("/example/summary/integer/Test", json={"value": 55})
    resp_64 = app.post("/example/summary/integer/Test", json={"value": 64})
    response = app.get("/example/summary/integer/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {"bin": 0.32, "count": 1, "_type": "example/summary/integer/Test", "_id": resp_0.json()["_id"]},
    )
    assert dict_equals(
        json_response["_data"][4],
        {
            "bin": 2.88,
            "count": 1,
            "_type": "example/summary/integer/Test",
            "_id": resp_3.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][32],
        {
            "bin": 20.8,
            "count": 1,
            "_type": "example/summary/integer/Test",
            "_id": resp_21.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][85],
        {
            "bin": 54.72,
            "count": 1,
            "_type": "example/summary/integer/Test",
            "_id": resp_55.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": 63.68,
            "count": 1,
            "_type": "example/summary/integer/Test",
            "_id": resp_64.json()["_id"],
        },
    )


def test_summary_integer_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/integer", ["insert", "getall", "search"])
    app.post("/example/summary/integer/Test", json={})
    response = app.get("/example/summary/integer/Test/:summary/value")
    json_response = response.json()

    assert json_response["_data"] == []


def test_summary_integer_single_item(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/integer    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/integer", ["insert", "getall", "search"])
    resp_64 = app.post("/example/summary/integer/Test", json={"value": 64})
    response = app.get("/example/summary/integer/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {"bin": 64.5, "count": 1, "_type": "example/summary/integer/Test", "_id": resp_64.json()["_id"]},
    )
    assert dict_equals(json_response["_data"][99], {"bin": 163.5, "count": 0, "_type": "example/summary/integer/Test"})


def test_summary_integer_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/integer/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            {dataset_name} |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | integer |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    resp_64 = app.post(f"/{dataset_name}/Test", json={"value": 64})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0], {"bin": 64.5, "count": 1, "_type": model_name, "_id": resp_64.json()["_id"]}
    )
    assert dict_equals(json_response["_data"][99], {"bin": 163.5, "count": 0, "_type": model_name})


def test_summary_number_no_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/number", ["insert", "getall", "search"])
    app.post("/example/summary/number/Test", json={"value": 0.0})
    app.post("/example/summary/number/Test", json={"value": 0.003})
    resp_410 = app.post("/example/summary/number/Test", json={"value": 0.410})
    resp_707 = app.post("/example/summary/number/Test", json={"value": 0.707})
    resp_1000 = app.post("/example/summary/number/Test", json={"value": 1.0})
    response = app.get("/example/summary/number/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {"bin": 0.005, "count": 2, "_type": "example/summary/number/Test"})
    assert dict_equals(
        json_response["_data"][40],
        {
            "bin": 0.405,
            "count": 1,
            "_type": "example/summary/number/Test",
            "_id": resp_410.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][70],
        {
            "bin": 0.705,
            "count": 1,
            "_type": "example/summary/number/Test",
            "_id": resp_707.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": 0.995,
            "count": 1,
            "_type": "example/summary/number/Test",
            "_id": resp_1000.json()["_id"],
        },
    )


def test_summary_number_with_fraction(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/number", ["insert", "getall", "search"])
    resp_0 = app.post("/example/summary/number/Test", json={"value": 0.0})
    resp_3 = app.post("/example/summary/number/Test", json={"value": 0.03})
    resp_21 = app.post("/example/summary/number/Test", json={"value": 0.21})
    resp_55 = app.post("/example/summary/number/Test", json={"value": 0.55})
    resp_64 = app.post("/example/summary/number/Test", json={"value": 0.64})
    response = app.get("/example/summary/number/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {"bin": 0.0032, "count": 1, "_type": "example/summary/number/Test", "_id": resp_0.json()["_id"]},
    )
    assert dict_equals(
        json_response["_data"][4],
        {
            "bin": 0.0288,
            "count": 1,
            "_type": "example/summary/number/Test",
            "_id": resp_3.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][32],
        {
            "bin": 0.208,
            "count": 1,
            "_type": "example/summary/number/Test",
            "_id": resp_21.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][85],
        {
            "bin": 0.5472,
            "count": 1,
            "_type": "example/summary/number/Test",
            "_id": resp_55.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": 0.6368,
            "count": 1,
            "_type": "example/summary/number/Test",
            "_id": resp_64.json()["_id"],
        },
    )


def test_summary_number_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/number", ["insert", "getall", "search"])
    app.post("/example/summary/number/Test", json={})
    response = app.get("/example/summary/number/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 0


def test_summary_number_single_item(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/number    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/number", ["insert", "getall", "search"])
    resp_5 = app.post("/example/summary/number/Test", json={"value": 5})
    response = app.get("/example/summary/number/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {"bin": 5.5, "count": 1, "_type": "example/summary/number/Test", "_id": resp_5.json()["_id"]},
    )
    assert dict_equals(json_response["_data"][99], {"bin": 104.5, "count": 0, "_type": "example/summary/number/Test"})


def test_summary_number_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/number/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            {dataset_name}    |        |         |         |
              |   |   | Test       |        | value    |         | 
              |   |   |   | value    | number |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    resp_5 = app.post(model_name, json={"value": 5})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0], {"bin": 5.5, "count": 1, "_type": model_name, "_id": resp_5.json()["_id"]}
    )
    assert dict_equals(json_response["_data"][99], {"bin": 104.5, "count": 0, "_type": model_name})


def test_summary_boolean(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/boolean    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | boolean |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/boolean", ["insert", "getall", "search"])
    app.post("/example/summary/boolean/Test", json={"value": True})
    app.post("/example/summary/boolean/Test", json={"value": True})
    app.post("/example/summary/boolean/Test", json={"value": True})
    resp = app.post("/example/summary/boolean/Test", json={"value": False})
    response = app.get("/example/summary/boolean/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(
        json_response["_data"][0],
        {"bin": False, "count": 1, "_type": "example/summary/boolean/Test", "_id": resp.json()["_id"]},
    )
    assert dict_equals(json_response["_data"][1], {"bin": True, "count": 3, "_type": "example/summary/boolean/Test"})


def test_summary_boolean_missing(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/boolean    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | boolean |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/boolean", ["insert", "getall", "search"])
    resp = app.post("/example/summary/boolean/Test", json={"value": False})
    response = app.get("/example/summary/boolean/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(
        json_response["_data"][0],
        {"bin": False, "count": 1, "_type": "example/summary/boolean/Test", "_id": resp.json()["_id"]},
    )
    assert dict_equals(json_response["_data"][1], {"bin": True, "count": 0, "_type": "example/summary/boolean/Test"})


def test_summary_boolean_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/boolean    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | boolean |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/boolean", ["insert", "getall", "search"])
    app.post("/example/summary/boolean/Test", json={})
    response = app.get("/example/summary/boolean/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(json_response["_data"][0], {"bin": False, "count": 0, "_type": "example/summary/boolean/Test"})
    assert dict_equals(json_response["_data"][1], {"bin": True, "count": 0, "_type": "example/summary/boolean/Test"})


def test_summary_boolean_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/boolean/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            {dataset_name}    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | boolean |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    app.post(model_name, json={"value": True})
    app.post(model_name, json={"value": True})
    app.post(model_name, json={"value": True})
    resp = app.post(model_name, json={"value": False})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(
        json_response["_data"][0], {"bin": False, "count": 1, "_type": model_name, "_id": resp.json()["_id"]}
    )
    assert dict_equals(json_response["_data"][1], {"bin": True, "count": 3, "_type": model_name})


def test_summary_string_enum_inside(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
              |   |   |   |          | enum   |         | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/string", ["insert", "getall", "search"])
    resp = app.post("/example/summary/string/Test", json={"value": "TEST1"})
    app.post("/example/summary/string/Test", json={"value": "TEST2"})
    app.post("/example/summary/string/Test", json={"value": "TEST2"})
    app.post("/example/summary/string/Test", json={"value": "TEST3"})
    app.post("/example/summary/string/Test", json={"value": "TEST3"})
    app.post("/example/summary/string/Test", json={"value": "TEST3"})
    response = app.get("/example/summary/string/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {"bin": "TEST1", "count": 1, "_type": "example/summary/string/Test", "_id": resp.json()["_id"]},
    )
    assert dict_equals(json_response["_data"][1], {"bin": "TEST2", "count": 2, "_type": "example/summary/string/Test"})
    assert dict_equals(json_response["_data"][2], {"bin": "TEST3", "count": 3, "_type": "example/summary/string/Test"})


def test_summary_string_enum_outside(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   |   |          | enum   | test    | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string | test    |        |         | open   |

            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/string", ["insert", "getall", "search"])
    resp = app.post("/example/summary/string/Test", json={"value": "TEST1"})
    app.post("/example/summary/string/Test", json={"value": "TEST2"})
    app.post("/example/summary/string/Test", json={"value": "TEST2"})
    app.post("/example/summary/string/Test", json={"value": "TEST3"})
    app.post("/example/summary/string/Test", json={"value": "TEST3"})
    app.post("/example/summary/string/Test", json={"value": "TEST3"})
    response = app.get("/example/summary/string/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {"bin": "TEST1", "count": 1, "_type": "example/summary/string/Test", "_id": resp.json()["_id"]},
    )
    assert dict_equals(json_response["_data"][1], {"bin": "TEST2", "count": 2, "_type": "example/summary/string/Test"})
    assert dict_equals(json_response["_data"][2], {"bin": "TEST3", "count": 3, "_type": "example/summary/string/Test"})


def test_summary_string_missing(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
              |   |   |   |          | enum   |         | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/string", ["insert", "getall", "search"])
    resp = app.post("/example/summary/string/Test", json={"value": "TEST1"})
    app.post("/example/summary/string/Test", json={"value": "TEST2"})
    app.post("/example/summary/string/Test", json={"value": "TEST2"})
    response = app.get("/example/summary/string/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {"bin": "TEST1", "count": 1, "_type": "example/summary/string/Test", "_id": resp.json()["_id"]},
    )
    assert dict_equals(json_response["_data"][1], {"bin": "TEST2", "count": 2, "_type": "example/summary/string/Test"})
    assert dict_equals(json_response["_data"][2], {"bin": "TEST3", "count": 0, "_type": "example/summary/string/Test"})


def test_summary_string_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
              |   |   |   |          | enum   |         | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/string", ["insert", "getall", "search"])
    response = app.get("/example/summary/string/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {"bin": "TEST1", "count": 0, "_type": "example/summary/string/Test"})
    assert dict_equals(json_response["_data"][1], {"bin": "TEST2", "count": 0, "_type": "example/summary/string/Test"})
    assert dict_equals(json_response["_data"][2], {"bin": "TEST3", "count": 0, "_type": "example/summary/string/Test"})


def test_summary_string_no_enum(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            example/summary/string   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/string", ["insert", "getall", "search"])
    app.post("/example/summary/string/Test", json={"value": "TEST2"})
    response = app.get("/example/summary/string/Test/:summary/value")

    assert response.status_code == 500


def test_summary_string_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/string/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | source | prepare | access | uri
            {dataset_name}   |        |         |        |         |        |
              |   |   | Test       |        |         |        |         |        |
              |   |   |   | value    | string |         |        |         | open   |
              |   |   |   |          | enum   |         | 1      | "TEST1" | open   |
              |   |   |   |          |        |         | 2      | "TEST2" | open   |
              |   |   |   |          |        |         | 3      | "TEST3" | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    resp = app.post(model_name, json={"value": "TEST1"})
    app.post(model_name, json={"value": "TEST2"})
    app.post(model_name, json={"value": "TEST2"})
    app.post(model_name, json={"value": "TEST3"})
    app.post(model_name, json={"value": "TEST3"})
    app.post(model_name, json={"value": "TEST3"})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0], {"bin": "TEST1", "count": 1, "_type": model_name, "_id": resp.json()["_id"]}
    )
    assert dict_equals(json_response["_data"][1], {"bin": "TEST2", "count": 2, "_type": model_name})
    assert dict_equals(json_response["_data"][2], {"bin": "TEST3", "count": 3, "_type": model_name})


def test_summary_time(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/time", ["insert", "getall", "search"])
    app.post("/example/summary/time/Test", json={"value": "10:00:00"})
    app.post("/example/summary/time/Test", json={"value": "10:00:05"})
    resp_middle = app.post("/example/summary/time/Test", json={"value": "14:08:03"})
    resp_last = app.post("/example/summary/time/Test", json={"value": "22:12:00"})
    response = app.get("/example/summary/time/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0], {"bin": "10:03:39.600000", "count": 2, "_type": "example/summary/time/Test"}
    )
    assert dict_equals(
        json_response["_data"][33],
        {
            "bin": "14:05:13.200000",
            "count": 1,
            "_type": "example/summary/time/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "22:08:20.400000",
            "count": 1,
            "_type": "example/summary/time/Test",
            "_id": resp_last.json()["_id"],
        },
    )


@pytest.mark.parametrize("ref, given, expected_0, expected_99", time_units_data.values(), ids=time_units_data.keys())
def test_summary_time_single_given_custom_units(
    rc: RawConfig, postgresql: str, request: FixtureRequest, ref: str, given: str, expected_0: str, expected_99: str
):
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | {ref} | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/time", ["insert", "getall", "search"])
    resp = app.post("/example/summary/time/Test", json={"value": given})
    response = app.get("/example/summary/time/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": expected_0,
            "count": 1,
            "_type": "example/summary/time/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": expected_99,
            "count": 0,
            "_type": "example/summary/time/Test",
        },
    )


@pytest.mark.parametrize(
    "ref, given, expected_0, expected_99", time_ignore_units_data.values(), ids=time_ignore_units_data.keys()
)
def test_summary_time_single_given_custom_units(
    rc: RawConfig, postgresql: str, request: FixtureRequest, ref: str, given: str, expected_0: str, expected_99: str
):
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | {ref} | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/time", ["insert", "getall", "search"])
    resp = app.post("/example/summary/time/Test", json={"value": given})
    response = app.get("/example/summary/time/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": expected_0,
            "count": 1,
            "_type": "example/summary/time/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": expected_99,
            "count": 0,
            "_type": "example/summary/time/Test",
        },
    )


def test_summary_time_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/time    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time | 200L | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/time", ["insert", "getall", "search"])
    response = app.get("/example/summary/time/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 0
    assert json_response["_data"] == []


def test_summary_time_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/time/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            {dataset_name}    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | time |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    app.post(model_name, json={"value": "10:00:00"})
    app.post(model_name, json={"value": "10:00:05"})
    resp_middle = app.post(model_name, json={"value": "14:08:03"})
    resp_last = app.post(model_name, json={"value": "22:12:00"})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {"bin": "10:03:39.600000", "count": 2, "_type": model_name})
    assert dict_equals(
        json_response["_data"][33],
        {
            "bin": "14:05:13.200000",
            "count": 1,
            "_type": model_name,
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "22:08:20.400000",
            "count": 1,
            "_type": model_name,
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_datetime(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime |   | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/datetime", ["insert", "getall", "search"])
    app.post("/example/summary/datetime/Test", json={"value": "2023-01-01T10:00:00"})
    app.post("/example/summary/datetime/Test", json={"value": "2023-01-02T10:00:05"})
    resp_middle = app.post("/example/summary/datetime/Test", json={"value": "2023-09-12T14:08:03"})
    resp_last = app.post("/example/summary/datetime/Test", json={"value": "2023-12-29T22:12:00"})
    response = app.get("/example/summary/datetime/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {"bin": "2023-01-03 05:30:03.600000", "count": 2, "_type": "example/summary/datetime/Test"},
    )
    assert dict_equals(
        json_response["_data"][70],
        {
            "bin": "2023-09-13 23:38:27.600000",
            "count": 1,
            "_type": "example/summary/datetime/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "2023-12-28 02:41:56.400000",
            "count": 1,
            "_type": "example/summary/datetime/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_datetime_single_given_no_units(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime |   | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/datetime", ["insert", "getall", "search"])
    resp = app.post("/example/summary/datetime/Test", json={"value": "2023-01-01T10:00:00"})
    response = app.get("/example/summary/datetime/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": "2023-01-01 22:00:00",
            "count": 1,
            "_type": "example/summary/datetime/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "2023-04-10 22:00:00",
            "count": 0,
            "_type": "example/summary/datetime/Test",
        },
    )


@pytest.mark.parametrize(
    "ref, given, expected_0, expected_99", datetime_units_data.values(), ids=datetime_units_data.keys()
)
def test_summary_datetime_single_given_custom_units(
    rc: RawConfig, postgresql: str, request: FixtureRequest, ref: str, given: str, expected_0: str, expected_99: str
):
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime | {ref} | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/datetime", ["insert", "getall", "search"])
    resp = app.post("/example/summary/datetime/Test", json={"value": given})
    response = app.get("/example/summary/datetime/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": expected_0,
            "count": 1,
            "_type": "example/summary/datetime/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": expected_99,
            "count": 0,
            "_type": "example/summary/datetime/Test",
        },
    )


@pytest.mark.parametrize(
    "ref, given, expected_0, expected_99", datetime_ignore_units_data.values(), ids=datetime_ignore_units_data.keys()
)
def test_summary_datetime_single_given_custom_units_ignore(
    rc: RawConfig, postgresql: str, request: FixtureRequest, ref: str, given: str, expected_0: str, expected_99: str
):
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime | {ref} | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/datetime", ["insert", "getall", "search"])
    resp = app.post("/example/summary/datetime/Test", json={"value": given})
    response = app.get("/example/summary/datetime/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": expected_0,
            "count": 1,
            "_type": "example/summary/datetime/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": expected_99,
            "count": 0,
            "_type": "example/summary/datetime/Test",
        },
    )


def test_summary_datetime_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/datetime    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime | 200L | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/datetime", ["insert", "getall", "search"])
    response = app.get("/example/summary/datetime/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 0
    assert json_response["_data"] == []


def test_summary_datetime_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/datetime/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            {dataset_name}    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | datetime |   | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    app.post(model_name, json={"value": "2023-01-01T10:00:00"})
    app.post(model_name, json={"value": "2023-01-02T10:00:05"})
    resp_middle = app.post(model_name, json={"value": "2023-09-12T14:08:03"})
    resp_last = app.post(model_name, json={"value": "2023-12-29T22:12:00"})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0], {"bin": "2023-01-03 05:30:03.600000", "count": 2, "_type": model_name}
    )
    assert dict_equals(
        json_response["_data"][70],
        {
            "bin": "2023-09-13 23:38:27.600000",
            "count": 1,
            "_type": model_name,
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "2023-12-28 02:41:56.400000",
            "count": 1,
            "_type": model_name,
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_date(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/date    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | date |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/date", ["insert", "getall", "search"])
    app.post("/example/summary/date/Test", json={"value": "2023-01-01"})
    app.post("/example/summary/date/Test", json={"value": "2023-01-02"})
    resp_middle = app.post("/example/summary/date/Test", json={"value": "2023-08-01"})
    resp_last = app.post("/example/summary/date/Test", json={"value": "2024-01-08"})
    response = app.get("/example/summary/date/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0], {"bin": "2023-01-02 20:38:24", "count": 2, "_type": "example/summary/date/Test"}
    )
    assert dict_equals(
        json_response["_data"][56],
        {
            "bin": "2023-07-30 04:19:12",
            "count": 1,
            "_type": "example/summary/date/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "2024-01-06 03:21:36",
            "count": 1,
            "_type": "example/summary/date/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_date_single_given_no_units(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/date    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | date |   | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/date", ["insert", "getall", "search"])
    resp = app.post("/example/summary/date/Test", json={"value": "2023-01-01"})
    response = app.get("/example/summary/date/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": "2023-01-01 12:00:00",
            "count": 1,
            "_type": "example/summary/date/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "2023-04-10 12:00:00",
            "count": 0,
            "_type": "example/summary/date/Test",
        },
    )


@pytest.mark.parametrize("ref, given, expected_0, expected_99", date_units_data.values(), ids=date_units_data.keys())
def test_summary_date_single_given_custom_units(
    rc: RawConfig, postgresql: str, request: FixtureRequest, ref: str, given: str, expected_0: str, expected_99: str
):
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/date    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | date | {ref} | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/date", ["insert", "getall", "search"])
    resp = app.post("/example/summary/date/Test", json={"value": given})
    response = app.get("/example/summary/date/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": expected_0,
            "count": 1,
            "_type": "example/summary/date/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": expected_99,
            "count": 0,
            "_type": "example/summary/date/Test",
        },
    )


@pytest.mark.parametrize(
    "ref, given, expected_0, expected_99", date_ignore_units_data.values(), ids=date_ignore_units_data.keys()
)
def test_summary_date_single_given_custom_units_ignore(
    rc: RawConfig, postgresql: str, request: FixtureRequest, ref: str, given: str, expected_0: str, expected_99: str
):
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/date    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | date | {ref} | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/date", ["insert", "getall", "search"])
    resp = app.post("/example/summary/date/Test", json={"value": given})
    response = app.get("/example/summary/date/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": expected_0,
            "count": 1,
            "_type": "example/summary/date/Test",
            "_id": resp.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": expected_99,
            "count": 0,
            "_type": "example/summary/date/Test",
        },
    )


def test_summary_date_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/date    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | date | 200L | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/date", ["insert", "getall", "search"])
    response = app.get("/example/summary/date/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 0
    assert json_response["_data"] == []


def test_summary_date_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/date/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            {dataset_name}    |        |         |         |
              |   |   | Test       |        |      |         | 
              |   |   |   | value    | date |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    app.post(model_name, json={"value": "2023-01-01"})
    app.post(model_name, json={"value": "2023-01-02"})
    resp_middle = app.post(model_name, json={"value": "2023-08-01"})
    resp_last = app.post(model_name, json={"value": "2024-01-08"})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 100
    assert dict_equals(json_response["_data"][0], {"bin": "2023-01-02 20:38:24", "count": 2, "_type": model_name})
    assert dict_equals(
        json_response["_data"][56],
        {
            "bin": "2023-07-30 04:19:12",
            "count": 1,
            "_type": model_name,
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][99],
        {
            "bin": "2024-01-06 03:21:36",
            "count": 1,
            "_type": model_name,
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_level_4_no_uri(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri | level
            example/summary/ref        |        |           |        |     |
              |   |   | Test           |        |           |        |     |
              |   |   |   | value_test | ref    | Ref       | open   |     | 4
              |   |   | Ref            |        | value_ref |        |     |
              |   |   |   | value_ref  | string |           | open   |     | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref", ["insert", "getall", "search"])
    resp_0 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_0"})
    resp_1 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_1"})
    resp_2 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_2"})

    id_0 = resp_0.json()["_id"]
    id_1 = resp_1.json()["_id"]
    id_2 = resp_2.json()["_id"]

    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    resp_middle = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_1}})
    resp_last = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_2}})
    response = app.get("/example/summary/ref/Test/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0], {"bin": extract_uuid(id_0), "count": 2, "_type": "example/summary/ref/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": extract_uuid(id_1),
            "count": 1,
            "_type": "example/summary/ref/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "bin": extract_uuid(id_2),
            "count": 1,
            "_type": "example/summary/ref/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_level_4_with_uri(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri
            example/summary/ref        |        |           |        |
                                       | prefix | dct       |        | http://purl.org/dc/dcmitype/
                                       |        | dcat      |        | http://www.w3.org/ns/dcat#
              |   |   | Test           |        |           |        |
              |   |   |   | value_test | ref    | Ref       | open   |
              |   |   | Ref            |        | value_ref |        | dct:label
              |   |   |   | value_ref  | string |           | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref", ["insert", "getall", "search"])
    resp_0 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_0"})
    resp_1 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_1"})
    resp_2 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_2"})

    id_0 = resp_0.json()["_id"]
    id_1 = resp_1.json()["_id"]
    id_2 = resp_2.json()["_id"]

    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    resp_middle = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_1}})
    resp_last = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_2}})
    response = app.get("/example/summary/ref/Test/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": extract_uuid(id_0),
            "count": 2,
            "label": "http://purl.org/dc/dcmitype/label",
            "_type": "example/summary/ref/Test",
        },
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": extract_uuid(id_1),
            "count": 1,
            "label": "http://purl.org/dc/dcmitype/label",
            "_type": "example/summary/ref/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "bin": extract_uuid(id_2),
            "count": 1,
            "label": "http://purl.org/dc/dcmitype/label",
            "_type": "example/summary/ref/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_level_4_with_uri_wrong(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri
            example/summary/ref        |        |           |        |
                                       | prefix | dct       |        | http://purl.org/dc/dcmitype/
                                       |        | dcat      |        | http://www.w3.org/ns/dcat#
              |   |   | Test           |        |           |        |
              |   |   |   | value_test | ref    | Ref       | open   |
              |   |   | Ref            |        | value_ref |        | test:label
              |   |   |   | value_ref  | string |           | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref", ["insert", "getall", "search"])
    resp_0 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_0"})
    resp_1 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_1"})
    resp_2 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_2"})

    id_0 = resp_0.json()["_id"]
    id_1 = resp_1.json()["_id"]
    id_2 = resp_2.json()["_id"]

    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    resp_middle = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_1}})
    resp_last = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_2}})
    response = app.get("/example/summary/ref/Test/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0], {"bin": extract_uuid(id_0), "count": 2, "_type": "example/summary/ref/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": extract_uuid(id_1),
            "count": 1,
            "_type": "example/summary/ref/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "bin": extract_uuid(id_2),
            "count": 1,
            "_type": "example/summary/ref/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_level_4_with_uri_url(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri
            example/summary/ref        |        |           |        |
                                       | prefix | dct       |        | http://purl.org/dc/dcmitype/
                                       |        | dcat      |        | http://www.w3.org/ns/dcat#
              |   |   | Test           |        |           |        |
              |   |   |   | value_test | ref    | Ref       | open   |
              |   |   | Ref            |        | value_ref |        | https://www.google.com/
              |   |   |   | value_ref  | string |           | open   |
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref", ["insert", "getall", "search"])
    resp_0 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_0"})
    resp_1 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_1"})
    resp_2 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_2"})

    id_0 = resp_0.json()["_id"]
    id_1 = resp_1.json()["_id"]
    id_2 = resp_2.json()["_id"]

    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    resp_middle = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_1}})
    resp_last = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_2}})
    response = app.get("/example/summary/ref/Test/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {
            "bin": extract_uuid(id_0),
            "count": 2,
            "label": "https://www.google.com/",
            "_type": "example/summary/ref/Test",
        },
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": extract_uuid(id_1),
            "count": 1,
            "label": "https://www.google.com/",
            "_type": "example/summary/ref/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "bin": extract_uuid(id_2),
            "count": 1,
            "label": "https://www.google.com/",
            "_type": "example/summary/ref/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_level_4_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/ref/{'a' * 100}"
    model_name = dataset_name + "/Test"
    model_ref_name = dataset_name + "/Ref"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property   | type   | ref       | access | uri | level
            {dataset_name}        |        |           |        |     |
              |   |   | Test           |        |           |        |     |
              |   |   |   | value_test | ref    | Ref       | open   |     | 4
              |   |   | Ref            |        | value_ref |        |     |
              |   |   |   | value_ref  | string |           | open   |     | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    resp_0 = app.post(model_ref_name, json={"value_ref": "test_0"})
    resp_1 = app.post(model_ref_name, json={"value_ref": "test_1"})
    resp_2 = app.post(model_ref_name, json={"value_ref": "test_2"})

    id_0 = resp_0.json()["_id"]
    id_1 = resp_1.json()["_id"]
    id_2 = resp_2.json()["_id"]

    app.post(model_name, json={"value_test": {"_id": id_0}})
    app.post(model_name, json={"value_test": {"_id": id_0}})
    resp_middle = app.post(model_name, json={"value_test": {"_id": id_1}})
    resp_last = app.post(model_name, json={"value_test": {"_id": id_2}})
    response = app.get(f"/{model_name}/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {"bin": extract_uuid(id_0), "count": 2, "_type": model_name})
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": extract_uuid(id_1),
            "count": 1,
            "_type": model_name,
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "bin": extract_uuid(id_2),
            "count": 1,
            "_type": model_name,
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_level_3_no_uri(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri | level
            example/summary/ref3        |        |           |        |     |
              |   |   | Test           |        |           |        |     |
              |   |   |   | value_test | ref    | Ref       | open   |     | 3
              |   |   | Ref            |        | value_ref |        |     |
              |   |   |   | value_ref  | string |           | open   |     | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref3", ["insert", "getall", "search"])
    app.post("/example/summary/ref3/Ref", json={"value_ref": "test_0"})
    app.post("/example/summary/ref3/Ref", json={"value_ref": "test_1"})
    app.post("/example/summary/ref3/Ref", json={"value_ref": "test_2"})

    app.post("/example/summary/ref3/Test", json={"value_test": {"value_ref": "test_0"}})
    app.post("/example/summary/ref3/Test", json={"value_test": {"value_ref": "test_0"}})
    resp_middle = app.post("/example/summary/ref3/Test", json={"value_test": {"value_ref": "test_1"}})
    resp_last = app.post("/example/summary/ref3/Test", json={"value_test": {"value_ref": "test_2"}})
    response = app.get("/example/summary/ref3/Test/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {"bin": "test_0", "count": 2, "_type": "example/summary/ref3/Test"})
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": "test_1",
            "count": 1,
            "_type": "example/summary/ref3/Test",
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "bin": "test_2",
            "count": 1,
            "_type": "example/summary/ref3/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_level_3_multiple_ref_props(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri | level
            example/summary/ref3multi        |        |           |        |     |
              |   |   | Test           |        |           |        |     |
              |   |   |   | value_test | ref    | Ref       | open   |     | 3
              |   |   | Ref            |        | value_ref, value_t |        |     |
              |   |   |   | value_ref  | string |           | open   |     | 
              |   |   |   | value_t  | string |           | open   |     | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref3multi", ["insert", "getall", "search"])
    app.post("/example/summary/ref3multi/Ref", json={"value_ref": "test_0", "value_t": "asd"})
    app.post("/example/summary/ref3multi/Ref", json={"value_ref": "test_1"})
    app.post("/example/summary/ref3multi/Ref", json={"value_ref": "test_2"})

    app.post("/example/summary/ref3multi/Test", json={"value_test": {"value_ref": "test_0", "value_t": "asd"}})
    app.post("/example/summary/ref3multi/Test", json={"value_test": {"value_ref": "test_0", "value_t": "asd"}})
    app.post("/example/summary/ref3multi/Test", json={"value_test": {"value_ref": "test_1"}})
    app.post("/example/summary/ref3multi/Test", json={"value_test": {"value_ref": "test_2"}})
    response = app.get("/example/summary/ref3multi/Test/:summary/value_test")

    assert response.status_code == 500


def test_summary_ref_level_3_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/ref3/{'a' * 100}"
    model_name = dataset_name + "/Test"
    model_ref_name = dataset_name + "/Ref"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property   | type   | ref       | access | uri | level
            {dataset_name}        |        |           |        |     |
              |   |   | Test           |        |           |        |     |
              |   |   |   | value_test | ref    | Ref       | open   |     | 3
              |   |   | Ref            |        | value_ref |        |     |
              |   |   |   | value_ref  | string |           | open   |     | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    app.post(model_ref_name, json={"value_ref": "test_0"})
    app.post(model_ref_name, json={"value_ref": "test_1"})
    app.post(model_ref_name, json={"value_ref": "test_2"})

    app.post(model_name, json={"value_test": {"value_ref": "test_0"}})
    app.post(model_name, json={"value_test": {"value_ref": "test_0"}})
    resp_middle = app.post(model_name, json={"value_test": {"value_ref": "test_1"}})
    resp_last = app.post(model_name, json={"value_test": {"value_ref": "test_2"}})
    response = app.get(f"/{model_name}/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {"bin": "test_0", "count": 2, "_type": model_name})
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": "test_1",
            "count": 1,
            "_type": model_name,
            "_id": resp_middle.json()["_id"],
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "bin": "test_2",
            "count": 1,
            "_type": model_name,
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_missing(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri | level
            example/summary/ref        |        |           |        |     |
              |   |   | Test           |        |           |        |     |
              |   |   |   | value_test | ref    | Ref       | open   |     | 4
              |   |   | Ref            |        | value_ref |        |     |
              |   |   |   | value_ref  | string |           | open   |     | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref", ["insert", "getall", "search"])
    resp_0 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_0"})
    app.post("/example/summary/ref/Ref", json={"value_ref": "test_1"})
    resp_2 = app.post("/example/summary/ref/Ref", json={"value_ref": "test_2"})

    id_0 = resp_0.json()["_id"]
    id_2 = resp_2.json()["_id"]

    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_0}})
    resp_last = app.post("/example/summary/ref/Test", json={"value_test": {"_id": id_2}})
    response = app.get("/example/summary/ref/Test/:summary/value_test")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(
        json_response["_data"][0], {"bin": extract_uuid(id_0), "count": 2, "_type": "example/summary/ref/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "bin": extract_uuid(id_2),
            "count": 1,
            "_type": "example/summary/ref/Test",
            "_id": resp_last.json()["_id"],
        },
    )


def test_summary_ref_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property   | type   | ref       | access | uri | level
            example/summary/ref        |        |           |        |     |
              |   |   | Test           |        |           |        |     |
              |   |   |   | value_test | ref    | Ref       | open   |     | 4
              |   |   | Ref            |        | value_ref |        |     |
              |   |   |   | value_ref  | string |           | open   |     | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/ref", ["insert", "getall", "search"])
    app.post("/example/summary/ref/Ref", json={"value_ref": "test_0"})
    app.post("/example/summary/ref/Ref", json={"value_ref": "test_1"})
    app.post("/example/summary/ref/Ref", json={"value_ref": "test_2"})

    response = app.get("/example/summary/ref/Test/:summary/value_test")
    json_response = response.json()
    assert len(json_response["_data"]) == 0
    assert json_response["_data"] == []


def test_summary_geometry_no_srid(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    resp_10_20 = app.post("/example/summary/geometry/Test", json={"value": "POINT(10 20)"})
    resp_40_80 = app.post("/example/summary/geometry/Test", json={"value": "POINT(40 80)"})
    response = app.get("/example/summary/geometry/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0], {"cluster": 2, "centroid": "POINT(0 0)", "_type": "example/summary/geometry/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "cluster": 1,
            "centroid": "POINT(10 20)",
            "_id": resp_10_20.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "cluster": 1,
            "centroid": "POINT(40 80)",
            "_id": resp_40_80.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )


def test_summary_geometry_with_srid(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry_srid    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry(3346) |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry_srid", ["insert", "getall", "search"])
    app.post("/example/summary/geometry_srid/Test", json={"value": "POINT(6000000.15 180000.05)"})
    app.post("/example/summary/geometry_srid/Test", json={"value": "POINT(6000000.15 180000.05)"})
    resp_10_20 = app.post("/example/summary/geometry_srid/Test", json={"value": "POINT(6200000.05 200000.15)"})
    resp_40_80 = app.post("/example/summary/geometry_srid/Test", json={"value": "POINT(6200000.05 400000.15)"})
    response = app.get("/example/summary/geometry_srid/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {"cluster": 2, "centroid": "POINT(6000000.15 180000.05)", "_type": "example/summary/geometry_srid/Test"},
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "cluster": 1,
            "centroid": "POINT(6200000.05 200000.15)",
            "_id": resp_10_20.json()["_id"],
            "_type": "example/summary/geometry_srid/Test",
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "cluster": 1,
            "centroid": "POINT(6200000.05 400000.15)",
            "_id": resp_40_80.json()["_id"],
            "_type": "example/summary/geometry_srid/Test",
        },
    )


def test_summary_geometry_linestring(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry(linestring) |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "LINESTRING(0 0, 90 90)"})
    app.post("/example/summary/geometry/Test", json={"value": "LINESTRING(0 90, 90 0)"})
    resp = app.post("/example/summary/geometry/Test", json={"value": "LINESTRING(0 0, 0 90)"})
    response = app.get("/example/summary/geometry/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(
        json_response["_data"][0], {"cluster": 2, "centroid": "POINT(45 45)", "_type": "example/summary/geometry/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {"cluster": 1, "centroid": "POINT(0 45)", "_id": resp.json()["_id"], "_type": "example/summary/geometry/Test"},
    )


def test_summary_geometry_polygon(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry(polygon) |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "POLYGON((-90 -90, 90 -90, 90 90, -90 90, -90 -90))"})
    app.post("/example/summary/geometry/Test", json={"value": "POLYGON((-25 -25, -25 25, 25 25, 25 -25, -25 -25))"})
    resp = app.post("/example/summary/geometry/Test", json={"value": "POLYGON((-90 90, -90 -90, 90 -90, -90 90))"})
    response = app.get("/example/summary/geometry/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(
        json_response["_data"][0], {"cluster": 2, "centroid": "POINT(0 0)", "_type": "example/summary/geometry/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "cluster": 1,
            "centroid": "POINT(-30 -30)",
            "_id": resp.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )


def test_summary_geometry_bbox_encapsulate_all(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    resp_10_20 = app.post("/example/summary/geometry/Test", json={"value": "POINT(10 20)"})
    resp_40_80 = app.post("/example/summary/geometry/Test", json={"value": "POINT(40 80)"})
    response = app.get("/example/summary/geometry/Test/:summary/value?bbox(0, 0, 100, 100)")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0], {"cluster": 2, "centroid": "POINT(0 0)", "_type": "example/summary/geometry/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "cluster": 1,
            "centroid": "POINT(10 20)",
            "_id": resp_10_20.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "cluster": 1,
            "centroid": "POINT(40 80)",
            "_id": resp_40_80.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )


def test_summary_geometry_bbox_encapsulate_none(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(10 20)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(40 80)"})
    response = app.get("/example/summary/geometry/Test/:summary/value?bbox(100, 100, 200, 200)")
    json_response = response.json()

    assert len(json_response["_data"]) == 0
    assert json_response["_data"] == []


def test_summary_geometry_bbox_encapsulate_partial(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    resp_10_20 = app.post("/example/summary/geometry/Test", json={"value": "POINT(10 20)"})
    resp_40_80 = app.post("/example/summary/geometry/Test", json={"value": "POINT(40 80)"})
    response = app.get("/example/summary/geometry/Test/:summary/value?bbox(10, 10, 100, 100)")
    json_response = response.json()

    assert len(json_response["_data"]) == 2
    assert dict_equals(
        json_response["_data"][0],
        {
            "cluster": 1,
            "centroid": "POINT(10 20)",
            "_id": resp_10_20.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "cluster": 1,
            "centroid": "POINT(40 80)",
            "_id": resp_40_80.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )


def test_summary_geometry_bbox_negative(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    resp_10_20 = app.post("/example/summary/geometry/Test", json={"value": "POINT(10 20)"})
    resp_40_80 = app.post("/example/summary/geometry/Test", json={"value": "POINT(40 80)"})
    response = app.get("/example/summary/geometry/Test/:summary/value?bbox(-100, -100, 100, 100)")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0], {"cluster": 2, "centroid": "POINT(0 0)", "_type": "example/summary/geometry/Test"}
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "cluster": 1,
            "centroid": "POINT(10 20)",
            "_id": resp_10_20.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "cluster": 1,
            "centroid": "POINT(40 80)",
            "_id": resp_40_80.json()["_id"],
            "_type": "example/summary/geometry/Test",
        },
    )


def test_summary_geometry_bbox_with_srid(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | TestSrid       |        |         |         | 
              |   |   |   | value    | geometry(3346) |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)

    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 180000.05)"})
    app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 180000.05)"})
    resp_10_20 = app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6200000.05 200000.15)"})
    resp_40_80 = app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6200000.05 400000.15)"})
    response = app.get("/example/summary/geometry/TestSrid/:summary/value?bbox(6000000, 180000, 6200001, 400001)")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {"cluster": 2, "centroid": "POINT(6000000.15 180000.05)", "_type": "example/summary/geometry/TestSrid"},
    )
    assert dict_equals(
        json_response["_data"][1],
        {
            "cluster": 1,
            "centroid": "POINT(6200000.05 200000.15)",
            "_id": resp_10_20.json()["_id"],
            "_type": "example/summary/geometry/TestSrid",
        },
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "cluster": 1,
            "centroid": "POINT(6200000.05 400000.15)",
            "_id": resp_40_80.json()["_id"],
            "_type": "example/summary/geometry/TestSrid",
        },
    )


def test_summary_geometry_bbox_bad_request(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(0 0)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(10 20)"})
    app.post("/example/summary/geometry/Test", json={"value": "POINT(40 80)"})
    response = app.get("/example/summary/geometry/Test/:summary/value?bbox(10, 10, 100, 100, 5000)")

    assert response.status_code == 400


def test_summary_geometry_empty(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    response = app.get("/example/summary/geometry/Test/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 0
    assert json_response["_data"] == []


def test_summary_geometry_under_unique_limit(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | TestSrid       |        |         |         | 
              |   |   |   | value    | geometry(3346) |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)

    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    for i in range(5):
        app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 180000.05)"})
    for i in range(5):
        app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 190000.05)"})
    for i in range(20):
        app.post("/example/summary/geometry/TestSrid", json={"value": f"POINT(6000000.15 180010.{i * 2 + 1})"})

    response = app.get("/example/summary/geometry/TestSrid/:summary/value")
    json_response = response.json()
    assert len(json_response["_data"]) == 22
    assert dict_equals(
        json_response["_data"][0],
        {"cluster": 5, "centroid": "POINT(6000000.15 180000.05)", "_type": "example/summary/geometry/TestSrid"},
    )
    assert dict_equals(
        json_response["_data"][1],
        {"cluster": 5, "centroid": "POINT(6000000.15 190000.05)", "_type": "example/summary/geometry/TestSrid"},
    )
    assert sum([item["cluster"] for item in json_response["_data"]]) == 30


def test_summary_geometry_over_unique_limit(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | TestSrid       |        |         |         | 
              |   |   |   | value    | geometry(3346) |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)

    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    for i in range(5):
        app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 180000.05)"})
    for i in range(5):
        app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 190000.05)"})
    for i in range(40):
        app.post("/example/summary/geometry/TestSrid", json={"value": f"POINT(6000000.15 180010.{i * 2 + 1})"})

    response = app.get("/example/summary/geometry/TestSrid/:summary/value")
    json_response = response.json()
    assert len(json_response["_data"]) == 25
    assert dict_equals(
        json_response["_data"][0],
        {"cluster": 5, "centroid": "POINT(6000000.15 180000.05)", "_type": "example/summary/geometry/TestSrid"},
    )
    assert dict_equals(
        json_response["_data"][1],
        {"cluster": 5, "centroid": "POINT(6000000.15 190000.05)", "_type": "example/summary/geometry/TestSrid"},
    )
    assert sum([item["cluster"] for item in json_response["_data"]]) == 50


def test_summary_geometry_under_unique_limit_bbox(rc: RawConfig, postgresql: str, request: FixtureRequest):
    context = bootstrap_manifest(
        rc,
        """
            d | r | b | m | property | type   | ref     | access  | uri
            example/summary/geometry    |        |         |         |
              |   |   | TestSrid       |        |         |         | 
              |   |   |   | value    | geometry(3346) |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)

    app.authmodel("example/summary/geometry", ["insert", "getall", "search"])
    for i in range(5):
        app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 180000.05)"})
    for i in range(5):
        app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6000000.15 190000.05)"})
    resp = app.post("/example/summary/geometry/TestSrid", json={"value": "POINT(6010000.15 194000.05)"})
    for i in range(39):
        app.post("/example/summary/geometry/TestSrid", json={"value": f"POINT(6000000.15 200010.{i * 2 + 1})"})

    response = app.get("/example/summary/geometry/TestSrid/:summary/value?bbox(6000000, 180000, 6200001, 195000)")
    json_response = response.json()
    assert len(json_response["_data"]) == 3
    assert dict_equals(
        json_response["_data"][0],
        {"cluster": 5, "centroid": "POINT(6000000.15 180000.05)", "_type": "example/summary/geometry/TestSrid"},
    )
    assert dict_equals(
        json_response["_data"][1],
        {"cluster": 5, "centroid": "POINT(6000000.15 190000.05)", "_type": "example/summary/geometry/TestSrid"},
    )
    assert dict_equals(
        json_response["_data"][2],
        {
            "_id": resp.json()["_id"],
            "cluster": 1,
            "centroid": "POINT(6010000.15 194000.05)",
            "_type": "example/summary/geometry/TestSrid",
        },
    )
    assert sum([item["cluster"] for item in json_response["_data"]]) == 11


def test_summary_geometry_long_name(rc: RawConfig, postgresql: str, request: FixtureRequest):
    dataset_name = f"example/summary/geometry/{'a' * 100}"
    model_name = dataset_name + "/Test"
    context = bootstrap_manifest(
        rc,
        f"""
            d | r | b | m | property | type   | ref     | access  | uri
            {dataset_name}    |        |         |         |
              |   |   | Test       |        |         |         | 
              |   |   |   | value    | geometry |         | open    | 
            """,
        backend=postgresql,
        request=request,
    )
    app = create_test_client(context)
    app.authmodel(dataset_name, ["insert", "getall", "search"])
    app.post(model_name, json={"value": "POINT(0 0)"})
    app.post(model_name, json={"value": "POINT(0 0)"})
    resp_10_20 = app.post(model_name, json={"value": "POINT(10 20)"})
    resp_40_80 = app.post(model_name, json={"value": "POINT(40 80)"})
    response = app.get(f"/{model_name}/:summary/value")
    json_response = response.json()

    assert len(json_response["_data"]) == 3
    assert dict_equals(json_response["_data"][0], {"cluster": 2, "centroid": "POINT(0 0)", "_type": model_name})
    assert dict_equals(
        json_response["_data"][1],
        {"cluster": 1, "centroid": "POINT(10 20)", "_id": resp_10_20.json()["_id"], "_type": model_name},
    )
    assert dict_equals(
        json_response["_data"][2],
        {"cluster": 1, "centroid": "POINT(40 80)", "_id": resp_40_80.json()["_id"], "_type": model_name},
    )
