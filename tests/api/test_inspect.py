import json
import pathlib

import sqlalchemy as sa

from spinta.components import Context
from spinta.testing.client import TestClient
from spinta.testing.datasets import Sqlite
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.utils import get_error_codes


def test_inspect_unauthorized(
    context: Context,
    app: TestClient,
):
    form_data = {"resource.source": "http://www.example.com", "dataset": "test"}
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 403


def test_inspect_empty(
    context: Context,
    app: TestClient,
):
    app.authorize(["spinta_inspect"])
    form_data = {}
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["MissingFormKeys"]


def test_inspect_resource_missing_dataset(
    context: Context,
    app: TestClient,
):
    app.authorize(["spinta_inspect"])
    form_data = {"resource.source": "http://www.example.com"}
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredFormKeyWithCondition"]


def test_inspect_source_not_url(
    context: Context,
    app: TestClient,
):
    app.authorize(["spinta_inspect"])
    form_data = {"resource.source": "C:/test.db"}
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidInputData"]

    form_data = {"manifest.source": "C:/test.db"}
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidInputData"]


def test_inspect_type_not_given(
    context: Context,
    app: TestClient,
):
    app.authorize(["spinta_inspect"])
    form_data = {"resource.source": "http://www.example.com", "dataset": "test"}
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredFormKeyWithCondition"]

    form_data = {
        "manifest.source": "http://www.example.com",
    }
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["RequiredFormKeyWithCondition"]


def test_inspect_file_and_source_given(
    context: Context,
    app: TestClient,
):
    app.authorize(["spinta_inspect"])
    form_data = {"resource.file": b"", "resource.source": "http://www.example.com", "dataset": "test"}
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidFormKeyCombination"]

    form_data = {
        "manifest.file": b"",
        "manifest.source": "http://www.example.com",
    }
    resp = app.post("/:inspect", data=form_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidFormKeyCombination"]


def test_inspect_manifest_resource_with_non_url_path(
    context: Context,
    app: TestClient,
    tmp_path: pathlib.Path,
    sqlite: Sqlite,
):
    sqlite.init(
        {
            "COUNTRY": [
                sa.Column("NAME", sa.Text),
            ],
        }
    )

    table = f"""
       d | r | m | property | type    | ref | source  | prepare | access  | title
       datasets/gov/example |         |     |         |         |         | Example
         | schema           | sql     |     | {sqlite.dsn} |         |         |
                            |         |     |         |         |         |
         |   | Country      |         |     | COUNTRY |         |         | Country
         |   |   | name     | string  |     | NAME    |         | open    | Country name
         |   |   | code     | string  |     | CODE    |         | open    | Country code
       """

    create_tabular_manifest(context, tmp_path / "manifest.csv", table)
    app.authorize(["spinta_inspect"])
    with open(tmp_path / "manifest.csv", "rb") as f:
        form_data = {"manifest.type": "tabular"}
        files = {"manifest.file": ("manifest.csv", f, "text/csv")}
        resp = app.post("/:inspect", data=form_data, files=files)
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ["InvalidResourceSource"]


def test_inspect_resource_file(
    context: Context,
    app: TestClient,
    tmp_path: pathlib.Path,
):
    json_manifest = [
        {
            "name": "Lithuania",
            "code": "LT",
            "location": {"latitude": 54.5, "longitude": 12.6},
            "cities": [
                {"name": "Vilnius", "weather": {"temperature": 24.7, "wind_speed": 12.4}},
                {"name": "Kaunas", "weather": {"temperature": 29.7, "wind_speed": 11.4}},
            ],
        },
        {"name": "Latvia", "code": "LV", "cities": [{"name": "Riga"}]},
    ]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))

    app.authorize(["spinta_inspect"])
    with open(tmp_path / "manifest.json", "rb") as f:
        form_data = {"resource.type": "json", "dataset": "datasets/gov/aaa/atlieku_tvarkymas"}
        files = {"resource.file": ("manifest.json", f, "application/json")}
        resp = app.post("/:inspect", data=form_data, files=files)
    assert resp.status_code == 200
    assert "text/csv" in resp.headers["Content-Type"]
    assert (
        "id,dataset,resource,base,model,property,type,ref,source,source.type,prepare,origin,count,level,status,visibility,access,uri,eli,title,description\r\n"
        ",datasets/gov/aaa/atlieku_tvarkymas,,,,,,,,,,,,,,,,,,,\r\n"
        ",,resource,,,,dask/json,,https://get.data.gov.lt/datasets/gov/aaa/atlieku_tvarkymas,,,,,,,,,,,,\r\n"
        ",,,,,,,,,,,,,,,,,,,,\r\n"
        ",,,,Model1,,,,.,,,,,,develop,private,,,,,\r\n"
        ",,,,,name,string required unique,,name,,,,,,develop,private,,,,,\r\n"
        ",,,,,code,string required unique,,code,,,,,,develop,private,,,,,\r\n"
        ",,,,,location_latitude,number unique,,location.latitude,,,,,,develop,private,,,,,\r\n"
        ",,,,,location_longitude,number unique,,location.longitude,,,,,,develop,private,,,,,\r\n"
        ",,,,,,,,,,,,,,,,,,,,\r\n"
        ",,,,Cities,,,,cities,,,,,,develop,private,,,,,\r\n"
        ",,,,,name,string required unique,,name,,,,,,develop,private,,,,,\r\n"
        ",,,,,weather_temperature,number unique,,weather.temperature,,,,,,develop,private,,,,,\r\n"
        ",,,,,weather_wind_speed,number unique,,weather.wind_speed,,,,,,develop,private,,,,,\r\n"
        ",,,,,parent,ref,Model1,..,,,,,,develop,private,,,,,\r\n"
    ) in resp.text


def test_inspect_resource_file_format_xlsx(
    context: Context,
    app: TestClient,
    tmp_path: pathlib.Path,
):
    json_manifest = [
        {
            "name": "Lithuania",
            "code": "LT",
            "location": {"latitude": 54.5, "longitude": 12.6},
            "cities": [
                {"name": "Vilnius", "weather": {"temperature": 24.7, "wind_speed": 12.4}},
                {"name": "Kaunas", "weather": {"temperature": 29.7, "wind_speed": 11.4}},
            ],
        },
        {"name": "Latvia", "code": "LV", "cities": [{"name": "Riga"}]},
    ]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(json_manifest))

    app.authorize(["spinta_inspect"])
    with open(tmp_path / "manifest.json", "rb") as f:
        form_data = {"resource.type": "json", "dataset": "new/dataset"}
        files = {"resource.file": ("manifest.json", f, "application/json")}
        resp = app.post("/:inspect?format(xlsx)", data=form_data, files=files)
    assert resp.status_code == 200
    assert "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in resp.headers["Content-Type"]
