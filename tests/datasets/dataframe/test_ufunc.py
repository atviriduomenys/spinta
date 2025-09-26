from pathlib import Path

import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from spinta.auth import AdminToken
from spinta.commands import getall, get_model
from spinta.components import Store
from spinta.core.enums import Mode
from spinta.core.config import RawConfig
from spinta.exceptions import InvalidBase64String
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest


@pytest.fixture
def fs():
    fs = MemoryFileSystem()
    yield fs
    MemoryFileSystem.store.clear()


def test_csv_tabular_seperator_default_error(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("šalis;miestas\nlt;Vilnius\nlv;Ryga\nee;Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref  | source              | prepare | access
    example                  |          |      |                     |         |
      | csv                  | dask/csv |      | memory://cities.csv |         |
      |   |   | City         |          | name |                     |         |
      |   |   |   | name     | string   |      | miestas             |         | open
      |   |   |   | country  | string   |      | šalis               |         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/City")
    assert resp.status_code == 404


def test_csv_tabular_seperator(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("šalis;miestas\nlt;Vilnius\nlv;Ryga\nee;Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref  | source              | prepare          | access
    example                  |          |      |                     |                  |
      | csv                  | dask/csv |      | memory://cities.csv | tabular(sep:";") |
      |   |   | City         |          | name |                     |                  |
      |   |   |   | name     | string   |      | miestas             |                  | open
      |   |   |   | country  | string   |      | šalis               |                  | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xsd_with_base64_returns_binary_base64_encoded_in_response(rc: RawConfig, tmp_path: Path):
    """base64 decoding happens for internal use, but value is once again encoded for response"""
    xml = """<CITY>
    <NAME>Kaunas</NAME>
    <COUNTRY>Lietuva</COUNTRY>
    <FILE>VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIDEzIGxhenkgZG9ncy4=</FILE>
    </CITY>"""

    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | source                | prepare         | access
        example                  |          |                       |                 |
          | res1                 | dask/xml | {tmp_path}/cities.xml |                 |
          |   |   | City         |          | /CITY                 |                 |
          |   |   |   | name     | string   | NAME                  |                 | open
          |   |   |   | country  | string   | COUNTRY               |                 | open
          |   |   |   | file     | binary   | FILE                  | base64()        | open
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall"])

    resp = app.get("/example/City")
    assert listdata(resp, sort=False) == [
        (
            "Lietuva",
            "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIDEzIGxhenkgZG9ncy4=",
            "Kaunas",
        )
    ]


def test_xds_getall_decodes_base64_for_internal_use(rc: RawConfig, tmp_path: Path):
    """base64 decoding happens for internal use"""
    xml = """<CITY>
    <NAME>Kaunas</NAME>
    <COUNTRY>Lietuva</COUNTRY>
    <FILE>VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIDEzIGxhenkgZG9ncy4=</FILE>
    </CITY>"""

    path = tmp_path / "cities.xml"
    path.write_text(xml)
    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | source                | prepare          | access
        example                  |          |                       |                  |
          | res1                 | dask/xml | {tmp_path}/cities.xml |                  |
          |   |   | City         |          | /CITY                 |                  |
          |   |   |   | name     | string   | NAME                  |                  | open
          |   |   |   | country  | string   | COUNTRY               |                  | open
          |   |   |   | file     | binary   | FILE                  | base64()         | open
        """,
        mode=Mode.external,
    )

    with context:
        store: Store = context.get("store")
        for keymap in store.keymaps.values():
            context.attach(f"keymap.{keymap.name}", lambda: keymap)

        context.set("auth.token", AdminToken())

        model = get_model(context, manifest, "example/City")
        result = list(getall(context, model, model.backend))

        del result[0]["_id"]
        assert result == [
            {
                "_type": "example/City",
                "country": "Lietuva",
                "file": b"The quick brown fox jumps over 13 lazy dogs.",
                "name": "Kaunas",
            }
        ]


@pytest.mark.parametrize("invalid_base64_string", ["lalald", "ąčę"])
def test_xsd_getall_raise_error_if_base64_is_invalid(rc: RawConfig, tmp_path: Path, invalid_base64_string: str):
    xml = f"""<CITY><NAME>Kaunas</NAME><COUNTRY>Lietuva</COUNTRY><FILE>{invalid_base64_string}</FILE></CITY>"""

    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
        d | r | b | m | property | type     | source                | prepare         | access
        example                  |          |                       |                 |
          | res1                 | dask/xml | {tmp_path}/cities.xml |                 |
          |   |   | City         |          | /CITY                 |                 |
          |   |   |   | name     | string   | NAME                  |                 | open
          |   |   |   | country  | string   | COUNTRY               |                 | open
          |   |   |   | file     | binary   | FILE                  | base64()        | open
        """,
        mode=Mode.external,
    )

    with context:
        store: Store = context.get("store")
        for keymap in store.keymaps.values():
            context.attach(f"keymap.{keymap.name}", lambda: keymap)

        context.set("auth.token", AdminToken())

        model = get_model(context, manifest, "example/City")

        with pytest.raises(InvalidBase64String) as e:
            list(getall(context, model, model.backend))

        assert e.value.message == (
            f'Value of property "file" cannot be decoded because "{invalid_base64_string}" is not valid base64 string.'
        )


def test_csv_and_operation(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("salis,miestas\nlt,Vilnius\nlt,Kaunas\nlt,Siauliai\nlv,Ryga\nee,Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref  | source              | prepare          | access
    example                  |          |      |                     |                  |
      | csv                  | dask/csv |      | memory://cities.csv |                  |
      |   |   | City         |          | name |                     |                  |
      |   |   |   | name     | string   |      | miestas             |                  | open
      |   |   |   | country  | string   |      | salis               |                  | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall", "search"])

    resp = app.get('/example/City?select()&country="lt"&name="Kaunas"')
    assert listdata(resp, sort=False) == [
        ("lt", "Kaunas"),
    ]


def test_csv_or_operation(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("salis,miestas\nlt,Vilnius\nlt,Kaunas\nlt,Siauliai\nlv,Ryga\nee,Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref  | source              | prepare          | access
    example                  |          |      |                     |                  |
      | csv                  | dask/csv |      | memory://cities.csv |                  |
      |   |   | City         |          | name |                     |                  |
      |   |   |   | name     | string   |      | miestas             |                  | open
      |   |   |   | country  | string   |      | salis               |                  | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall", "search"])

    resp = app.get('/example/City?select()&name="Vilnius"|country="ee"|name="Ryga"')
    assert listdata(resp, sort=False) == [("lt", "Vilnius"), ("lv", "Ryga"), ("ee", "Talin")]


def test_csv_and_or_operation(rc: RawConfig, fs: AbstractFileSystem):
    fs.pipe("cities.csv", ("salis,miestas\nlt,Vilnius\nlt,Kaunas\nlt,Siauliai\nlv,Ryga\nee,Talin").encode("utf-8"))

    context, manifest = prepare_manifest(
        rc,
        """
    d | r | b | m | property | type     | ref  | source              | prepare          | access
    example                  |          |      |                     |                  |
      | csv                  | dask/csv |      | memory://cities.csv |                  |
      |   |   | City         |          | name |                     |                  |
      |   |   |   | name     | string   |      | miestas             |                  | open
      |   |   |   | country  | string   |      | salis               |                  | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/City", ["getall", "search"])

    resp = app.get('/example/City?select()&country="ee"|(country="lt"&name="Kaunas")|name="Ryga"')
    assert listdata(resp, sort=False) == [("lt", "Kaunas"), ("lv", "Ryga"), ("ee", "Talin")]
