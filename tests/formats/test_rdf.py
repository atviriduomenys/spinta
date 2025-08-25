import base64
import uuid
from pathlib import Path

from _pytest.fixtures import FixtureRequest

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.data import pushdata, encode_page_values_manually
from spinta.testing.manifest import bootstrap_manifest
from starlette.datastructures import Headers
import pytest


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_get_all_without_uri(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   | Country      |        | name    |         | 
      |   |   |   | name     | string |         | open    | 
      |   |   | City         |        | name    |         | 
      |   |   |   | name     | string |         | open    |
      |   |   |   | country  | ref    | Country | open    | 
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall"])

    country = pushdata(app, "/example/rdf/Country", {"name": "Lithuania"})
    city1 = pushdata(
        app,
        "/example/rdf/City",
        {
            "name": "Vilnius",
            "country": {"_id": country["_id"]},
        },
    )
    city2 = pushdata(
        app,
        "/example/rdf/City",
        {
            "name": "Kaunas",
            "country": {"_id": country["_id"]},
        },
    )

    res = app.get("/example/rdf/City/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/rdf/City/{city2["_id"]}" rdf:type="example/rdf/City" '
        f'pav:version="{city2["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': city2['name'], '_id': city2['_id']})}</_page>\n"
        f"  <name>{city2['name']}</name>\n"
        f'  <country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n'
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/rdf/City/{city1["_id"]}" rdf:type="example/rdf/City" '
        f'pav:version="{city1["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': city1['name'], '_id': city1['_id']})}</_page>\n"
        f"  <name>{city1['name']}</name>\n"
        f'  <country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n'
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_get_all_with_uri(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | dct:country
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall"])

    country = pushdata(app, "/example/rdf/Country", {"name": "Lithuania"})
    city1 = pushdata(
        app,
        "/example/rdf/City",
        {
            "name": "Vilnius",
            "country": {"_id": country["_id"]},
        },
    )
    city2 = pushdata(
        app,
        "/example/rdf/City",
        {
            "name": "Kaunas",
            "country": {"_id": country["_id"]},
        },
    )

    res = app.get("/example/rdf/City/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<dcat:city rdf:about="/example/rdf/City/{city2["_id"]}" rdf:type="example/rdf/City" '
        f'pav:version="{city2["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': city2['name'], '_id': city2['_id']})}</_page>\n"
        f"  <dct:name>{city2['name']}</dct:name>\n"
        f'  <dct:country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n'
        f"</dcat:city>\n"
        f'<dcat:city rdf:about="/example/rdf/City/{city1["_id"]}" rdf:type="example/rdf/City" '
        f'pav:version="{city1["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': city1['name'], '_id': city1['_id']})}</_page>\n"
        f"  <dct:name>{city1['name']}</dct:name>\n"
        f'  <dct:country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n'
        f"</dcat:city>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_get_one(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | dct:country
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getone"])

    country = pushdata(app, "/example/rdf/Country", {"name": "Lithuania"})
    city = pushdata(
        app,
        "/example/rdf/City",
        {
            "name": "Vilnius",
            "country": {"_id": country["_id"]},
        },
    )

    res = app.get(f"/example/rdf/City/{city['_id']}/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<dcat:city rdf:about="/example/rdf/City/{city["_id"]}" rdf:type="example/rdf/City" '
        f'pav:version="{city["_revision"]}">\n'
        f"  <dct:name>{city['name']}</dct:name>\n"
        f'  <dct:country rdf:resource="/example/rdf/Country/{country["_id"]}"/>\n'
        f"</dcat:city>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_with_file(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf/file         |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | flag     | file   |         | open    | dct:flag
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf/file", ["insert", "getone"])

    country = pushdata(
        app,
        "/example/rdf/file/Country",
        {
            "name": "Lithuania",
            "flag": {
                "_id": "file.txt",
                "_content_type": "text/plain",
                "_content": base64.b64encode(b"DATA").decode(),
            },
        },
    )

    res = app.get(f"/example/rdf/file/Country/{country['_id']}/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<dcat:country rdf:about="/example/rdf/file/Country/{country["_id"]}" '
        f'rdf:type="example/rdf/file/Country" '
        f'pav:version="{country["_revision"]}">\n'
        f"  <dct:name>{country['name']}</dct:name>\n"
        f'  <dct:flag rdf:about="/example/rdf/file/Country/{country["_id"]}/flag"/>\n'
        f"</dcat:country>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_get_with_uri_model_rename(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf/rename              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | uri    |         | open    | dcat:country
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall"])

    country = pushdata(
        app, "/example/rdf/rename/Country", {"name": "Lithuania", "country": "https://example.com/country/Lithuania"}
    )

    res = app.get("/example/rdf/rename/Country/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<dcat:country rdf:about="https://example.com/country/Lithuania" '
        f'rdf:type="example/rdf/rename/Country" '
        f'pav:version="{country["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': country['name'], '_id': country['_id']})}</_page>\n"
        f"  <dct:name>{country['name']}</dct:name>\n"
        f"</dcat:country>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_get_with_uri_ref_rename(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf/rename              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | uri    |         | open    | dcat:country
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | dct:country
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall"])

    country = pushdata(
        app, "/example/rdf/rename/Country", {"name": "Lithuania", "country": "https://example.com/country/Lithuania"}
    )
    city = pushdata(
        app,
        "/example/rdf/rename/City",
        {
            "name": "Vilnius",
            "country": {"_id": country["_id"]},
        },
    )

    res = app.get("/example/rdf/rename/City/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<dcat:city rdf:about="/example/rdf/rename/City/{city["_id"]}" rdf:type="example/rdf/rename/City" '
        f'pav:version="{city["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': city['name'], '_id': city['_id']})}</_page>\n"
        f"  <dct:name>{city['name']}</dct:name>\n"
        f'  <dct:country rdf:resource="https://example.com/country/Lithuania"/>\n'
        f"</dcat:city>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_empty_ref(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf/ref              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | 
      |   |   |   | name     | string |         | open    | dct:name
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | 
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall"])

    city = pushdata(
        app,
        "/example/rdf/ref/City",
        {
            "name": "Vilnius",
        },
    )

    res = app.get("/example/rdf/ref/City/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<dcat:city rdf:about="/example/rdf/ref/City/{city["_id"]}" rdf:type="example/rdf/ref/City" '
        f'pav:version="{city["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': city['name'], '_id': city['_id']})}</_page>\n"
        f"  <dct:name>{city['name']}</dct:name>\n"
        f"</dcat:city>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_mixed_ref(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf/ref/multi              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | 
      |   |   |   | name     | string |         | open    | dct:name
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | 
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall"])

    country = pushdata(app, "/example/rdf/ref/multi/Country", {"name": "Lithuania"})
    vilnius = pushdata(app, "/example/rdf/ref/multi/City", {"name": "Vilnius", "country": {"_id": country.get("_id")}})
    ryga = pushdata(
        app,
        "/example/rdf/ref/multi/City",
        {
            "name": "Ryga",
        },
    )

    res = app.get("/example/rdf/ref/multi/City/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<dcat:city rdf:about="/example/rdf/ref/multi/City/{ryga["_id"]}" rdf:type="example/rdf/ref/multi/City" '
        f'pav:version="{ryga["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': ryga['name'], '_id': ryga['_id']})}</_page>\n"
        f"  <dct:name>{ryga['name']}</dct:name>\n"
        f"</dcat:city>\n"
        f'<dcat:city rdf:about="/example/rdf/ref/multi/City/{vilnius["_id"]}" rdf:type="example/rdf/ref/multi/City" '
        f'pav:version="{vilnius["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'name': vilnius['name'], '_id': vilnius['_id']})}</_page>\n"
        f"  <dct:name>{vilnius['name']}</dct:name>\n"
        f'  <country rdf:resource="/example/rdf/ref/multi/Country/{country["_id"]}"/>\n'
        f"</dcat:city>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_namespace_all(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type   | ref     | access  | uri
    example/rdf/ref/simple   |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   |   |          |        | test    |         | http://test.com
      |   |   | Country      |        | name    |         | 
      |   |   |   | name     | string |         | open    |
      |   |   | City         |        | name    |         |
      |   |   |   | name     | string |         | open    |
      |   |   |   | country  | ref    | Country | open    | 
    example/rdf/ref/multi              |        |         |         |
      |   |   |   |          | prefix | rdf     |         | http://www.rdf.com
      |   |   |   |          |        | pav     |         | http://purl.org/pav/
      |   |   |   |          |        | dcat    |         | http://www.dcat.com
      |   |   |   |          |        | dct     |         | http://dct.com
      |   |   | Country      |        | name    |         | dcat:country
      |   |   |   | name     | string |         | open    | dct:name
      |   |   | City         |        | name    |         | dcat:city
      |   |   |   | name     | string |         | open    | dct:name
      |   |   |   | country  | ref    | Country | open    | 
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall"])

    country_mix = pushdata(app, "/example/rdf/ref/multi/Country", {"name": "Lithuania"})
    vilnius_mix = pushdata(
        app, "/example/rdf/ref/multi/City", {"name": "Vilnius", "country": {"_id": country_mix.get("_id")}}
    )
    ryga_mix = pushdata(
        app,
        "/example/rdf/ref/multi/City",
        {
            "name": "Ryga",
        },
    )

    country_simple = pushdata(app, "/example/rdf/ref/simple/Country", {"name": "Lithuania"})
    vilnius_simple = pushdata(
        app, "/example/rdf/ref/simple/City", {"name": "Vilnius", "country": {"_id": country_simple.get("_id")}}
    )
    ryga_simple = pushdata(
        app,
        "/example/rdf/ref/simple/City",
        {
            "name": "Ryga",
        },
    )

    res = app.get("/example/rdf/ref/:all/:format/rdf").text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns:test="http://test.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/rdf/ref/simple/Country/{country_simple["_id"]}" rdf:type="example/rdf/ref/simple/Country" '
        f'pav:version="{country_simple["_revision"]}">\n'
        f"  <name>{country_simple['name']}</name>\n"
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/rdf/ref/simple/City/{vilnius_simple["_id"]}" rdf:type="example/rdf/ref/simple/City" '
        f'pav:version="{vilnius_simple["_revision"]}">\n'
        f"  <name>{vilnius_simple['name']}</name>\n"
        f'  <country rdf:resource="/example/rdf/ref/simple/Country/{country_simple["_id"]}"/>\n'
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/rdf/ref/simple/City/{ryga_simple["_id"]}" rdf:type="example/rdf/ref/simple/City" '
        f'pav:version="{ryga_simple["_revision"]}">\n'
        f"  <name>{ryga_simple['name']}</name>\n"
        f"</rdf:Description>\n"
        f'<dcat:country rdf:about="/example/rdf/ref/multi/Country/{country_mix["_id"]}" rdf:type="example/rdf/ref/multi/Country" '
        f'pav:version="{country_mix["_revision"]}">\n'
        f"  <dct:name>{country_mix['name']}</dct:name>\n"
        f"</dcat:country>\n"
        f'<dcat:city rdf:about="/example/rdf/ref/multi/City/{vilnius_mix["_id"]}" rdf:type="example/rdf/ref/multi/City" '
        f'pav:version="{vilnius_mix["_revision"]}">\n'
        f"  <dct:name>{vilnius_mix['name']}</dct:name>\n"
        f'  <country rdf:resource="/example/rdf/ref/multi/Country/{country_mix["_id"]}"/>\n'
        f"</dcat:city>\n"
        f'<dcat:city rdf:about="/example/rdf/ref/multi/City/{ryga_mix["_id"]}" rdf:type="example/rdf/ref/multi/City" '
        f'pav:version="{ryga_mix["_revision"]}">\n'
        f"  <dct:name>{ryga_mix['name']}</dct:name>\n"
        f"</dcat:city>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/rdf/text         |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall", "search"])

    lt = pushdata(app, "/example/rdf/text/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}})
    uk = pushdata(app, "/example/rdf/text/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}})

    res = app.get(
        "/example/rdf/text/Country/:format/rdf?sort(id)", headers=Headers(headers={"accept-language": "lt"})
    ).text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/rdf/text/Country/{lt["_id"]}" rdf:type="example/rdf/text/Country" '
        f'pav:version="{lt["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': lt['id'], '_id': lt['_id']})}</_page>\n"
        f"  <id>{lt['id']}</id>\n"
        f'  <name xml:lang="lt">Lietuva</name>\n'
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/rdf/text/Country/{uk["_id"]}" rdf:type="example/rdf/text/Country" '
        f'pav:version="{uk["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': uk['id'], '_id': uk['_id']})}</_page>\n"
        f"  <id>{uk['id']}</id>\n"
        f'  <name xml:lang="lt">Anglija</name>\n'
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_text_with_lang(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/rdf/text/lang    |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall", "search"])

    lt = pushdata(
        app, "/example/rdf/text/lang/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}}
    )
    uk = pushdata(
        app, "/example/rdf/text/lang/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}}
    )

    res = app.get(
        "/example/rdf/text/lang/Country/:format/rdf?lang(*)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/rdf/text/lang/Country/{lt["_id"]}" rdf:type="example/rdf/text/lang/Country" '
        f'pav:version="{lt["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': lt['id'], '_id': lt['_id']})}</_page>\n"
        f"  <id>{lt['id']}</id>\n"
        f"  <name>LT</name>\n"
        f'  <name xml:lang="en">Lithuania</name>\n'
        f'  <name xml:lang="lt">Lietuva</name>\n'
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/rdf/text/lang/Country/{uk["_id"]}" rdf:type="example/rdf/text/lang/Country" '
        f'pav:version="{uk["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': uk['id'], '_id': uk['_id']})}</_page>\n"
        f"  <id>{uk['id']}</id>\n"
        f"  <name>UK</name>\n"
        f'  <name xml:lang="en">England</name>\n'
        f'  <name xml:lang="lt">Anglija</name>\n'
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )

    res = app.get(
        "/example/rdf/text/lang/Country/:format/rdf?lang(en)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/rdf/text/lang/Country/{lt["_id"]}" rdf:type="example/rdf/text/lang/Country" '
        f'pav:version="{lt["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': lt['id'], '_id': lt['_id']})}</_page>\n"
        f"  <id>{lt['id']}</id>\n"
        f'  <name xml:lang="en">Lithuania</name>\n'
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/rdf/text/lang/Country/{uk["_id"]}" rdf:type="example/rdf/text/lang/Country" '
        f'pav:version="{uk["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': uk['id'], '_id': uk['_id']})}</_page>\n"
        f"  <id>{uk['id']}</id>\n"
        f'  <name xml:lang="en">England</name>\n'
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )

    res = app.get(
        "/example/rdf/text/lang/Country/:format/rdf?lang(en,lt)&sort(id)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert (
        res == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:about="/example/rdf/text/lang/Country/{lt["_id"]}" rdf:type="example/rdf/text/lang/Country" '
        f'pav:version="{lt["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': lt['id'], '_id': lt['_id']})}</_page>\n"
        f"  <id>{lt['id']}</id>\n"
        f'  <name xml:lang="en">Lithuania</name>\n'
        f'  <name xml:lang="lt">Lietuva</name>\n'
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:about="/example/rdf/text/lang/Country/{uk["_id"]}" rdf:type="example/rdf/text/lang/Country" '
        f'pav:version="{uk["_revision"]}">\n'
        f"  <_page>{encode_page_values_manually({'id': uk['id'], '_id': uk['_id']})}</_page>\n"
        f"  <id>{uk['id']}</id>\n"
        f'  <name xml:lang="en">England</name>\n'
        f'  <name xml:lang="lt">Anglija</name>\n'
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_changes_text(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/rdf/text/changes |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | text    |         | open    | 3     |
      |   |   |   | name@en  | string  |         | open    |       |
      |   |   |   | name@lt  | string  |         | open    |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall", "search", "changes"])

    lt = pushdata(
        app, "/example/rdf/text/changes/Country", {"id": 0, "name": {"lt": "Lietuva", "en": "Lithuania", "C": "LT"}}
    )
    uk = pushdata(
        app, "/example/rdf/text/changes/Country", {"id": 1, "name": {"lt": "Anglija", "en": "England", "C": "UK"}}
    )

    resp = app.get(
        "/example/rdf/text/changes/Country/:changes/-10/:format/rdf?select(id,name)",
        headers=Headers(headers={"accept-language": "lt"}),
    ).text
    assert (
        resp == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f'<rdf:Description rdf:type="example/rdf/text/changes/Country">\n'
        f"  <id>{lt['id']}</id>\n"
        f"  <name>LT</name>\n"
        f'  <name xml:lang="en">Lithuania</name>\n'
        f'  <name xml:lang="lt">Lietuva</name>\n'
        f"</rdf:Description>\n"
        f'<rdf:Description rdf:type="example/rdf/text/changes/Country">\n'
        f"  <id>{uk['id']}</id>\n"
        f"  <name>UK</name>\n"
        f'  <name xml:lang="en">England</name>\n'
        f'  <name xml:lang="lt">Anglija</name>\n'
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )


@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_empty(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property | type    | ref     | access  | level | uri
    example/rdf/empty        |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | Country      |         | name    |         |       | 
      |   |   |   | id       | integer |         |         |       |
      |   |   |   | name     | string  |         | open    | 3     |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context)
    app.authmodel("example/rdf", ["insert", "getall", "search", "changes"])

    resp = app.get("/example/rdf/empty/Country/:format/rdf?select(id,name)").text
    assert (
        resp == '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<rdf:RDF\n"
        ' xmlns:rdf="http://www.rdf.com"\n'
        ' xmlns:pav="http://purl.org/pav/"\n'
        ' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        ' xmlns:dcat="http://www.dcat.com"\n'
        ' xmlns:dct="http://dct.com"\n'
        ' xmlns="https://testserver/">\n'
        "</rdf:RDF>\n"
    )


@pytest.mark.skip(reason="Requires #925 to be implemented (Denorm, Object types)")
@pytest.mark.manifests("internal_sql", "csv")
def test_rdf_changes_corrupt_data(
    manifest_type: str,
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc,
        """
    d | r | b | m | property     | type    | ref     | access  | level | uri
    example/rdf/changes/corrupt |         |         |         |       | 
      |   |   |   |          | prefix  | rdf     |         |       | http://www.rdf.com
      |   |   |   |          |         | pav     |         |       | http://purl.org/pav/
      |   |   |   |          |         | dcat    |         |       | http://www.dcat.com
      |   |   |   |          |         | dct     |         |       | http://dct.com
      |   |   | City             |         | name    | open    |       | 
      |   |   |   | id           | integer |         |         |       |
      |   |   |   | name         | string  |         |         |       |
      |   |   |   | country      | ref     | Country |         |       |
      |   |   |   | country.test | string  |         |         |       |
      |   |   |   | obj          | object  |         |         |       |
      |   |   |   | obj.test     | string  |         |         |       |
      |   |   | Country          |         | name    | open    |       | 
      |   |   |   | id           | integer |         |         |       |
      |   |   |   | name         | string  |         |         |       |
    """,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type=manifest_type,
        request=request,
        full_load=True,
    )
    app = create_test_client(context, scope=["spinta_set_meta_fields"])
    app.authmodel("example/rdf", ["insert", "getall", "search", "changes"])
    country_id = str(uuid.uuid4())
    city_id = str(uuid.uuid4())
    pushdata(app, "/example/rdf/changes/corrupt/Country", {"_id": country_id, "id": 0, "name": "Lietuva"})
    pushdata(
        app,
        "/example/rdf/changes/corrupt/City",
        {
            "_id": city_id,
            "id": 0,
            "name": "Vilnius",
            "country": {"_id": country_id, "test": "t_lt"},
            "obj": {"test": "t_obj"},
        },
    )

    resp = app.get(
        "/example/rdf/changes/corrupt/City/:changes/-10/:format/rdf?select(_id, id, name, country, obj)"
    ).text
    assert (
        resp == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f"<rdf:Description "
        f'rdf:about="/example/rdf/changes/corrupt/City/{city_id}" '
        'rdf:type="example/rdf/changes/corrupt/City"'
        ">\n"
        f"  <id>0</id>\n"
        f"  <name>Vilnius</name>\n"
        f"  <country>\n"
        "    <rdf:Description "
        f'rdf:about="/example/rdf/changes/corrupt/Country/{country_id}" '
        'rdf:type="example/rdf/changes/corrupt/Country">\n'
        "      <test>t_lt</test>\n"
        "    </rdf:Description>\n"
        "  </country>\n"
        "  <obj>\n"
        "    <test>t_obj</test>\n"
        "  </obj>\n"
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )

    # Corrupt changelog data
    store = context.get("store")
    backend: PostgreSQL = store.manifest.backend
    model = commands.get_model(context, store.manifest, "example/rdf/changes/corrupt/City")
    with backend.begin() as transaction:
        table = backend.get_table(model, TableType.CHANGELOG)
        transaction.execute(
            table.update()
            .values(
                data={
                    "id": 0,
                    "name": "Vilnius",
                    "new": "new",
                    "country": {"_id": country_id, "testas": "testas"},
                    "obj": {"test": "t_obj_updated", "nested": {"test": "test"}},
                }
            )
            .where(table.c._rid == city_id)
        )

    resp = app.get("/example/rdf/changes/corrupt/City/:changes/-10/:format/rdf").text
    assert (
        resp == f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<rdf:RDF\n"
        f' xmlns:rdf="http://www.rdf.com"\n'
        f' xmlns:pav="http://purl.org/pav/"\n'
        f' xmlns:xml="http://www.w3.org/XML/1998/namespace"\n'
        f' xmlns:dcat="http://www.dcat.com"\n'
        f' xmlns:dct="http://dct.com"\n'
        f' xmlns="https://testserver/">\n'
        f"<rdf:Description "
        f'rdf:about="/example/rdf/changes/corrupt/City/{city_id}" '
        'rdf:type="example/rdf/changes/corrupt/City"'
        ">\n"
        f"  <id>0</id>\n"
        f"  <name>Vilnius</name>\n"
        f"  <country>\n"
        "    <rdf:Description "
        f'rdf:about="/example/rdf/changes/corrupt/Country/{country_id}" '
        'rdf:type="example/rdf/changes/corrupt/Country">\n'
        "    </rdf:Description>\n"
        "  </country>\n"
        "  <obj>\n"
        "    <test>t_obj_updated</test>\n"
        "  </obj>\n"
        f"</rdf:Description>\n"
        f"</rdf:RDF>\n"
    )
