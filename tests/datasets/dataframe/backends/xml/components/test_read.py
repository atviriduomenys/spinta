from spinta.core.config import RawConfig, Path
from spinta.core.enums import Mode
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import prepare_manifest


def test_xml_read(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city>
                <code>lt</code>
                <name>Vilnius</name>
            </city>
            <city>
                <code>lv</code>
                <name>Ryga</name>
            </city>
            <city>
                <code>ee</code>
                <name>Talin</name>
            </city>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source       | access
    example/xml              |          |      |              |
      | xml                  | dask/xml |      | {path}       |
      |   |   | City         |          | name | /cities/city |
      |   |   |   | name     | string   |      | name         | open
      |   |   |   | country  | string   |      | code         | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_with_attributes(rc: RawConfig, tmp_path: Path):
    xml = """
        <cities>
            <city code="lt" name="Vilnius"/>
            <city code="lv" name="Ryga"/>
            <city code="ee" name="Talin"/>
        </cities>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source       | access
    example/xml              |          |      |              |
      | xml                  | dask/xml |      | {path}       |
      |   |   | City         |          | name | /cities/city |
      |   |   |   | name     | string   |      | @name        | open
      |   |   |   | country  | string   |      | @code        | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/City", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_refs_level_3(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source                         | access | level
       example/xml              |          |         |                                |        |
         | xml                  | dask/xml |         | {path}                         |        |
         |   |   | City         |          | name    | /countries/country/cities/city |        |
         |   |   |   | name     | string   |         | @name                          | open   |
         |   |   |   | code     | ref      | Country | ../../code                     | open   | 3
                     |  
         |   |   | Country      |          | country | /countries/country             |        |
         |   |   |   | name     | string   |         | name                           | open   |
         |   |   |   | country  | string   |         | code                           | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]


def test_xml_read_refs_level_4(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country>
                <code>lt</code>
                <name>Lietuva</name>
                <cities>
                    <city name="Vilnius"/>
                </cities>
            </country>
            <country>
                <code>lv</code>
                <name>Latvija</name>
                <cities>
                    <city name="Ryga"/>
                </cities>
            </country>
            <country>
                <code>ee</code>
                <name>Estija</name>
                <cities>
                    <city name="Talin"/>
                </cities>
            </country>
        </countries>
    """
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source                         | access | level
       example/xml              |          |         |                                |        |
         | xml                  | dask/xml |         | {path}                         |        |
         |   |   | City         |          | name    | /countries/country/cities/city |        |
         |   |   |   | name     | string   |         | @name                          | open   |
         |   |   |   | code     | ref      | Country | ../..                          | open   | 4
                     |  
         |   |   | Country      |          | country | /countries/country             |        |
         |   |   |   | name     | string   |         | name                           | open   |
         |   |   |   | country  | string   |         | code                           | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/Country")
    countries = {c["country"]: c["_id"] for c in listdata(resp, "_id", "country", full=True)}
    assert sorted(countries) == ["ee", "lt", "lv"]

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        (countries["lt"], "Vilnius"),
        (countries["lv"], "Ryga"),
        (countries["ee"], "Talin"),
    ]


def test_xml_read_multiple_sources(rc: RawConfig, tmp_path: Path):
    xml0 = """
        <cities>
            <city name="Vilnius" code="lt"/>
            <city name="Ryga" code="lv"/>
            <city name="Talin" code="ee"/>
        </cities>
    """
    path0 = tmp_path / "cities.xml"
    path0.write_text(xml0)

    xml1 = """
        <countries>
            <country code="lt" name="Lietuva"/>
            <country code="lv" name="Latvija"/>
            <country code="ee" name="Estija"/>
        </countries>
    """
    path1 = tmp_path / "countries.xml"
    path1.write_text(xml1)

    context, manifest = prepare_manifest(
        rc,
        f"""
       d | r | b | m | property | type     | ref     | source             | access | level
       example/xml              |          |         |                    |        |
         | xml_city             | dask/xml |         | {path0}            |        |
         |   |   | City         |          | name    | /cities/city       |        |
         |   |   |   | name     | string   |         | @name              | open   |
         |   |   |   | code     | string   |         | @code              | open   | 3
                     |  
         | xml_country          | dask/xml |         | {path1}            |        |
         |   |   | Country      |          | code    | /countries/country |        |
         |   |   |   | name     | string   |         | @name              | open   |
         |   |   |   | code     | string   |         | @code              | open   |
        """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml", ["getall"])

    resp = app.get("/example/xml/City")
    assert listdata(resp, sort=False) == [
        ("lt", "Vilnius"),
        ("lv", "Ryga"),
        ("ee", "Talin"),
    ]

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
        ("ee", "Estija"),
    ]


def test_xml_read_with_empty(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country code="lt"/>
            <country code="lv" name="Latvija"/>
            <country name="Estija"/>
        </countries>"""
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property | type     | ref  | source               
           example/xml                |          |      |                    
             | resource               | dask/xml |      | {path}             
                                      |          |      |                    
             |   | Country |          |          | code | /countries/country 
             |   |         | name     | string   |      | @name              
             |   |         | code     | string   |      | @code              
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Country", ["getall"])

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None),
        ("lv", "Latvija"),
        (None, "Estija"),
    ]


def test_xml_read_with_empty_nested(rc: RawConfig, tmp_path: Path):
    xml = """
        <countries>
            <country code="lt" name="Lietuva"/>
            <country name="Latvija">
                <location>
                    <lon>3</lon>
                </location>
            </country>
            <country code="ee">
                <location>
                    <lon>5</lon>
                    <lat>4</lat>
                </location>
            </country>
        </countries>"""
    path = tmp_path / "cities.xml"
    path.write_text(xml)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | m              | property  | type     | ref  | source                  
           example/xml                 |          |      |                   
             | resource                | dask/xml |      | {path}            
                                       |          |      |                   
             |   | Country |           |          | code | /countries/country
             |   |         | name      | string   |      | @name             
             |   |         | code      | string   |      | @code             
             |   |         | latitude  | integer  |      | location/lat      
             |   |         | longitude | integer  |      | location/lon      
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Country", ["getall"])

    resp = app.get("/example/xml/Country")
    assert listdata(resp, sort=False) == [
        ("lt", None, None, "Lietuva"),
        (None, None, 3, "Latvija"),
        ("ee", 4, 5, None),
    ]


def test_xml_read_parametrize_simple_iterate_pages(rc: RawConfig, tmp_path: Path):
    page_count = 10
    for i in range(1, page_count):
        current_page_file = tmp_path / f"page{i - 1}.xml"
        xml_manifest = f"""
        <pages name="Page {i}">
            <next>{str(tmp_path / f"page{i}.xml") if i != page_count - 1 else ""}</next>
        </pages>
        """
        current_page_file.write_text(xml_manifest)

    context, manifest = prepare_manifest(
        rc,
        f"""
    d | r | b | m | property | type     | ref  | source                   | prepare     | access
    example/xml              |          |      |                          |             |
      | resource             | dask/xml |      | {{path}}                 |             |
      |   |   |              | param    | path | {tmp_path / "page0.xml"} |             |
      |   |   |              |          |      | Page                     | read().next |
      |   |   | Page         |          | name | ../pages                 |             |
      |   |   |   | name     | string   |      | @name                    |             | open
      |   |   |   | next     | uri      |      | next/text()              |             | open
    """,
        mode=Mode.external,
    )
    context.loaded = True
    app = create_test_client(context)
    app.authmodel("example/xml/Page", ["getall"])

    resp = app.get("/example/xml/Page")
    assert listdata(resp, "name", sort=False) == [
        "Page 1",
        "Page 2",
        "Page 3",
        "Page 4",
        "Page 5",
        "Page 6",
        "Page 7",
        "Page 8",
        "Page 9",
    ]
