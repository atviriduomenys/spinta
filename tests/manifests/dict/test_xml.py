from spinta.core.config import RawConfig

from pathlib import Path

from spinta.testing.manifest import load_manifest, compare_manifest


def test_xml_normal(rc: RawConfig, tmp_path: Path):
    xml = '''
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
        </countries>'''
    path = tmp_path / 'manifest.xml'
    path.write_text(xml)

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(manifest, f'''
d | r | model   | property     | type           | ref     | source
dataset                  |                |         |
  | resource             | xml            |         | manifest.xml
                         |                |         |
  |   | Country          |                |         | /countries/country
  |   |   | code         | string unique  |         | @code
  |   |   | name         | string unique  |         | @name
  |   |   | location_lon | integer unique |         | location/lon
  |   |   | location_lat | integer unique |         | location/lat

''')
    assert a == b


def test_xml_blank_node(rc: RawConfig, tmp_path: Path):
    xml = '''
    <galaxy name="Milky">
        <solar_system name="Solar">
            <planet name="Earth">
                <countries>
                    <country>
                        <code>lt</code>
                        <name>Lietuva></name>
                        <location lat="0" lon="1"/>
                    </country>
                    <country>
                        <code>lv</code>
                        <name>Latvija></name>
                        <location lat="2" lon="3"/>
                    </country>
                    <country>
                        <code>ee</code>
                        <name>Estija></name>
                        <location lat="4" lon="5"/>
                    </country>
                </countries>
            </planet>
        </solar_system>
    </galaxy>'''
    path = tmp_path / 'manifest.xml'
    path.write_text(xml)

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(manifest, f'''
d | r | model   | property                        | type                    | ref    | source
dataset                                     |                         |        |
  | resource                                | xml                     |        | manifest.xml
                                            |                         |        |
  |   | Model1                              |                         |        | .
  |   |   | galaxy_name                     | string required unique  |        | galaxy/@name
  |   |   | galaxy_solar_system_name        | string required unique  |        | galaxy/solar_system/@name
  |   |   | galaxy_solar_system_planet_name | string required unique  |        | galaxy/solar_system/planet/@name
                                            |                         |        |
  |   | Country                             |                         |        | /galaxy/solar_system/planet/countries/country
  |   |   | code                            | string required unique  |        | code
  |   |   | name                            | string required unique  |        | name
  |   |   | location_lat                    | integer required unique |        | location/@lat
  |   |   | location_lon                    | integer required unique |        | location/@lon
  |   |   | parent                          | ref                     | Model1 | ../../../../..

''')
    assert a == b


def test_xml_allowed_namespace(rc: RawConfig, tmp_path: Path):
    xml = '''
        <countries xmlns:xsi="http://www.example.com/xmlns/xsi" xmlns="http://www.example.com/xmlns" xmlns:new="http://www.example.com/xmlns/new">
            <new:country xsi:code="lt" name="Lietuva"/>
            <new:country name="Latvija">
                <location xmlns:test="http://www.example.com/xmlns/test">
                    <test:lon>3</test:lon>
                </location>
            </new:country>
            <new:country xsi:code="ee">
                <location>
                    <test:lon>5</test:lon>
                    <test:lat>4</test:lat>
                </location>
            </new:country>
        </countries>'''
    path = tmp_path / 'manifest.xml'
    path.write_text(xml)

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(manifest, f'''
d | r | model   | property     | type           | ref   | source                 | uri
dataset                  |                |       |                        |
                         | prefix         | xsi   |                        | http://www.example.com/xmlns/xsi
                         |                | xmlns |                        | http://www.example.com/xmlns
                         |                | new   |                        | http://www.example.com/xmlns/new
                         |                | test  |                        | http://www.example.com/xmlns/test
                         |                |       |                        |
  | resource             | xml            |       | manifest.xml           |
                         |                |       |                        |
  |   | Country          |                |       | /countries/new:country |
  |   |   | code         | string unique  |       | @xsi:code              |
  |   |   | name         | string unique  |       | @name                  |
  |   |   | location_lon | integer unique |       | location/test:lon      |
  |   |   | location_lat | integer unique |       | location/test:lat      |

''')
    assert a == b


def test_xml_disallowed_namespace(rc: RawConfig, tmp_path: Path):
    xml = '''
        <countries test:xsi="http://www.example.com/xmlns/xsi" xmlns="http://www.example.com/xmlns" xmlns:new="http://www.example.com/xmlns/new">
            <new:country xsi:code="lt" name="Lietuva"/>
            <new:country name="Latvija">
                <location xmlns:test="http://www.example.com/xmlns/test">
                    <test:lon>3</test:lon>
                </location>
            </new:country>
            <new:country xsi:code="ee">
                <location>
                    <test:lon>5</test:lon>
                    <test:lat>4</test:lat>
                </location>
            </new:country>
        </countries>'''
    path = tmp_path / 'manifest.xml'
    path.write_text(xml)

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(manifest, f'''
d | r | model   | property      | type                | ref   | source                 | uri
dataset                   |                     |       |                        |
                          | prefix              | xmlns |                        | http://www.example.com/xmlns
                          |                     | new   |                        | http://www.example.com/xmlns/new
                          |                     | test  |                        | http://www.example.com/xmlns/test
                          |                     |       |                        |
  | resource              | xml                 |       | manifest.xml           |
                          |                     |       |                        |
  |   | Model1            |                     |       | .                      |
  |   |   | countries_xsi | url required unique |       | countries/@test:xsi    |
                          |                     |       |                        |
  |   | Country           |                     |       | /countries/new:country |
  |   |   | xsi_code      | string unique       |       | @xsi:code              |
  |   |   | name          | string unique       |       | @name                  |
  |   |   | location_lon  | integer unique      |       | location/test:lon      |
  |   |   | location_lat  | integer unique      |       | location/test:lat      |
  |   |   | parent        | ref                 | Model1 | ../..                  |

''')
    assert a == b


def test_xml_inherit_nested(rc: RawConfig, tmp_path: Path):
    xml = '''
    <countries>
        <country name="Lithuania" code="LT">
            <location test="nope">
                <coords>54.5</coords>
                <coords>58.6</coords>
                <geos>
                    <geo geo_test="test"/>
                    <geo geo_test="test"/>
                </geos>
            </location>
            <cities>
                <city name="Vilnius">
                    <location>
                        <coords>54.5</coords>
                        <coords>55.1</coords>
                        <geos>
                            <geo geo_test="5"/>
                            <geo geo_test="4"/>
                        </geos>
                    </location>
                </city>
                <city name="Kaunas"></city>
            </cities>
        </country>
        <country name="Latvia" code="LV">
            <cities>
                <city name="Riga"></city>
                <city name="Empty"></city>
            </cities>
        </country>
    </countries>'''
    path = tmp_path / 'manifest.xml'
    path.write_text(xml)

    manifest = load_manifest(rc, path)
    manifest.datasets["dataset"].resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(manifest, f'''
d | r | model   | property        | type                    | ref     | source
dataset                     |                         |         |
  | resource                | xml                     |         | manifest.xml
                            |                         |         |
  |   | Country             |                         |         | /countries/country
  |   |   | name            | string required unique  |         | @name
  |   |   | code            | string required unique  |         | @code
  |   |   | location_test   | string unique           |         | location/@test
  |   |   | location_coords | array                   |         | location/coords
                            |                         |         |
  |   | Geo                 |                         |         | /countries/country/location/geos/geo
  |   |   | geo_test        | string required         |         | @geo_test
  |   |   | country         | ref                     | Country | ../../..
                            |                         |         |
  |   | Geo1                |                         |         | /countries/country/cities/city/location/geos/geo
  |   |   | geo_test        | integer required unique |         | @geo_test
  |   |   | city            | ref                     | City    | ../../..
                            |                         |         |
  |   | City                |                         |         | /countries/country/cities/city
  |   |   | name            | string required unique  |         | @name
  |   |   | location_coords | array                   |         | location/coords
  |   |   | country         | ref                     | Country | ../..
''')
    assert a == b
