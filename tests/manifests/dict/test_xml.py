from unittest.mock import ANY

from responses import RequestsMock

from spinta import commands
from spinta.core.config import RawConfig

from pathlib import Path

from spinta.manifests.dict.components import MappingMeta, DictFormat, MappedDataset, MappedModels, MappedProperties
from spinta.manifests.dict.helpers import XMLIterSchemaReader
from spinta.testing.manifest import compare_manifest, load_manifest_and_context


class TestXMLIterSchemaReader:
    @staticmethod
    def get_reader(path: str) -> XMLIterSchemaReader:
        mapping_meta = MappingMeta.get_for(DictFormat.XML)
        dataset_structure = MappedDataset(
            dataset="dataset",
            given_dataset_name="dataset",
            resource="resource",
            resource_type=f"dask/{DictFormat.XML.value}",
            resource_path=path,
            models={},
        )

        return XMLIterSchemaReader(path, mapping_meta, dataset_structure)

    def test_reads_xml_from_file(self, tmp_path: Path):
        xml = """
            <countries xmlns="www.test.test/xmlns" xmlns:xsi="www.test.test/xsi">
                <xsi:country xmlns:test="www.test.test/test" name="Lietuva"/>
            </countries>
        """
        path = tmp_path / "data.xml"
        path.write_text(xml)

        reader = self.get_reader(str(path))
        reader.read_xml()

        assert reader.namespaces
        assert reader.dataset_structure.models

    def test_reads_xml_from_url(self, responses: RequestsMock):
        url = "https://test.test/xml"
        xml = """
            <countries xmlns="www.test.test/xmlns" xmlns:xsi="www.test.test/xsi">
                <xsi:country xmlns:test="www.test.test/test" name="Lietuva"/>
            </countries>
        """
        responses.add(responses.GET, url, body=xml, status=200, content_type="application/xml")
        reader = self.get_reader(url)
        reader.read_xml()

        assert reader.namespaces
        assert reader.dataset_structure.models

    def test_reads_namespaces(self, tmp_path: Path):
        xml = """
            <countries xmlns="www.test.test/xmlns" xmlns:xsi="www.test.test/xsi">
                <xsi:country xmlns:test="www.test.test/test" name="Lietuva"/>
            </countries>
        """
        path = tmp_path / "data.xml"
        path.write_text(xml)

        reader = self.get_reader(str(path))
        reader.read_xml()

        assert reader.namespaces == [
            ("xmlns", "www.test.test/xmlns"),
            ("xsi", "www.test.test/xsi"),
            ("test", "www.test.test/test"),
        ]

    def test_reads_model_with_attributes_and_elements(self, tmp_path: Path):
        xml = """
            <countries>
                <country name="Lietuva">
                    <code>LT</code>
                    <neighbour_country_names>Latvia</neighbour_country_names>
                    <neighbour_country_names>Lenkija</neighbour_country_names>
                </country>
                <country name="Latvija">
                    <code>LV</code>
                    <neighbour_country_names>Lietuva</neighbour_country_names>
                    <neighbour_country_names>Estija</neighbour_country_names>
                </country>
            </countries>
        """
        path = tmp_path / "data.xml"
        path.write_text(xml)

        reader = self.get_reader(str(path))
        reader.read_xml()

        assert reader._XMLIterSchemaReader__structural_data == {
            "countries": {
                "country": [
                    {
                        "@name": "Latvija",
                        "code": "LV",
                        "neighbour_country_names": [
                            "Estija",
                        ],
                    }
                ]
            }
        }

        assert reader.dataset_structure == MappedDataset(
            dataset="dataset",
            given_dataset_name="dataset",
            resource="resource",
            resource_type="dask/xml",
            resource_path=str(path),
            models={
                "country": {
                    "countries/country": MappedModels(
                        name="country",
                        source="countries/country",
                        properties={
                            "@name": MappedProperties(name="@name", source="@name", extra="", type_detector=ANY),
                            "code": MappedProperties(name="code", source="code", extra="", type_detector=ANY),
                            "neighbour_country_names": MappedProperties(
                                name="neighbour_country_names",
                                source="neighbour_country_names",
                                extra="",
                                type_detector=ANY,
                            ),
                        },
                    )
                }
            },
        )

    def test_reads_model_with_ref_to_another_model(self, tmp_path: Path):
        xml = """
            <countries>
                <country name="Lietuva">
                    <city>
                        <name>Vilnius</name>
                    </city>
                    <city>
                        <name>Kaunas</name>
                    </city>
                </country>
                <country name="Latvija">
                    <city>
                        <name>Riga</name>
                    </city>
                    <city>
                        <name>Daugavpils</name>
                    </city>
                </country>
            </countries>
        """
        path = tmp_path / "data.xml"
        path.write_text(xml)

        reader = self.get_reader(str(path))
        reader.read_xml()

        assert reader._XMLIterSchemaReader__structural_data == {
            "countries": {
                "country": [
                    {
                        "@name": "Latvija",
                        "city": [
                            {
                                "name": "Daugavpils",
                            }
                        ],
                    }
                ]
            }
        }

        assert reader.dataset_structure == MappedDataset(
            dataset="dataset",
            given_dataset_name="dataset",
            resource="resource",
            resource_type="dask/xml",
            resource_path=str(path),
            models={
                "country": {
                    "countries/country": MappedModels(
                        name="country",
                        source="countries/country",
                        properties={
                            "@name": MappedProperties(name="@name", source="@name", extra="", type_detector=ANY)
                        },
                    )
                },
                "city": {
                    "countries/country[]/city": MappedModels(
                        name="city",
                        source="countries/country[]/city",
                        properties={
                            "name": MappedProperties(name="name", source="name", extra="", type_detector=ANY),
                            "country": MappedProperties(
                                name="country", source="..", extra="countries/country", type_detector=ANY
                            ),
                        },
                    )
                },
            },
        )

    def test_reads_xml_with_namespaces(self, tmp_path: Path):
        xml = """
            <countries xmlns="www.test.test/xmlns" xmlns:ctry="www.test.test/Country" xmlns:attr="www.test.test/Attribute">
                <ctry:country xmlns:cty="www.test.test/City" name="Lietuva">
                    <attr:code>LT</code>
                    <attr:neighbour_country_names>Latvia</attr:neighbour_country_names>
                    <attr:neighbour_country_names>Lenkija</attr:neighbour_country_names>
                    <cty:city>
                        <attr:name>Vilnius</attr:name>
                    </cty:city>
                    <cty:city>
                        <attr:name>Kaunas</attr:name>
                    </cty:city>
                </country>
                <ctry:country name="Latvija">
                    <attr:code>LV</attr:code>
                    <attr:neighbour_country_names>Lietuva</attr:neighbour_country_names>
                    <attr:neighbour_country_names>Estija</attr:neighbour_country_names>
                    <cty:city>
                        <attr:name>Riga</attr:name>
                    </cty:city>
                    <cty:city>
                        <attr:name>Daugavpils</attr:name>
                    </cty:city>
                </ctry:country>
            </countries>
        """
        path = tmp_path / "data.xml"
        path.write_text(xml)

        reader = self.get_reader(str(path))
        reader.read_xml()

        assert reader._XMLIterSchemaReader__structural_data == {
            "countries": {
                "ctry:country": [
                    {
                        "@name": "Latvija",
                        "attr:code": "LV",
                        "attr:neighbour_country_names": ["Estija"],
                        "cty:city": [
                            {
                                "attr:name": "Daugavpils",
                            }
                        ],
                    }
                ]
            }
        }

        assert reader.dataset_structure == MappedDataset(
            dataset="dataset",
            given_dataset_name="dataset",
            resource="resource",
            resource_type="dask/xml",
            resource_path=str(path),
            models={
                "ctry:country": {
                    "countries/ctry:country": MappedModels(
                        name="ctry:country",
                        source="countries/ctry:country",
                        properties={
                            "@name": MappedProperties(name="@name", source="@name", extra="", type_detector=ANY),
                            "attr:code": MappedProperties(
                                name="attr:code", source="attr:code", extra="", type_detector=ANY
                            ),
                            "attr:neighbour_country_names": MappedProperties(
                                name="attr:neighbour_country_names",
                                source="attr:neighbour_country_names",
                                extra="",
                                type_detector=ANY,
                            ),
                        },
                    )
                },
                "cty:city": {
                    "countries/ctry:country[]/cty:city": MappedModels(
                        name="cty:city",
                        source="countries/ctry:country[]/cty:city",
                        properties={
                            "attr:name": MappedProperties(
                                name="attr:name", source="attr:name", extra="", type_detector=ANY
                            ),
                            "ctry:country": MappedProperties(
                                name="ctry:country", source="..", extra="countries/ctry:country", type_detector=ANY
                            ),
                        },
                    )
                },
            },
        )

    def test_reads_xml_with_incorrect_namespaces(self, tmp_path: Path):
        xml = """
            <countries>
                <ctry:country name="Lietuva">
                    <attr:code>LT</code>
                    <attr:neighbour_country_names>Latvia</attr:neighbour_country_names>
                    <attr:neighbour_country_names>Lenkija</attr:neighbour_country_names>
                    <cty:city>
                        <attr:name>Vilnius</attr:name>
                    </cty:city>
                    <cty:city>
                        <attr:name>Kaunas</attr:name>
                    </cty:city>
                </country>
                <ctry:country name="Latvija">
                    <attr:code>LV</attr:code>
                    <attr:neighbour_country_names>Lietuva</attr:neighbour_country_names>
                    <attr:neighbour_country_names>Estija</attr:neighbour_country_names>
                    <cty:city>
                        <attr:name>Riga</attr:name>
                    </cty:city>
                    <cty:city>
                        <attr:name>Daugavpils</attr:name>
                    </cty:city>
                </ctry:country>
            </countries>
        """
        path = tmp_path / "data.xml"
        path.write_text(xml)

        reader = self.get_reader(str(path))
        reader.read_xml()

        assert reader._XMLIterSchemaReader__structural_data == {
            "countries": {
                "ctry:country": [
                    {
                        "@name": "Latvija",
                        "attr:code": "LV",
                        "attr:neighbour_country_names": ["Estija"],
                        "cty:city": [
                            {
                                "attr:name": "Daugavpils",
                            }
                        ],
                    }
                ]
            }
        }

        assert reader.dataset_structure == MappedDataset(
            dataset="dataset",
            given_dataset_name="dataset",
            resource="resource",
            resource_type="dask/xml",
            resource_path=str(path),
            models={
                "ctry:country": {
                    "countries/ctry:country": MappedModels(
                        name="ctry:country",
                        source="countries/ctry:country",
                        properties={
                            "@name": MappedProperties(name="@name", source="@name", extra="", type_detector=ANY),
                            "attr:code": MappedProperties(
                                name="attr:code", source="attr:code", extra="", type_detector=ANY
                            ),
                            "attr:neighbour_country_names": MappedProperties(
                                name="attr:neighbour_country_names",
                                source="attr:neighbour_country_names",
                                extra="",
                                type_detector=ANY,
                            ),
                        },
                    )
                },
                "cty:city": {
                    "countries/ctry:country[]/cty:city": MappedModels(
                        name="cty:city",
                        source="countries/ctry:country[]/cty:city",
                        properties={
                            "attr:name": MappedProperties(
                                name="attr:name", source="attr:name", extra="", type_detector=ANY
                            ),
                            "ctry:country": MappedProperties(
                                name="ctry:country", source="..", extra="countries/ctry:country", type_detector=ANY
                            ),
                        },
                    )
                },
            },
        )


def test_xml_normal(rc: RawConfig, tmp_path: Path):
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
    path = tmp_path / "manifest.xml"
    path.write_text(xml)

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(
        manifest,
        """
d | r | model | property     | type           | ref     | source
dataset                  |                |         |
  | resource             | dask/xml       |         | manifest.xml
                         |                |         |
  |   | Country          |                |         | /countries/country
  |   |   | code         | string unique  |         | @code
  |   |   | name         | string unique  |         | @name
  |   |   | location_lon | integer unique |         | location/lon
  |   |   | location_lat | integer unique |         | location/lat

""",
        context,
    )
    assert a == b


def test_xml_single_entry_initial_model(rc: RawConfig, tmp_path: Path):
    xml = """
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
    </galaxy>
    """
    path = tmp_path / "manifest.xml"
    path.write_text(xml)

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(
        manifest,
        """
d | r | model   | property                        | type                    | ref    | source
dataset                                     |                         |        |
  | resource                                | dask/xml                |        | manifest.xml
                                            |                         |        |
  |   | Galaxy                              |                         |        | /galaxy
  |   |   | name                            | string unique           |        | @name
  |   |   | solar_system_name               | string unique           |        | solar_system/@name
  |   |   | solar_system_planet_name        | string unique           |        | solar_system/planet/@name
                                            |                         |        |
  |   | Country                             |                         |        | /galaxy/solar_system/planet/countries/country
  |   |   | code                            | string unique           |        | code
  |   |   | name                            | string unique           |        | name
  |   |   | location_lat                    | integer unique          |        | location/@lat
  |   |   | location_lon                    | integer unique          |        | location/@lon
  |   |   | galaxy                          | ref                     | Galaxy | ../../../..

""",
        context,
    )
    assert a == b


def test_xml_allowed_namespace(rc: RawConfig, tmp_path: Path):
    xml = """
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
        </countries>"""
    path = tmp_path / "manifest.xml"
    path.write_text(xml)

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(
        manifest,
        """
d | r | model   | property     | type           | ref   | source                 | uri
dataset                  |                |       |                        |
                         | prefix         | xsi   |                        | http://www.example.com/xmlns/xsi
                         |                | xmlns |                        | http://www.example.com/xmlns
                         |                | new   |                        | http://www.example.com/xmlns/new
                         |                | test  |                        | http://www.example.com/xmlns/test
                         |                |       |                        |
  | resource             | dask/xml       |       | manifest.xml           |
                         |                |       |                        |
  |   | Country          |                |       | /countries/new:country |
  |   |   | code         | string unique  |       | @xsi:code              |
  |   |   | name         | string unique  |       | @name                  |
  |   |   | location_lon | integer unique |       | location/test:lon      |
  |   |   | location_lat | integer unique |       | location/test:lat      |

""",
        context,
    )
    assert a == b


def test_xml_disallowed_namespace(rc: RawConfig, tmp_path: Path):
    xml = """
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
        </countries>"""
    path = tmp_path / "manifest.xml"
    path.write_text(xml)

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(
        manifest,
        """
d | r | model   | property      | type                | ref       | source                 | uri
dataset                   |                     |           |                        |
                          | prefix              | xmlns     |                        | http://www.example.com/xmlns
                          |                     | new       |                        | http://www.example.com/xmlns/new
                          |                     | test      |                        | http://www.example.com/xmlns/test
                          |                     |           |                        |
  | resource              | dask/xml            |           | manifest.xml           |
                          |                     |           |                        |
  |   | Countries         |                     |           | /countries             |
  |   |   | xsi           | url unique          |           | @test:xsi              |
                          |                     |           |                        |
  |   | Country           |                     |           | /countries/new:country |
  |   |   | xsi_code      | string unique       |           | @xsi:code              |
  |   |   | name          | string unique       |           | @name                  |
  |   |   | location_lon  | integer unique      |           | location/test:lon      |
  |   |   | location_lat  | integer unique      |           | location/test:lat      |
  |   |   | countries     | ref                 | Countries | ..                     |

""",
        context,
    )
    assert a == b


def test_xml_inherit_nested(rc: RawConfig, tmp_path: Path):
    xml = """
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
    </countries>"""
    path = tmp_path / "manifest.xml"
    path.write_text(xml)

    context, manifest = load_manifest_and_context(rc, path)
    commands.get_dataset(context, manifest, "dataset").resources["resource"].external = "manifest.xml"
    a, b = compare_manifest(
        manifest,
        """
d | r | m | property          | type                    | ref     | source
dataset                       |                         |         |
  | resource                  | dask/xml                |         | manifest.xml
                              |                         |         |
  |   | Country               |                         |         | /countries/country
  |   |   | name              | string unique           |         | @name
  |   |   | code              | string unique           |         | @code
  |   |   | location_test     | string unique           |         | location/@test
  |   |   | location_coords[] | number                  |         | location/coords
                              |                         |         |
  |   | Geo                   |                         |         | /countries/country/location/geos/geo
  |   |   | geo_test          | string unique           |         | @geo_test
  |   |   | country           | ref                     | Country | ../../..
                              |                         |         |
  |   | Geo1                  |                         |         | /countries/country/cities/city/location/geos/geo
  |   |   | geo_test          | integer unique          |         | @geo_test
  |   |   | city              | ref                     | City    | ../../..
                              |                         |         |
  |   | City                  |                         |         | /countries/country/cities/city
  |   |   | name              | string unique           |         | @name
  |   |   | location_coords[] | number                  |         | location/coords
  |   |   | country           | ref                     | Country | ../..
""",
        context,
    )
    assert a == b
