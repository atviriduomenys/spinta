from pyexpat import model
from typing import Callable
import pytest

from spinta.cli import data
from spinta.manifests.xsd2.helpers import XSDReader, State, XSDProperty, XSDType, XSDModel, XSDDatasetResource
from unittest.mock import MagicMock, patch
from lxml import etree


def test_process_element_inline_type():
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="cityPopulation" type="integer"/>
    </xs:schema>"""
    state = State()
    xsd_reader = XSDReader("test", "test")

    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]
    # Call the method to process the schema (assuming process_element parses XSD and returns an XSDProperty instance)
    result = xsd_reader.process_element(element, state)[0]

    # Expected values based on the given requirements
    assert isinstance(result, XSDProperty)

    # Assert that the 'name' is 'city_population'
    assert result.xsd_name == "cityPopulation"

    # Assert that the 'source' is 'cityPopulation'
    assert result.source == "cityPopulation/text()"

    # Assert that 'required' is True
    assert result.required is True

    assert result.is_array is False

    # Assert that 'type' is an instance of XSDType and is of type 'integer'
    assert isinstance(result.type, XSDType)
    assert result.type.name == "integer"


def test_process_element_inline_type_array():
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="cityPopulation" type="integer" maxOccurs="unbounded"/>
    </xs:schema>"""
    state = State()
    xsd_reader = XSDReader("test", "test")

    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]
    # Call the method to process the schema (assuming process_element parses XSD and returns an XSDProperty instance)
    result = xsd_reader.process_element(element, state)[0]

    # Expected values based on the given requirements
    assert isinstance(result, XSDProperty)

    # Assert that the 'name' is 'city_population'
    assert result.xsd_name == "cityPopulation"

    # Assert that the 'source' is 'cityPopulation'
    assert result.source == "cityPopulation/text()"

    # Assert that 'required' is True
    assert result.required is True

    assert result.is_array is True

    # Assert that 'type' is an instance of XSDType and is of type 'integer'
    assert isinstance(result.type, XSDType)
    assert result.type.name == "integer"


def test_process_element_simple_type():
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="cityPopulation">
      <xs:simpleType>
        <xs:restriction base="xs:integer">
          <xs:minInclusive value="1"/>
          <xs:maxInclusive value="1000000000"/>
        </xs:restriction>
      </xs:simpleType> 
      </xs:element>
    </xs:schema>"""
    state = State()
    xsd_reader = XSDReader("test", "test")
    xsd_type = XSDType()
    xsd_type.xsd_type = "cityPopulation"
    xsd_type.name = "integer"
    xsd_reader.custom_types = {"populationType": xsd_type}
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]
    # Call the method to process the schema (assuming process_element parses XSD and returns an XSDProperty instance)
    result = xsd_reader.process_element(element, state)[0]

    # Expected values based on the given requirements
    assert isinstance(result, XSDProperty)

    assert result.xsd_name == "cityPopulation"

    # Assert that the 'source' is 'cityPopulation'
    assert result.source == "cityPopulation/text()"

    # Assert that 'required' is True
    assert result.required is True

    assert result.is_array is False

    # Assert that 'type' is an instance of XSDType and is of type 'integer'
    assert isinstance(result.type, XSDType)
    assert result.type.name == "integer"


def test_process_element_simple_type_array():
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="cityPopulation" maxOccurs="unbounded">
      <xs:simpleType>
        <xs:restriction base="xs:integer">
          <xs:minInclusive value="1"/>
          <xs:maxInclusive value="1000000000"/>
        </xs:restriction>
      </xs:simpleType> 
      </xs:element>
    </xs:schema>"""
    state = State()
    xsd_reader = XSDReader("test", "test")
    xsd_type = XSDType()
    xsd_type.xsd_type = "cityPopulation"
    xsd_type.name = "integer"
    xsd_reader.custom_types = {"populationType": xsd_type}
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]
    # Call the method to process the schema (assuming process_element parses XSD and returns an XSDProperty instance)
    result = xsd_reader.process_element(element, state)[0]

    # Expected values based on the given requirements
    assert isinstance(result, XSDProperty)

    assert result.xsd_name == "cityPopulation"

    # Assert that the 'source' is 'cityPopulation'
    assert result.source == "cityPopulation/text()"

    # Assert that 'required' is True
    assert result.required is True

    assert result.is_array is True

    # Assert that 'type' is an instance of XSDType and is of type 'integer'
    assert isinstance(result.type, XSDType)
    assert result.type.name == "integer"


def test_process_element_separate_simple_type():
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:simpleType name="populationType">
        <xs:restriction base="xs:integer">
          <xs:minInclusive value="1"/>
          <xs:maxInclusive value="1000000000"/>
        </xs:restriction>
      </xs:simpleType>
      <xs:element name="cityPopulation" type="populationType"/>
    </xs:schema>"""
    state = State()
    xsd_reader = XSDReader("test", "test")
    xsd_type = XSDType()
    xsd_type.xsd_type = "cityPopulation"
    xsd_type.name = "integer"
    xsd_reader.custom_types = {"populationType": xsd_type}
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]
    # Call the method to process the schema (assuming process_element parses XSD and returns an XSDProperty instance)
    result = xsd_reader.process_element(element, state)[0]

    # Expected values based on the given requirements
    assert isinstance(result, XSDProperty)

    assert result.xsd_name == "cityPopulation"

    # Assert that the 'source' is 'cityPopulation'
    assert result.source == "cityPopulation/text()"

    # Assert that 'required' is True
    assert result.required is True

    assert result.is_array is False

    # Assert that 'type' is an instance of XSDType and is of type 'integer'
    assert isinstance(result.type, XSDType)
    assert result.type.name == "integer"


def test_process_element_separate_simple_type_array():
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:simpleType name="populationType">
        <xs:restriction base="xs:integer">
          <xs:minInclusive value="1"/>
          <xs:maxInclusive value="1000000000"/>
        </xs:restriction>
      </xs:simpleType>
      <xs:element name="cityPopulation" type="populationType" maxOccurs="unbounded"/>
    </xs:schema>"""
    state = State()
    xsd_reader = XSDReader("test", "test")
    xsd_type = XSDType()
    xsd_type.xsd_type = "cityPopulation"
    xsd_type.name = "integer"
    xsd_reader.custom_types = {"populationType": xsd_type}
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]
    # Call the method to process the schema (assuming process_element parses XSD and returns an XSDProperty instance)
    result = xsd_reader.process_element(element, state)[0]

    # Expected values based on the given requirements
    assert isinstance(result, XSDProperty)

    assert result.xsd_name == "cityPopulation"

    # Assert that the 'source' is 'cityPopulation'
    assert result.source == "cityPopulation/text()"

    assert result.is_array is True

    # Assert that 'required' is True
    assert result.required is True

    # Assert that 'type' is an instance of XSDType and is of type 'integer'
    assert isinstance(result.type, XSDType)
    assert result.type.name == "integer"


@patch.object(XSDReader, 'process_complex_type')
def test_process_element_complex_type(mock_process_complex_type):
    # Mocking the return value of process_complex_type
    xsd_schema = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

  <!-- Definition of an element 'country' with a complexType specified inside -->
  <xs:element name="country">
    <xs:complexType>
      <xs:sequence>
        <!-- Two elements inside the complexType -->
        <xs:element name="name" type="xs:string"/>
        <xs:element name="population" type="xs:integer"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>

</xs:schema>
    """
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]
    dataset_resource = XSDDatasetResource(dataset_name="test")
    mock_model_1 = XSDModel(dataset_resource=dataset_resource)
    mock_model_1.name = "Country",
    mock_model_1.source = "country",

    mock_property1 = XSDProperty()
    mock_property1.xsd_name = "name"
    mock_property1.source = "name"
    mock_property1.type = "string"

    mock_property2 = XSDProperty()
    mock_property2.xsd_name = "population"
    mock_property2.source = "population"
    mock_property2.type = "integer"

    mock_model_1.properties = {
        "name": mock_property1,
        "population": mock_property2,
    },
    mock_model_1.is_root_model = False

    # Set the return value of the mocked process_complex_type
    mock_process_complex_type.return_value = [mock_model_1, ]

    # Create an instance of XSDReader
    reader = XSDReader("test", "test")

    # Call the method you want to test (which uses process_complex_type internally)
    state = State()
    result = reader.process_element(element, state)[0]

    assert isinstance(result, XSDProperty)

    # Check that the first model's name is 'Country'
    assert result.xsd_name == "country"

    assert result.source == "country"

    assert result.is_array is False

    # Assert that 'required' is True
    assert result.required is True

    assert result.type.name == "ref"


def test_process_element_complex_type_separate():
    xsd_schema = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

  <!-- Definition of an element 'country' with a complexType specified inside -->
  <xs:element name="country" type="countryType"/>
    <xs:complexType name="countryType">
      <xs:sequence>
        <!-- Two elements inside the complexType -->
        <xs:element name="name" type="xs:string"/>
        <xs:element name="population" type="xs:integer"/>
      </xs:sequence>
    </xs:complexType>

</xs:schema>
    """
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]

    # Create an instance of XSDReader
    reader = XSDReader("test", "test")

    # Call the method you want to test (which uses process_complex_type internally)
    state = State()
    result = reader.process_element(element, state)[0]

    assert isinstance(result, XSDProperty)

    # Check that the first model's name is 'Country'
    assert result.xsd_name == "country"

    assert result.source == "country"

    assert result.is_array is False

    # Assert that 'required' is True
    assert result.required is True


def test_process_element_ref():
    xsd_schema = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
                <xs:element ref="country"/>
</xs:schema>

    """
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]

    # Create an instance of XSDReader
    reader = XSDReader("test", "test")

    # Call the method you want to test (which uses process_complex_type internally)
    state = State()
    result = reader.process_element(element, state)[0]

    assert isinstance(result, XSDProperty)

    # Check that the first model's name is 'Country'
    assert result.xsd_name == "country"

    assert result.source == "country"

    assert result.is_array is False

    # Assert that 'required' is True
    assert result.required is True

    assert result.type.name == 'ref'


def test_process_element_ref_backref():
    xsd_schema = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
                <xs:element ref="country" maxOccurs="unbounded"/>
</xs:schema>

    """
    xsd_root = etree.fromstring(xsd_schema)
    element = xsd_root.xpath('./*[local-name() = "element"]')[0]

    # Create an instance of XSDReader
    reader = XSDReader("test", "test")

    # Call the method you want to test (which uses process_complex_type internally)
    state = State()
    result = reader.process_element(element, state)[0]

    assert isinstance(result, XSDProperty)

    # Check that the first model's name is 'Country'
    assert result.xsd_name == "country"

    assert result.source == "country"

    assert result.is_array is True

    # Assert that 'required' is True
    assert result.required is True

    assert result.type.name == 'backref'

# tests for process_complex_type


def test_process_complex_type_attributes():
    xsd = """
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

  <!-- Define the root element 'country' -->
  <xs:element name="country">
    <xs:complexType>
        <!-- Define the 'name' and 'code' attributes inside 'country' -->
        <xs:attribute name="name" type="xs:string"/>
        <xs:attribute name="code" type="xs:string"/>

    </xs:complexType>
  </xs:element>

</xs:schema>

    """
    schema = etree.XML(xsd)

    root = schema.find(".//{http://www.w3.org/2001/XMLSchema}complexType")
    state = State()
    # Create an instance of XSDReader
    reader = XSDReader("test", "test")
    models = reader.process_complex_type(root, state)

    assert isinstance(models[0], XSDModel)

    assert len(reader.models) == 1

    assert len(models[0].properties) == 2


# Test process_choice
@pytest.fixture
def setup_instance():
    """Fixture to set up an instance of XSDReader and mock state."""
    instance = XSDReader("test", "test")
    state = MagicMock(spec=State)
    return instance, state


@pytest.fixture
def sample_xsd():
    """Fixture to provide an XML Schema (XSD) with complexType and choice."""
    xsd_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="root">
            <xs:complexType>
                <xs:choice>
                    <xs:element name="element1" type="xs:string"/>
                    <xs:element name="element2" type="xs:string"/>
                </xs:choice>
            </xs:complexType>
        </xs:element>
    </xs:schema>
    """
    return etree.XML(xsd_string)


def test_process_choice_with_choice_node(setup_instance, sample_xsd):
    """Test when the node contains a choice element in a complexType."""
    instance, state = setup_instance

    # Parse the XML node for 'root' element and its children
    root = sample_xsd.find(".//{http://www.w3.org/2001/XMLSchema}choice")

    # Mock process_element for the child elements inside the choice
    with patch.object(instance, 'process_element', return_value=['element_property']) as mock_process_element:
        result = instance.process_choice(root, state)

        # Ensure the process_element is called for each choice element
        assert mock_process_element.call_count == 2
        assert result == [['element_property'], ['element_property']]


def test_process_choice_ignores_comments(setup_instance):
    """Test that comment nodes are ignored."""
    xsd_string = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="root">
            <xs:complexType>
                <!-- This is a comment -->
                <xs:choice>
                    <xs:element name="element1" type="xs:string"/>
                </xs:choice>
            </xs:complexType>
        </xs:element>
    </xs:schema>
    """
    schema = etree.XML(xsd_string)
    instance, state = setup_instance

    root = schema.find(".//{http://www.w3.org/2001/XMLSchema}choice")

    # Mock process_element for the child element
    with patch.object(instance, 'process_element', return_value=['element_property']) as mock_process_element:
        result = instance.process_choice(root, state)

        # Ensure comments are ignored and element is processed
        mock_process_element.assert_called_once_with(root.find(".//{http://www.w3.org/2001/XMLSchema}element"), state)
        assert result == [['element_property']]


def test_process_choice_with_array(setup_instance):
        xsd_schema = """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:choice maxOccurs="unbounded">
                <xs:element name="ItemA" type="xs:string"/>
                <xs:element name="ItemB" type="xs:string"/>
                <xs:element name="ItemRef" type="ref"/>
            </xs:choice>
        </xs:schema>
        """
        xsd_reader, state = setup_instance

        xsd_root = etree.fromstring(xsd_schema)
        choice = xsd_root.find('.//xs:choice', namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

        property_groups = xsd_reader.process_choice(choice, state)

        assert len(property_groups) == 1
        assert len(property_groups[0]) == 3

        expected_results = {
            "ItemA": ("string", True),
            "ItemB": ("string", True),
            "ItemRef": ("backref", True),
        }

        for prop in property_groups[0]:
            assert prop.xsd_name in expected_results, f"Unexpected property name: {prop.xsd_name}"

            expected_type, expected_is_array = expected_results[prop.xsd_name]
            assert prop.type.name == expected_type, f"Property '{prop.xsd_name}' has incorrect type."
            assert prop.is_array == expected_is_array, f"Property '{prop.xsd_name}' array flag incorrect."


def test_process_sequence_only_elements(setup_instance):
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:sequence>
            <xs:element name="Name" type="xs:string"/>
            <xs:element name="Population" type="xs:integer"/>
            <xs:element name="Area" type="xs:decimal" minOccurs="0"/>
        </xs:sequence>
    </xs:schema>
    """
    xsd_reader, state = setup_instance

    xsd_root = etree.fromstring(xsd_schema)

    sequence = xsd_root.xpath('./*[local-name() = "sequence"]')[0]
    property_groups = xsd_reader.process_sequence(sequence, state)

    # Expected property groups: only one group since there are no choices
    assert len(property_groups) == 1

    # Extract property names from the group
    group = property_groups[0]
    property_names = [prop.xsd_name for prop in group]

    expected_names = ["Name", "Population", "Area"]
    assert property_names == expected_names


def test_process_sequence_with_single_choice(setup_instance):
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:sequence>
            <xs:element name="Name" type="xs:string"/>
            <xs:choice>
                <xs:element name="Capital" type="xs:string"/>
                <xs:element name="Population" type="xs:integer"/>
            </xs:choice>
            <xs:element name="Area" type="xs:decimal"/>
        </xs:sequence>
    </xs:schema>
    """
    xsd_reader, state = setup_instance

    xsd_root = etree.fromstring(xsd_schema)
    sequence = xsd_root.xpath('./*[local-name() = "sequence"]')[0]

    property_groups = xsd_reader.process_sequence(sequence, state)

    # Expected property groups: two groups due to the choice between Population and Area
    assert len(property_groups) == 2

    group1 = property_groups[0]
    group1_names = [prop.xsd_name for prop in group1]
    group2 = property_groups[1]
    group2_names = [prop.xsd_name for prop in group2]

    expected_group1 = ["Name", "Capital", "Area"]
    expected_group2 = ["Name", "Population", "Area"]

    assert group1_names == expected_group1
    assert group2_names == expected_group2


def test_process_sequence_with_multiple_choices(setup_instance):
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:sequence>
            <xs:element name="Name" type="xs:string"/>
            <xs:choice>
                <xs:element name="Mayor" type="xs:string"/>
                <xs:element name="CouncilSize" type="xs:integer"/>
            </xs:choice>
            <xs:choice>
                <xs:element name="Founded" type="xs:date"/>
                <xs:element name="Population" type="xs:integer"/>
            </xs:choice>
            <xs:element name="Area" type="xs:decimal"/>
        </xs:sequence>
    </xs:schema>
    """
    xsd_reader, state = setup_instance

    xsd_root = etree.fromstring(xsd_schema)
    sequence = xsd_root.find('.//xs:sequence', namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

    property_groups = xsd_reader.process_sequence(sequence, state)

    # Expected property groups: four groups due to two choices with two options each
    assert len(property_groups) == 4, "There should be exactly four property groups."

    expected_groups = [
        ["Name", "Mayor", "Founded", "Area"],
        ["Name", "Mayor", "Population", "Area"],
        ["Name", "CouncilSize", "Founded", "Area"],
        ["Name", "CouncilSize", "Population", "Area"],
    ]

    for group, expected in zip(property_groups, expected_groups):
        property_names = [prop.xsd_name for prop in group]
        assert property_names == expected, "Property group does not match any expected group."


def test_process_sequence_with_nested_sequence_in_choice(setup_instance):
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:sequence>
            <xs:element name="RegionName" type="xs:string"/>
            <xs:choice>
                <xs:sequence>
                    <xs:element name="Country" type="xs:string"/>
                    <xs:element name="Population" type="xs:integer"/>
                </xs:sequence>
                <xs:sequence>
                    <xs:element name="Province" type="xs:string"/>
                    <xs:element name="Area" type="xs:decimal"/>
                </xs:sequence>
            </xs:choice>
            <xs:element name="Climate" type="xs:string"/>
        </xs:sequence>
    </xs:schema>
    """
    xsd_reader, state = setup_instance

    xsd_root = etree.fromstring(xsd_schema)
    sequence = xsd_root.find('.//xs:sequence', namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

    property_groups = xsd_reader.process_sequence(sequence, state)

    # Expected property groups: two groups due to the choice between two nested sequences
    expected_groups = [
        ["RegionName", "Country", "Population", "Climate"],
        ["RegionName", "Province", "Area", "Climate"],
    ]

    assert len(property_groups) == 2, "There should be exactly two property groups."

    group1 = property_groups[0]
    group1_names = [prop.xsd_name for prop in group1]
    group2 = property_groups[1]
    group2_names = [prop.xsd_name for prop in group2]

    assert group1_names == expected_groups[0], "First property group does not match expected names."
    assert group2_names == expected_groups[1], "Second property group does not match expected names."


def test_process_sequence_only_choices(setup_instance):
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:sequence>
            <xs:choice>
                <xs:element name="Online" type="xs:boolean"/>
                <xs:element name="Onsite" type="xs:boolean"/>
            </xs:choice>
            <xs:choice>
                <xs:element name="FullTime" type="xs:boolean"/>
                <xs:element name="PartTime" type="xs:boolean"/>
            </xs:choice>
        </xs:sequence>
    </xs:schema>
    """
    xsd_reader, state = setup_instance

    xsd_root = etree.fromstring(xsd_schema)
    sequence = xsd_root.find('.//xs:sequence', namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

    property_groups = xsd_reader.process_sequence(sequence, state)

    expected_groups = [
        ["Online", "FullTime"],
        ["Online", "PartTime"],
        ["Onsite", "FullTime"],
        ["Onsite", "PartTime"],
    ]

    assert len(property_groups) == 4, "There should be exactly four property groups."

    for group, expected in zip(property_groups, expected_groups):
        property_names = [prop.xsd_name for prop in group]
        assert property_names == expected, "Property group does not match any expected group."

@pytest.fixture
def xsd_reader():
    return XSDReader("test", "test")

@pytest.fixture
def create_xsd_model() -> Callable[..., XSDModel]:
    def _create(name: str) -> XSDModel:
        dataset_resource = XSDDatasetResource(dataset_name="test")
        model = XSDModel(dataset_resource=dataset_resource)
        model.xsd_name = name
        return model
    return _create

@pytest.fixture
def create_ref_xsd_property() -> Callable[..., XSDProperty]:
    def _create(name: str, type: str, ref_model: str) -> XSDProperty:
        prop = XSDProperty(
            xsd_name=name,
            property_type=XSDType(name=type),
            required=True,
        )
        if type == "ref":
            prop.xsd_ref_to = ref_model
        elif type == "type":
            prop.xsd_type_to = ref_model
        return prop
    return _create

@pytest.fixture
def setup_models(xsd_reader, create_xsd_model, create_ref_xsd_property):
    model_a = create_xsd_model("ModelA")
    model_b = create_xsd_model("ModelB")

    prop1 = create_ref_xsd_property("prop1", "ref", model_b.xsd_name)
    prop2 = create_ref_xsd_property("prop2", "type", model_a.xsd_name)

    model_a.properties["prop1"] = prop1
    model_b.properties["prop2"] = prop2

    xsd_reader.models.extend([model_a, model_b])

    xsd_reader.top_level_element_models[model_b.xsd_name] = model_b
    xsd_reader.top_level_complex_type_models[model_a.xsd_name] = model_a
    
    return {
        "xsd_reader": xsd_reader,
        "models": {
            "ModelA": model_a,
            "ModelB": model_b,
        },
        "properties": {
            "prop1": prop1,
            "prop2": prop2,
        }
    }


def test_post_process_refs_links_existing_references(setup_models):
    """
    Test that _post_process_refs correctly links properties to existing models.
    """
    xsd_reader, models, properties = setup_models["xsd_reader"], setup_models["models"], setup_models["properties"]
    model_a, model_b = models["ModelA"], models["ModelB"]
    prop1, prop2 = properties["prop1"], properties["prop2"]
    
    xsd_reader._post_process_refs()
        
    # Property1 in ModelA should link to ModelB via ref_model
    assert prop1.ref_model is not None
    assert prop1.ref_model == model_b
    
    # Property2 in ModelB should link to ModelA via ref_model
    assert prop2.ref_model is not None
    assert prop2.ref_model == model_a


@pytest.fixture
def create_xsd_models():
    """Fixture to create sample XSDModel instances."""
    france = XSDModel("test")

    france.name = "France"
    france.properties = {
        "lyon": XSDProperty(),
        "paris": XSDProperty()
    }
    france.properties["lyon"].name = "lyon"
    france.properties["paris"].name = "paris"

    germany = XSDModel("test")

    germany.name = "Germany"
    germany.properties = {
        "hamburg": XSDProperty(),
        "berlin": XSDProperty()
    }
    germany.properties["hamburg"].name = "hamburg"
    germany.properties["berlin"].name = "berlin"

    italy = XSDModel("test")

    italy.name = "Italy"
    italy.properties = {
        "rome": XSDProperty(),
        "milan": XSDProperty()
    }
    italy.properties["rome"].name = "rome"
    italy.properties["milan"].name = "milan"
    return [italy, france, germany]


def test_sort_models_by_name(create_xsd_models):
    """Test that the XSDModel list is sorted by the 'name' attribute (country name)."""
    models = create_xsd_models
    sorted_models = sorted(models, key=lambda model: model.name)

    # Extracting the sorted names to verify the order
    sorted_names = [model.name for model in sorted_models]

    assert sorted_names == ["France", "Germany", "Italy"]


def test_sort_properties_by_key(create_xsd_models):
    """Test that the properties dictionary in each XSDModel is sorted by key (city name)."""
    models = create_xsd_models

    for model in models:
        sorted_properties = dict(sorted(model.properties.items()))

        # Extract the sorted city names (keys) to verify the order
        sorted_keys = list(sorted_properties.keys())

        # Check that properties are sorted correctly for each model
        if model.name == "France":
            assert sorted_keys == ["lyon", "paris"]
        elif model.name == "Germany":
            assert sorted_keys == ["berlin", "hamburg"]
        elif model.name == "Italy":
            assert sorted_keys == ["milan", "rome"]

