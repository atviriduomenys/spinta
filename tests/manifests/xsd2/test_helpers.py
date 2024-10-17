import pytest

from spinta.manifests.xsd2.helpers import XSDReader, State, XSDProperty, XSDType, XSDModel, XSDDatasetResource, _is_array
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
