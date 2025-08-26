from typing import Callable
import pytest

from spinta.manifests.xsd2.helpers import XSDReader, State, XSDProperty, XSDType, XSDModel, XSDDatasetResource
from unittest.mock import MagicMock, patch
from lxml import etree

from spinta.utils.naming import to_property_name


def test_process_element_inline_type():
    xsd_schema = """
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
      <xs:element name="cityPopulation" type="integer"/>
    </xs:schema>"""
    state = State()
    xsd_reader = XSDReader("test", "test")

    xsd_root = etree.fromstring(xsd_schema)
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap

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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
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


@patch.object(XSDReader, "process_complex_type")
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
    mock_model_1.name = ("Country",)
    mock_model_1.source = ("country",)

    mock_property1 = XSDProperty()
    mock_property1.xsd_name = "name"
    mock_property1.source = "name"
    mock_property1.type = "string"

    mock_property2 = XSDProperty()
    mock_property2.xsd_name = "population"
    mock_property2.source = "population"
    mock_property2.type = "integer"

    mock_model_1.properties = (
        {
            "name": mock_property1,
            "population": mock_property2,
        },
    )
    mock_model_1.is_root_model = False

    # Set the return value of the mocked process_complex_type
    mock_process_complex_type.return_value = [
        mock_model_1,
    ]

    # Create an instance of XSDReader
    xsd_reader = XSDReader("test", "test")
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
    # Call the method you want to test (which uses process_complex_type internally)
    state = State()
    result = xsd_reader.process_element(element, state)[0]

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
    reader.root = xsd_root
    reader.namespaces = xsd_root.nsmap
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


def test_process_complex_type_with_extension(xsd_reader, create_xsd_model):
    complex_type_xml = """
    <complexType name="DerivedType">
        <complexContent>
            <extension base="BaseType">
                <sequence>
                    <element name="extendedProp" type="xs:string"/>
                </sequence>
                <element name="attr1" type="xs:int"/>
            </extension>
        </complexContent>
    </complexType>
    """
    complex_type_node = etree.fromstring(complex_type_xml)

    xsd_reader.process_complex_content = MagicMock(
        return_value=[
            [
                XSDProperty(xsd_name="extendedProp", property_type=XSDType(name="string")),
                XSDProperty(xsd_name="attr1", property_type=XSDType(name="int")),
            ]
        ]
    )
    state = State()
    state.extends_model = "BaseType"
    base_model = create_xsd_model("BaseType")
    xsd_reader.top_level_complex_type_models["BaseType"] = base_model

    models = xsd_reader.process_complex_type(complex_type_node, state)

    assert len(models) == 1, "Should return one model"
    model = models[0]
    prop_names = list(model.properties.keys())

    assert "extended_prop" in prop_names
    assert "attr1" in prop_names
    assert model.extends_model == "BaseType"


def test_process_complex_type_with_simple_content(xsd_reader):
    complex_type_xml = """
    <complexType name="CityType">
        <simpleContent>
            <extension base="xs:string">
                <attribute name="name" type="xs:string" use="required"/>
                <attribute name="code" type="xs:string" use="optional"/>
            </extension>
        </simpleContent>
    </complexType>
    """
    complex_type_node = etree.fromstring(complex_type_xml)

    xsd_reader.process_simple_content = MagicMock(
        return_value=[
            XSDProperty(xsd_name="text", property_type=XSDType(name="string")),
            XSDProperty(xsd_name="name", property_type=XSDType(name="string"), required=True),
            XSDProperty(xsd_name="code", property_type=XSDType(name="string"), required=False),
        ]
    )
    xsd_reader.dataset_resource.dataset_name = "dataset_name"

    state = State()
    models = xsd_reader.process_complex_type(complex_type_node, state)

    assert len(models) == 1
    model = models[0]
    assert model.basename == "CityType"
    assert model.name == "dataset_name/CityType"
    assert "text" in model.properties
    assert "name" in model.properties
    assert "code" in model.properties

    text_prop = model.properties["text"]
    name_prop = model.properties["name"]
    code_prop = model.properties["code"]

    assert text_prop.type.name == "string"
    assert name_prop.type.name == "string"
    assert code_prop.type.name == "string"
    assert name_prop.required is True
    assert code_prop.required is False

    xsd_reader.process_simple_content.assert_called_once_with(complex_type_node[0], state)


def test_process_complex_type_with_all(xsd_reader):
    xml = """
    <complexType name="TestType">
        <all>
            <element name="elementOne" type="xs:string" />
            <element name="elementTwo" type="xs:int" />
        </all>
    </complexType>
    """
    node = etree.fromstring(xml)
    xsd_reader.root = node
    xsd_reader.namespaces = []
    state = State()
    models = xsd_reader.process_complex_type(node, state)
    assert len(models) == 1
    model = models[0]

    assert len(model.properties) == 2
    assert model.properties["element_one"].type.name == "string"
    assert model.properties["element_one"].xsd_name == "elementOne"
    assert model.properties["element_two"].type.name == "integer"
    assert model.properties["element_two"].xsd_name == "elementTwo"


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
    reader.root = xsd_root
    reader.namespaces = xsd_root.nsmap
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
    reader.root = xsd_root
    reader.namespaces = xsd_root.nsmap
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

    assert result.type.name == "backref"


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
    with patch.object(instance, "process_element", return_value=["element_property"]) as mock_process_element:
        result = instance.process_choice(root, state)

        # Ensure the process_element is called for each choice element
        assert mock_process_element.call_count == 2
        assert result == [["element_property"], ["element_property"]]


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
    with patch.object(instance, "process_element", return_value=["element_property"]) as mock_process_element:
        result = instance.process_choice(root, state)

        # Ensure comments are ignored and element is processed
        mock_process_element.assert_called_once_with(root.find(".//{http://www.w3.org/2001/XMLSchema}element"), state)
        assert result == [["element_property"]]


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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
    choice = xsd_root.find(".//xs:choice", namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap

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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
    sequence = xsd_root.find(".//xs:sequence", namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
    sequence = xsd_root.find(".//xs:sequence", namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

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
    xsd_reader.root = xsd_root
    xsd_reader.namespaces = xsd_root.nsmap
    sequence = xsd_root.find(".//xs:sequence", namespaces={"xs": "http://www.w3.org/2001/XMLSchema"})

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
        prop.name = to_property_name(name)
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

    xsd_reader.top_level_element_models[model_b.xsd_name] = [model_b]
    xsd_reader.top_level_complex_type_models[model_a.xsd_name] = [model_a]

    return {
        "xsd_reader": xsd_reader,
        "models": {
            "ModelA": model_a,
            "ModelB": model_b,
        },
        "properties": {
            "prop1": prop1,
            "prop2": prop2,
        },
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
    france.properties = {"lyon": XSDProperty(), "paris": XSDProperty()}
    france.properties["lyon"].name = "lyon"
    france.properties["paris"].name = "paris"

    germany = XSDModel("test")

    germany.name = "Germany"
    germany.properties = {"hamburg": XSDProperty(), "berlin": XSDProperty()}
    germany.properties["hamburg"].name = "hamburg"
    germany.properties["berlin"].name = "berlin"

    italy = XSDModel("test")

    italy.name = "Italy"
    italy.properties = {"rome": XSDProperty(), "milan": XSDProperty()}
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


def test_post_process_refs_valid_prepare_with_properties(xsd_reader, create_xsd_model):
    extends_model = create_xsd_model("BaseType")
    base_prop = XSDProperty(xsd_name="baseProp", property_type=XSDType(name="string"))
    base_prop.name = "base_prop"
    extends_model.properties = {"base_prop": base_prop}

    derived_model = create_xsd_model("DerivedType")
    derived_model.extends_model = "BaseType"
    derived_prop = XSDProperty(xsd_name="derivedProp", property_type=XSDType(name="int"))
    derived_prop.name = "derived_prop"
    derived_model.properties = {"derived_prop": derived_prop}

    xsd_reader.models = [derived_model]
    xsd_reader.top_level_complex_type_models = {"BaseType": [extends_model]}

    xsd_reader._post_process_refs()

    assert derived_model.extends_model is extends_model


def test_post_process_refs_valid_prepare_with_empty_properties(xsd_reader, create_xsd_model):
    extends_model = create_xsd_model("BaseType")

    derived_model = create_xsd_model("DerivedType")
    derived_model.extends_model = "BaseType"
    derived_prop = XSDProperty(xsd_name="derivedProp", property_type=XSDType(name="int"))
    derived_prop.name = "derived_prop"
    derived_model.properties = {"derived_prop": derived_prop}

    xsd_reader.models = [derived_model]
    xsd_reader.top_level_complex_type_models = {"BaseType": [extends_model]}

    xsd_reader._post_process_refs()

    assert derived_model.extends_model is None


# def test_process_extension_simple_type(xsd_reader):
#     extension_xml = """
#     <extension base="xs:string">
#         <!-- No child elements -->
#     </extension>
#     """
#     extension_node = etree.fromstring(extension_xml)

#     state = State()

#     type_name = xsd_reader.process_extension(extension_node, state)

#     assert type_name == "string", "Should return the type name 'string'"


def test_process_extension_complex_type_no_children(xsd_reader, create_xsd_model):
    extension_xml = """
    <extension base="BaseType">
        <!-- No child elements -->
    </extension>
    """
    extension_node = etree.fromstring(extension_xml)

    state = State()
    base_model = create_xsd_model("BaseType")
    xsd_reader.top_level_complex_type_models["BaseType"] = base_model

    property_groups = xsd_reader.process_complex_type_extension(extension_node, state)

    assert isinstance(property_groups, list)
    assert len(property_groups) == 1
    assert property_groups[0] == []
    assert state.extends_model == "BaseType"


def test_process_extension_complex_type_with_sequence(xsd_reader, create_xsd_model):
    extension_xml = """
    <extension base="BaseType">
        <sequence>
            <element name="newProp1" type="xs:string"/>
            <element name="newProp2" type="xs:int"/>
        </sequence>
    </extension>
    """
    extension_node = etree.fromstring(extension_xml)

    xsd_reader.process_sequence = MagicMock(
        return_value=[
            [
                XSDProperty(xsd_name="newProp1", property_type=XSDType(name="string")),
                XSDProperty(xsd_name="newProp2", property_type=XSDType(name="int")),
            ]
        ]
    )

    state = State()
    base_model = create_xsd_model("BaseType")
    xsd_reader.top_level_complex_type_models["BaseType"] = base_model

    property_groups = xsd_reader.process_complex_type_extension(extension_node, state)

    assert len(property_groups) == 1
    prop_names = [prop.xsd_name for prop in property_groups[0]]
    assert "newProp1" in prop_names
    assert "newProp2" in prop_names
    assert state.extends_model == "BaseType"


def test_process_extension_complex_type_with_choice(xsd_reader, create_xsd_model):
    extension_xml = """
    <extension base="BaseType">
        <choice>
            <element name="choiceProp1" type="xs:string"/>
            <element name="choiceProp2" type="xs:int"/>
        </choice>
    </extension>
    """
    extension_node = etree.fromstring(extension_xml)

    xsd_reader.process_choice = MagicMock(
        return_value=[
            [XSDProperty(xsd_name="choiceProp1", property_type=XSDType(name="string"))],
            [XSDProperty(xsd_name="choiceProp2", property_type=XSDType(name="int"))],
        ]
    )

    state = State()
    base_model = create_xsd_model("BaseType")
    xsd_reader.top_level_complex_type_models["BaseType"] = base_model

    property_groups = xsd_reader.process_complex_type_extension(extension_node, state)

    assert len(property_groups) == 2
    prop_names_group1 = [prop.xsd_name for prop in property_groups[0]]
    prop_names_group2 = [prop.xsd_name for prop in property_groups[1]]
    assert "choiceProp1" in prop_names_group1
    assert "choiceProp2" not in prop_names_group1
    assert "choiceProp2" in prop_names_group2
    assert "choiceProp1" not in prop_names_group2
    assert state.extends_model == "BaseType"


def test_process_extension_complex_type_with_elements(xsd_reader, create_xsd_model):
    extension_xml = """
    <extension base="BaseType">
        <element name="attr1" type="xs:string"/>
        <element name="attr2" type="xs:int"/>
    </extension>
    """
    extension_node = etree.fromstring(extension_xml)

    def mock_process_element(node, state):
        name = node.attrib.get("name")
        type_name = node.attrib.get("type").split(":")[-1]
        return XSDProperty(xsd_name=name, property_type=XSDType(name=type_name))

    xsd_reader.process_element = mock_process_element

    state = State()
    base_model = create_xsd_model("BaseType")
    xsd_reader.top_level_complex_type_models["BaseType"] = base_model

    property_groups = xsd_reader.process_complex_type_extension(extension_node, state)

    assert len(property_groups) == 1
    prop_names = [prop.xsd_name for prop in property_groups[0]]
    assert "attr1" in prop_names
    assert "attr2" in prop_names
    assert state.extends_model == "BaseType"


def test_process_complex_content_with_extension(xsd_reader, create_xsd_model):
    complex_content_xml = """
    <complexContent>
        <extension base="BaseType">
            <sequence>
                <element name="seqProp1" type="xs:string"/>
            </sequence>
            <choice>
                <element name="choiceProp1" type="xs:int"/>
                <element name="choiceProp2" type="xs:float"/>
            </choice>
            <element name="attr1" type="xs:boolean"/>
        </extension>
    </complexContent>
    """
    complex_content_node = etree.fromstring(complex_content_xml)

    state = State()
    base_model = create_xsd_model("BaseType")
    xsd_reader.top_level_complex_type_models["BaseType"] = base_model

    def mock_process_complex_type_extension(node, state) -> list[list[XSDProperty]]:
        state.prepare_statement = 'extend("BaseType")'
        return [
            [
                XSDProperty(xsd_name="seqProp1", property_type=XSDType(name="string")),
                XSDProperty(xsd_name="choiceProp1", property_type=XSDType(name="int")),
                XSDProperty(xsd_name="attr1", property_type=XSDType(name="boolean")),
            ],
            [
                XSDProperty(xsd_name="seqProp1", property_type=XSDType(name="string")),
                XSDProperty(xsd_name="choiceProp2", property_type=XSDType(name="float")),
                XSDProperty(xsd_name="attr1", property_type=XSDType(name="boolean")),
            ],
        ]

    xsd_reader.process_complex_type_extension = mock_process_complex_type_extension

    property_groups = xsd_reader.process_complex_content(complex_content_node, state)

    assert len(property_groups) == 2
    prop_names_group1 = [prop.xsd_name for prop in property_groups[0]]
    prop_names_group2 = [prop.xsd_name for prop in property_groups[1]]
    assert "seqProp1" in prop_names_group1 and "seqProp1" in prop_names_group2
    assert "choiceProp1" in prop_names_group1 and "choiceProp1" not in prop_names_group2
    assert "choiceProp2" in prop_names_group2 and "choiceProp2" not in prop_names_group1
    assert "attr1" in prop_names_group1 and "attr1" in prop_names_group2
    assert state.prepare_statement == 'extend("BaseType")'


def test_process_complex_content_with_restriction(xsd_reader):
    complex_content_xml = """
    <complexContent>
        <restriction base="xs:string">
            <enumeration value="Option1"/>
            <enumeration value="Option2"/>
        </restriction>
    </complexContent>
    """
    complex_content_node = etree.fromstring(complex_content_xml)

    def mock_process_restriction(node, state):
        prop_type = XSDType(name="string")
        prop_type.enums = {"Option1": None, "Option2": None}
        return prop_type

    xsd_reader.process_restriction = mock_process_restriction

    state = State()

    property_groups = xsd_reader.process_complex_content(complex_content_node, state)

    assert len(property_groups) == 1
    prop_group = property_groups[0]
    assert len(prop_group) == 1
    prop = prop_group[0]
    assert prop.xsd_name == "value"
    assert prop.type.name == "string"
    assert prop.type.enums == {"Option1": None, "Option2": None}


def test_process_complex_content_with_mixed_content(xsd_reader):
    complex_content_xml = """
    <complexContent mixed="true">
        <extension base="BaseType">
            <!-- No child elements -->
        </extension>
    </complexContent>
    """
    complex_content_node = etree.fromstring(complex_content_xml)

    xsd_reader.process_complex_type_extension = MagicMock(return_value=[[]])

    state = State()

    property_groups = xsd_reader.process_complex_content(complex_content_node, state)

    assert len(property_groups) == 1
    prop_group = property_groups[0]
    prop_names = [prop.xsd_name for prop in prop_group]
    assert "text" in prop_names


def test_process_simple_type_extension_with_attributes_and_annotation(xsd_reader):
    extension_xml = """
    <extension base="xs:string">
        <attribute name="code" type="xs:string" use="optional"/>
        <annotation>
            <documentation>Text property description</documentation>
        </annotation>
    </extension>
    """
    extension_node = etree.fromstring(extension_xml)
    state = State()

    xsd_reader.process_attribute = MagicMock(
        return_value=XSDProperty(xsd_name="code", property_type=XSDType(name="string"))
    )
    xsd_reader.process_annotation = MagicMock(return_value="Text property description")

    properties = xsd_reader.process_simple_type_extension(extension_node, state)

    assert properties[0].xsd_name == "text"
    assert properties[0].type.name == "string"
    assert properties[0].description == "Text property description"

    assert properties[1].xsd_name == "code"
    assert properties[1].type.name == "string"


def test_process_simple_content_with_extension(xsd_reader):
    simple_content_xml = """
    <simpleContent>
        <extension base="xs:string">
            <attribute name="code" type="xs:string" use="optional"/>
            <annotation>
                <documentation>Text property description</documentation>
            </annotation>
        </extension>
    </simpleContent>
    """
    simple_content_node = etree.fromstring(simple_content_xml)
    state = State()

    xsd_reader.process_simple_type_extension = MagicMock(
        return_value=[
            XSDProperty(xsd_name="text", property_type=XSDType(name="string")),
            XSDProperty(xsd_name="code", property_type=XSDType(name="string"), source="@code"),
        ]
    )

    properties = xsd_reader.process_simple_content(simple_content_node, state)

    assert len(properties) == 2
    assert properties[0].xsd_name == "text"
    assert properties[0].type.name == "string"
    assert properties[1].xsd_name == "code"
    assert properties[1].type.name == "string"


def test_process_simple_content_with_restriction(xsd_reader):
    simple_content_xml = """
    <simpleContent>
        <restriction base="xs:integer"/>
    </simpleContent>
    """
    simple_content_node = etree.fromstring(simple_content_xml)
    state = State()

    xsd_reader.process_restriction = MagicMock(return_value=XSDType(name="integer"))

    properties = xsd_reader.process_simple_content(simple_content_node, state)

    assert len(properties) == 1
    assert properties[0].xsd_name == "value"
    assert properties[0].type.name == "integer"


def test_process_all(xsd_reader):
    xml = """
    <all>
        <element name="elementOne" type="xs:int" />
        <element name="elementTwo" type="xs:float" />
        <element name="elementThree" type="xs:boolean" />
    </all>
    """
    node = etree.fromstring(xml)
    xsd_reader.root = node
    xsd_reader.namespaces = []
    state = State()
    properties = xsd_reader.process_all(node, state)

    assert len(properties) == 3
    assert properties[0].xsd_name == "elementOne"
    assert properties[0].type.name == "integer"
    assert properties[1].xsd_name == "elementTwo"
    assert properties[1].type.name == "number"
    assert properties[2].xsd_name == "elementThree"
    assert properties[2].type.name == "boolean"


def test_process_all_with_attributes(xsd_reader):
    xml = """
    <all>
        <element name="requiredElement" type="xs:string" minOccurs="1" maxOccurs="1" />
    </all>
    """
    node = etree.fromstring(xml)
    xsd_reader.root = node
    xsd_reader.namespaces = []
    state = State()
    properties = xsd_reader.process_all(node, state)

    assert len(properties) == 1
    assert properties[0].xsd_name == "requiredElement"
    assert properties[0].type.name == "string"
    assert properties[0].required is True
    assert properties[0].is_array is False


def test_process_union(xsd_reader):
    union_xml = """
    <xs:union xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:simpleType>
            <xs:restriction base="xs:double"/>
        </xs:simpleType>
        <xs:simpleType>
            <xs:restriction base="xs:long"/>
        </xs:simpleType>
    </xs:union>
    """
    union_node = etree.fromstring(union_xml)
    state = State()

    xsd_type = xsd_reader.process_union(union_node, state)

    assert xsd_type.name == "number"
