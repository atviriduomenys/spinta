from lxml.etree import _ElementTree, _Element

from spinta.components import Context
from lxml import etree, objectify
from urllib.request import urlopen
from pprint import pprint

from spinta.utils.naming import to_property_name, to_model_name

# mapping of XSD datatypes to DSA datatypes
# XSD datatypes: https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes
# DSA datatypes: https://atviriduomenys.readthedocs.io/dsa/duomenu-tipai.html#duomenu-tipai
# todo finish mapping and make sure all things are mapped correctly
DATATYPES_MAPPING = {
    "string": "string",
    "boolean": "boolean",
    "decimal": "number",
    "float": "number",
    "double": "number",

    # Duration reikia mapinti su number arba integer, greičiausiai su integer ir XML duration
    # reikšmė konvertuoti į integer reikšmę nurodant prepare funkciją, kuri konveruoti duration
    # į integer, papildomai property.ref stulpelyje reikia nurodyti laiko vienetus:
    # https://atviriduomenys.readthedocs.io/dsa/vienetai.html#laiko-vienetai
    # todo add prepare functions
    "duration": "",

    "dateTime": "datetime",
    "time": "time",
    "date": "date",
    "gYearMonth": "",
    "gYear": "",
    "gMonthDay": "",
    "gDay": "",
    "gMonth": "",
    "hexBinary": "",
    "base64Binary": "",
    "anyURI": "uri",
    "QName": "",
    "NOTATION": "",
    "normalizedString": "string",
    "token": "string",
    "language": "string",
    "NMTOKEN": "string",
    "NMTOKENS": "",
    "Name": "",
    "NCName": "",
    "ID": "",
    "IDREF": "",
    "IDREFS": "",
    "ENTITY": "",
    "ENTITIES": "",
    "integer": "integer",
    "nonPositiveInteger": "",
    "negativeInteger": "",
    "long": "integer",
    "int": "integer",
    "short": "integer",
    "byte": "integer",
    "nonNegativeInteger": "integer",
    "unsignedLong": "integer",
    "unsignedInt": "integer",
    "unsignedShort": "integer",
    "unsignedByte": "integer",
    "positiveInteger": "integer",
    "yearMonthDuration": "",
    "dayTimeDuration": "",
    "dateTimeStamp": "",
    "": "",

}

"""
format of custom types:
{
    "type_name": {
        "base": "type_base"
    }
}
"""
custom_types = {}


def _get_external_info(path: str = None, document: _ElementTree = None, **kwargs) -> dict:
    # todo finish this
    external = {
        "dataset": "dataset1",
        "resource": "resource1"
    }
    external.update(kwargs)
    return external


def _extract_custom_types(node: _ElementTree) -> dict:
    # todo finish this
    return {}


def _get_text_property():
    return {
        'text': {
            'type': 'string',
            'external': {
                'name': 'text()',
            }
        }}


def _get_document_root(path):
    if path.startswith("http"):
        document = etree.parse(urlopen(path))
        objectify.deannotate(document, cleanup_namespaces=True)
        root = document.getroot()
        print(type(root))
    else:
        with open(path) as file:
            text = file.read()
            root = etree.fromstring(text)
    return root


def _get_dataset_and_resource_info(given_name):
    return {
        # todo actual dataset name (from file name/path file name)
        'type': 'dataset',
        'name': "dataset1",
        'resources': {
            "resource1": {
                'type': 'xml',
            },
        },
        'given_name': given_name
    }


def _get_description(element: etree.Element) -> str:
    annotation = element.xpath(f'./*[local-name() = "annotation"]')
    if annotation:
        documentation = annotation[0].xpath(f'./*[local-name() = "documentation"]')
        if documentation:
            return documentation[0].text
    return ""


def _node_is_simple_type_or_inline(node):
    return bool(
        (node.xpath(f'./*[local-name() = "annotation"]') and len(node.getchildren()) == 1) or
        (node.xpath(f'./*[local-name() = "simpleType"]')) or
        (len(node.getchildren()) == 0)
    )

def _node_is_ref(node):
    if node.get("ref"):
        return True
    return False


def _properties_from_simple_elements(node):
    properties = {}
    elements = node.xpath(f'./*[local-name() = "element"]')
    for element in elements:
        if _node_is_simple_type_or_inline(element) and not _node_is_ref(element):
            property_id, prop = _simple_element_to_property(element)
            properties[property_id] = prop
    return properties

def _get_property_type(node: etree.Element) -> str:
    if node.get("ref"):
        # todo decide whether this is a ref or a backref
        return "ref"
    property_type = node.get("type")
    if not property_type:
        # this is a self defined simple type, so we take it's base as type
        restrictions = node.xpath(f'./*[local-name() = "simpleType"]/*[local-name() = "restriction"]')
        if restrictions:
            property_type = restrictions[0].get("base", "")
        else:
            property_type = ""
    # getting rid of the prefix
    if ":" in property_type:
        property_type = property_type.split(":")[1]

    if property_type in DATATYPES_MAPPING:
        property_type = DATATYPES_MAPPING[property_type]
    elif property_type in custom_types:
        # todo handle other custom types. like data_ir_laikas to datetime
        property_type = custom_types.get(property_type).get("base", "")
    else:
        property_type = ""

    return property_type


def _node_to_partial_property(node: etree.Element) -> tuple[str, dict]:
    """Node can be either element or attribute.
    This function only processes things common to attributes and elements"""
    prop = dict()

    prop["description"] = _get_description(node) 
    property_name = node.get("name")
    prop["external"] = {"name": property_name}
    property_id = to_property_name(property_name)
    prop["type"] = _get_property_type(node)

    return property_id, prop


def _attributes_to_properties(element: etree.Element) -> dict:
    properties = {}
    attributes = element.xpath(f'./*[local-name() = "attribute"]')
    complex_type = element.xpath(f'./*[local-name() = "complexType"]')
    if complex_type:
        properties.update(_attributes_to_properties(complex_type[0]))
    for attribute in attributes:

        property_id, prop = _node_to_partial_property(attribute)

        # property source
        prop["external"]["name"] = f'@{prop["external"]["name"]}'

        # property required or not. For attributes only.
        use = attribute.get("use")
        if use == "required":
            required = True
        else:
            required = False
        prop["required"] = required
        properties[property_id] = prop

    # todo attribute can be a ref to an externally defined attribute also

    return properties


def _is_array(element: _Element) -> bool:
    max_occurs = element.get("maxOccurs", 1)
    return max_occurs == "unbounded" or int(max_occurs) > 1


def _is_required(element: _Element) -> bool:
    min_occurs = int(element.get("minOccurs", 1))
    if min_occurs > 0:
        return True
    return False


def _simple_element_to_property(element: _Element):
    property_id, prop = _node_to_partial_property(element)
    prop["external"]["name"] = f'{prop["external"]["name"]}/text()'
    if _is_array(element):
        property_id += "[]"
    if _is_required(element):
        prop["required"] = True
    else:
        prop["required"] = False
    return property_id, prop


def _element_to_property(element: _Element, root: _Element = None) -> tuple[str, dict]:
    """
    Receives an element and returns a tuple containing the id of the property and the property itself
    Elements can be properties in those cases when they have only description (annotation/documentation)
    or when they don't have any elements inside of them, i.e. when it's simple type.
    """
    # todo if this element has a reference, we need to decide if we add the data from reference here,
    # or if we make it ref type and have it pointed to another model
    # todo handle backrefs
    ref = to_model_name(element.get("ref"))
    property_id, prop = _node_to_partial_property(element)

    if _is_array(element):
        property_id += "[]"

    if not prop["external"]["name"]:
        prop["external"]["name"] = ref

    if ref:
        prop["model"] = ref
        prop["external"]["name"] = f'{prop["external"]["name"]}/text()'

    if _is_required(element):
        prop["required"] = True
    else:
        prop["required"] = False

    if ref:

        # if it's a reference, we first check if the referenced element can be a property
        # it can only be a property if maxOccurs isn't more than 1
        # https://atviriduomenys.readthedocs.io/dsa/duomenu-tipai.html#array
        # https://atviriduomenys.readthedocs.io/dsa/duomenu-tipai.html#backref
        max_occurs = element.get("maxOccurs", 1)
        if max_occurs == 1:
            referenced_element = root.xpath(f'/*[local-name() = "element"]')
            if ((referenced_element.xpath(f'./*[local-name() = "annotation"]') and len(referenced_element) == 1) or
                (referenced_element.xpath(f'./*[local-name() = "simpleType"]'))):
                property_id, prop = _element_to_property(referenced_element)

    return property_id, prop





def _complex_node_with_sequence_to_properties(complex_type_node: _Element, source_path: str, root: _Element = None) -> dict:
    """
    XSD attributes will get turned into properties

    Example:

        'properties': {
            'id': {
                'type': 'integer',
                'type_args': None,
                'required': True,
                'unique': True,
                'external': {
                    'name': 'ID',
                    'prepare': NA,
                }
            },
         },


    """

    """
    source: https://stackoverflow.com/questions/36286056/the-difference-between-all-sequence-choice-and-group-in-xsd
    When to use xsd:all, xsd:sequence, xsd:choice, or xsd:group:

    Use xsd:all when all child elements must be present, independent of order.
    Use xsd:sequence when child elements must be present per their occurrence constraints and order does matters.
    Use xsd:choice when one of the child element must be present.
    Use xsd:group as a way to wrap any of the above in order to name and reuse in multiple locations within an XSD.
    Note that occurrence constraints can appear on xsd:all, xsd:sequence, or xsd:choice in addition to the 
    child elements to achieve various cardinality effects.

    For example, if minOccurs="0" were added to xsd:element children of xsd:all, element order would be insignificant,
    but not all child elements would have to be present:
    """
    properties = {}
    complex_type_node = complex_type_node[0]
    print("COMPLEX TYPE NODE:", complex_type_node)

    # complex_type can be "mixed" which means that there might be text between sequence elements
    if complex_type_node.get("mixed"):
        properties.update(_get_text_property())
    if complex_type_node.xpath(f'./*[local-name() = "sequence"]'):
        sequence_node = complex_type_node.xpath(f'./*[local-name() = "sequence"]')[0]
        elements = sequence_node.xpath(f'./*[local-name() = "element"]')

        # if we already have properties, which means that this node consists not only of elements (but attributes or text)
        # then we treat it as final and create a model out of it
        # otherwise we can add it to the path as a part of path to a  model
        if properties or len(elements) > 1:
            # todo finish this
            pass

    return properties


def _parse_element(node: _Element, models: list, source_path: str = "", root: _Element = None):
    """
    Parses an element. If it is a complete model, it will be added to the models list.
    """
    final = False

    model = {
        "type": "model",
        "description": "",
        "name": to_model_name(node.get('name')),
        "external": _get_external_info(name=source_path),
    }

    model["name"] = f'{model["external"]["dataset"]}/{model["name"]}'

    source_path = f"{source_path}/{node.get('name')}"

    print("ELEMENT:", node)
    model["description"] = _get_description(node)
    model["properties"] = _attributes_to_properties(node)
    model["properties"].update(_properties_from_simple_elements(node))

    if model["properties"]:
        final = True

    if node.xpath(f'./*[local-name() = "complexType"]'):

        complex_type_node = node.xpath(f'./*[local-name() = "complexType"]')[0]
        if complex_type_node.xpath(f'./*[local-name() = "sequence"]'):
            complex_type_with_sequence_node = complex_type_node.xpath(f'./*[local-name() = "sequence"]')[0]
            complex_type_with_sequence_node_length = len(complex_type_with_sequence_node)

            # There is only one element in the complex node sequence, and it doesn't have annotation.
            # Then we just go deeper and add this model to the next model's path.
            if complex_type_with_sequence_node_length == 1 and not complex_type_with_sequence_node.xpath(f'./*[local-name() = "element"]')[0].get("ref"):
                print("SEQUENCE NODE:", complex_type_with_sequence_node)
                element = complex_type_with_sequence_node.xpath(f'./*[local-name() = "element"]')[0]
                _parse_element(element, models, source_path=source_path, root=root)
            #
            elif complex_type_with_sequence_node_length > 1:
                properties = _complex_node_with_sequence_to_properties(complex_type_node, source_path)
                model["properties"].update(properties)
                models.append(model)
            else:
                models.append(model)
        else:
            models.append(model)
    else:
        # final model, we don't go any deeper
        # todo handle cases where we need to go deeper if the complex type is described separately,

        models.append(model)

    # if we have sequences, we need to grab each element from this sequence and

    # print(element.xpath("xs:documentation", namespaces={'xs': 'http://www.w3.org/2001/XMLSchema'})[0].text)

    # print(etree.tostring(element, encoding="utf8"))


def _parse_root_node(root_node: _Element, models: list, path):
    # get properties from elements

    # Resource model - special case
    resource_model_external_info = _get_external_info(path, name="Resource")

    resource_model = {
        "type": "model",
        # todo what if data has the model called Resource model also?
        "name": "Resource",
        "description": "Įvairūs duomenys",
        "properties": {},
        "external": resource_model_external_info,
        "uri": "http://www.w3.org/2000/01/rdf-schema#Resource",
    }

    resource_model_properties = _properties_from_simple_elements(root_node)

    if resource_model["properties"]:
        models.append(resource_model)

    for node in root_node:
        if not _node_is_simple_type_or_inline(node):
            _parse_element(node, models, source_path="/")

        # todo complexContent is also an option.
        # todo there is also an option where complex type is on the same level as element, referenced by type


def read_schema(context: Context, path: str, prepare: str = None, dataset_name: str = '') -> dict:
    """
    This reads XSD schema from the url provided in path and yields asd schema models

    For now this is adjusted for XSD schemas of Registrų centras


    Elements can be:
    1. Simple type inline
    2. Simple type
    3. Complex type

    Elements can have annotations, which we add as description to either a model or a property.



    1. Simple type.
        a) not a reference. It's a property
        b) a reference
            i. maxOccurs
    2. Inline type. If element is a simple type or inline, it's a property.
        If it's a root element, it's then added as a property to a special Resource model.

        Simple type can have annotation too.
        Inline type can be a reference too.

    3. Complex type
        If element is complex type, it can be either a property or a model,
        or a part of a source path for a property or model, depending on other factors

            a) element has attributes. Then it's a model and attributes are properties.
            b) element has a sequence
                i. There is more than one element in the sequence. Then it's a model/end part of the model.
                   We treat child elements in the same way as we do root elements (recursion) and build a path
                ii. There is only one element in the sequence. It can then be a property, a model or an intermediate
                    in the path of a model.
                    Options:
                        A. That element isn't a ref.
                            We treat child elements in the same way as we do root elements (recursion) and build a path
                        B. That element is a ref.
                            B1. If maxOccurs > 1, this is a model, and that other element is also a model
                            B2. If maxOccurs = 1
                                B21. If both have annotation, then it's a model
                                B22. If only one of them has annotation, it's a part of a path
                            todo finish defining behaviours of different options for sequences

            c) element has a choice. todo define behaviour here



    We will build a list of models
    (another option was to make a generator for parsing models, and going deeper, but that would
    be more complex when returning models. although this option is also possible, but it can
    be reworked into this from an option with a list)

    If some model has references in the sequence, we need to also add those as models,
    mark their type as backref. In this case, we add them at the moment we meet them
    as xsd refs, because this way we will know that they are backrefs. We also need to add refs on
    those models to the current model.

    Element can be as a ref in more than one other element.

    Other things to handle: Resource model, custom types, enumerations

    """

    #  Dataset and resource info
    # todo add logic to get the data from the document/url

    dataset_and_resource_info = _get_dataset_and_resource_info(dataset_name)

    root = _get_document_root(path)

    custom_types = _extract_custom_types(root)

    models = list()
    models.append(dataset_and_resource_info)

    _parse_root_node(root, models, path)

    for parsed_model in models:
        pprint(parsed_model)

        yield None, parsed_model
