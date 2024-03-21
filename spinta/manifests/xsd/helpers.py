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
    "string": "text",
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
    "short": "",
    "byte": "",
    "nonNegativeInteger": "",
    "unsignedLong": "",
    "unsignedInt": "",
    "unsignedShort": "",
    "unsignedByte": "",
    "positiveInteger": "",
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


def _get_description(element: etree.Element) -> str:
    annotation = element.xpath(f'./*[local-name() = "annotation"]')
    if annotation:
        documentation = annotation[0].xpath(f'./*[local-name() = "documentation"]')
        if documentation:
            return documentation[0].text
    return ""


def _get_property_type(node: etree.Element) -> str:
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
    property_name = to_property_name(node.get("name"))
    property_id = property_name
    prop["external"] = {"name": property_name}
    prop["type"] = _get_property_type(node)

    return property_id, prop


def _element_to_property(element: etree.Element) -> tuple[str, dict]:
    """
    Receives an element and returns a tuple containing the id of the property and the property itself
    Elements can be properties in those cases when they have only description (annotation/documentation)
    """
    property_id, prop = _node_to_partial_property(element)

    # if maxOccurs > 1, then it's a list. specific to elements.
    max_occurs = element.get("maxOccurs", 1)
    if max_occurs > 1:
        prop["external"]["name"] += "[]"

    # specific to elements
    min_occurs = element.get("minOccurs", 1)
    if min_occurs > 0:
        prop["required"] = True
    else:
        prop["required"] = False

    prop["source"] = f"{element.get("name", "").upper()}"

    return property_id, prop


def _attributes_to_properties(element: etree.Element) -> dict:
    properties = {}
    attributes = element.xpath(f'./*[local-name() = "attribute"]')
    for attribute in attributes:

        property_id, prop = _node_to_partial_property(attribute)

        # property source
        prop["source"] = f"@{prop["external"]["name"]}"

        # property required or not. For attributes only.
        use = attribute.get("use")
        if use == "required":
            required = True
        else:
            required = False
        prop["required"] = required
        properties[property_id] = prop

    return properties


def _get_properties(element: _Element, source_path: str) -> dict:
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
    properties = {}

    attributes = _attributes_to_properties(element)
    properties.update(attributes)
    # todo add sequences and choices

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
    return properties


def _parse_element(node: _Element, models: list, source_path: str = "") -> dict:
    """
    Parses an element. If it is a complete model, it will be added to the models list.
    """

    source_path = f"/{source_path}{node.get('name')}"

    parsed_model = {
        "type": "model",
        "description": "",
        "external": {
            "name": to_model_name(node.get("name")),
        },
        "source": source_path,
    }

    # for element in node:
    print("ELEMENT:", node)
    # element.tag = etree.QName(element).localname
    parsed_model["description"] = _get_description(node)
    parsed_model["properties"] = _get_properties(node, source_path)

    # if we have either description or


    # todo handle sequences
    # if we have sequences, we need to grab each element from this sequence and

    # print(element.xpath("xs:documentation", namespaces={'xs': 'http://www.w3.org/2001/XMLSchema'})[0].text)

    # print(etree.tostring(element, encoding="utf8"))
    return parsed_model


def _get_external_info(path: str, document: _ElementTree) -> dict:
    # todo finish this
    return {}


def _extract_custom_types(node: _ElementTree) -> dict:
    # todo finish this
    return {}


def read_schema(context: Context, path: str, prepare: str = None, dataset_name: str = '') -> dict:
    """
    This reads XSD schema from the url provided in path and yields asd schema models

    For now this is adjusted for XSD schemas of Registrų centras
    At the moment we assume that model is an element that might have at least one of those inside:
    <xs:annotation>
    <xs:complexType>
    There are those different cases:
    1. The element has only annotation. In this case we create a special Resource model and
       add this element as a property to that Resource model



    1. If this element has only complexType, we leave the description of the model empty
    2. If this element has, both annotation and complexType,
       then we assign annotation/documentation to description, and we parse complexType
       and assign the parsed results to properties of the model
    4. If this element has only sequence with references, and none of its references has
       minOccurs set to 1, then we don't create model out of it but go deeper into all refs
    5. If this element has only sequence with, and some of the references have minOccurs set to 1,
    then we create a reference which is like this: ELEMENT_NAME@attribute
    6. Choices are handled similar to sequences


    We will build a list of models, and another list, that has "used" elements,
    that have already been used in models either as part of the path or as part of the property.
    (another option was to make a generator for parsing models, and going deeper, but that would
    be more complex when returning models. although this option is also possible, but it can
    be reworked into this from an option with a list)

    If some model has only references in the sequence, we need to also add those as models,
    mark their type as backref, and set as private. In this case, we add them at the moment we meet them
    as refs, because this way we will know that they are backrefs.

    Element can be as a choice in more than one other element.

    """
    document = etree.parse(urlopen(path))
    print(type(document))

    objectify.deannotate(document, cleanup_namespaces=True)
    root = document.getroot()
    print(type(root))

    custom_types = _extract_custom_types(root)

    # Resource model
    resource_external_info = _get_external_info(path, document)
    resource_external_info["name"] = "Resource"

    resource_model = {
        "type": "model",
        "description": "Įvairūs duomenys",
        "properties": {},
        "external": resource_external_info,

        # todo ask where uri needs to be, here or in "external"
        "uri": "http://www.w3.org/2000/01/rdf-schema#Resource",
    }
    models = []

    for node in root:

        # first we need to check if this model has complexType.
        # If it has, we create a separate model.
        # If it doesn't have, we add a special model Resource and add this element as a property to it
        # model.tag = etree.QName(model).localname
        print(node.xpath("*"))
        print(node.attrib)
        print(node.tag)

        # todo complexContent is also an option.
        # todo there is also an option where complex type is on the same level as element, referenced by type
        if node.xpath(f'./*[local-name() = "complexType"]'):
            _parse_element(node, models)

        # if we only have annotation, this means that it's a text-only element with no attributes, so we
        # add it to the Resource model
        # Same if we have annotation and simpleType, only then we need to parse the simple type too
        elif ((node.xpath(f'./*[local-name() = "annotation"]') and len(node) == 1) or
              ((node.xpath(f'./*[local-name() = "annotation"]') and
                  node.xpath(f'./*[local-name() = "simpleType"]') and len(node) == 2))):

            # todo find out what this id really is
            property_id, prop = _element_to_property(node)
            resource_model["properties"][property_id] = {
                "type": _get_property_type(node),
                "external": {"name": property_id},
                "description": _get_description(node)
            }

    models.append(resource_model)

    for parsed_model in models:
        parsed_model["external"] = resource_external_info
        pprint(parsed_model)

        # yield model