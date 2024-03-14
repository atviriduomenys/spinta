import xmltodict
from lxml.etree import _ElementTree

from spinta.components import Context
from lxml import etree, objectify
from urllib.request import urlopen
from pprint import pprint


# mapping of XSD datatypes to DSA datatypes
# XSD datatypes: https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes
# DSA datatypes: https://atviriduomenys.readthedocs.io/dsa/duomenu-tipai.html#duomenu-tipai
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


def get_description(element: etree.Element) -> str:
    annotation = element.xpath(f'./*[local-name() = "annotation"]')
    if annotation:
        documentation = annotation[0].xpath(f'./*[local-name() = "documentation"]')
        if documentation:
            return documentation[0].text
    return ""


def attributes_to_properties(element: etree.Element):
    properties = {}
    attributes = element.xpath(f'./*[local-name() = "attribute"]')
    for attribute in attributes:

        # property id
        property_id = attribute.get("name").lower()
        properties[property_id] = {}

        # property type
        property_type = attribute.get("type", "")
        if not property_type:

            # this is a self defined simple type, so we take it's base as type
            restrictions = attribute.xpath(f'./*[local-name() = "simpleType"]/*[local-name() = "restriction"]')
            if restrictions:
                property_type = restrictions[0].get("base")
            else:
                property_type = ""

        # getting rid of the prefix
        if ":" in property_type:
            property_type = property_type.split(":")[1]

        property_type = DATATYPES_MAPPING[property_type]
        properties[property_id]["type"] = property_type

        # property required or not
        use = attribute.get("use")
        if use == "required":
            required = True
        else:
            required = False
        properties[property_id]["required"] = required
        properties[property_id]["description"] = get_description(attribute)

    return properties


def get_properties(element: _ElementTree) -> dict:
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

    attributes = attributes_to_properties(element)
    properties.update(attributes)

    return properties


def parse_model(model: _ElementTree) -> dict:
    """

    """
    parsed_model = {
        "type": "model",
        "description": "",
        "name": model.get("name").capitalize(),
        "title": model.get("name").capitalize(),
    }

    for element in model:
        # print(element.prefix)
        # element.tag = etree.QName(element).localname
        parsed_model["description"] = get_description(model)
        parsed_model["properties"] = get_properties(element)

        # print(element.xpath("xs:documentation", namespaces={'xs': 'http://www.w3.org/2001/XMLSchema'})[0].text)

        # print(etree.tostring(element, encoding="utf8"))
    pprint(parsed_model)
    return parsed_model


def get_external_info(path: str, document: _ElementTree) -> dict:
    # todo finish this
    pass



def read_schema(context: Context, path: str, prepare: str = None, dataset_name: str = '') -> dict:
    """
    This reads XSD schema from the url provided in path and yields asd schema models

    For now this is adjusted for XSD schemas of Registrų centras
    At the moment we assume that model is an element that might have at least one of those inside:
    <xs:annotation>
    <xs:complexType>
    There are 3 cases:
    1. If this element has only complexType, we leave the description of the model empty
    2. If this element has, both annotation and complexType,
       then we assign annotation/documentation to description, and we parse complexType
       and assign the parsed results to properties of the model
    3. If this element has only annotation, we create a special Resource model and
       add this element as property to that Resource model

    What to do with sequences?
    What to do with choices?

    """
    document = etree.parse(urlopen(path))
    print(type(document))

    # todo find out if we could make this work:
    # etree.cleanup_namespaces(document)
    objectify.deannotate(document, cleanup_namespaces=True)
    document = document.getroot()

    external_info = get_external_info(path, document)

    resource_model = {
        "type": "model",
        "description": "Įvairūs duomenys",
        "properties": [],
        "name": "Resource",
        "external": external_info,
    }

    for model in document:

        # first we need to check if this model has complexType.
        # If it has, we create a separate model.
        # If it doesn't have, we add a special model Resource and add this element as a property to it
        # model.tag = etree.QName(model).localname
        if model.xpath(f'./*[local-name() = "complexType"]'):
            parsed_model = parse_model(model)
            parsed_model["external"] = external_info
        else:
            # todo add logic for adding this as a property to the Resource model
            pass
        # yield model

