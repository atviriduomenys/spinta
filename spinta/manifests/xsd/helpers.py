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


def _get_description(element: etree.Element) -> str:
    annotation = element.xpath(f'./*[local-name() = "annotation"]')
    if annotation:
        documentation = annotation[0].xpath(f'./*[local-name() = "documentation"]')
        if documentation:
            return documentation[0].text
    return ""


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
    # todo handle backrefs
    ref = to_model_name(node.get("ref"))

    property_name = node.get("name")
    property_id = to_property_name(property_name)
    if not property_name:
        property_name = ref

    if ref:
        prop["model"] = ref
    else:
        prop["external"] = {"name": property_name}

    prop["type"] = _get_property_type(node)

    return property_id, prop


def _element_to_property(element: etree.Element) -> tuple[str, dict]:
    """
    Receives an element and returns a tuple containing the id of the property and the property itself
    Elements can be properties in those cases when they have only description (annotation/documentation)
    or when they don't have any elements inside of them, i.e. when it's simple type.
    """
    # todo if this element has a reference, we need to decide if we add the data from reference here,
    # or if we make it ref type and have it pointed to another model
    
    property_id, prop = _node_to_partial_property(element)
    if prop["type"] != "ref":
        prop["external"]["name"] = f'{prop["external"]["name"]}/text()'

    # if maxOccurs > 1, then it's a list. specific to elements.
    max_occurs = element.get("maxOccurs", 1)
    if max_occurs == "unbounded" or int(max_occurs) > 1:
        property_id += "[]"

    # specific to elements
    min_occurs = int(element.get("minOccurs", 1))
    if min_occurs > 0:
        prop["required"] = True
    else:
        prop["required"] = False

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
    # sequences of elements
    # todo handle choices too
    complex_type_node = element.xpath(f'./*[local-name() = "complexType"]')
    if len(complex_type_node) > 0:
        complex_type_node = complex_type_node[0]
        print("COMPLEX TYPE NODE:", complex_type_node)

        # complex_type can be "mixed" which means that there might be text between sequence elements
        if complex_type_node.get("mixed"):
            #
            text_property = {
                'text': {
                    'type': 'string',
                    'external': {
                        'name': 'text()',
                    }
                }}
            properties.update(text_property)
        if complex_type_node.xpath(f'./*[local-name() = "sequence"]'):
            sequence_node = complex_type_node.xpath(f'./*[local-name() = "sequence"]')[0]
            elements = sequence_node.xpath(f'./*[local-name() = "element"]')

            # if we already have properties, which means that this node consists not only of elements (but attributes or text)
            # then we treat it as final and create a model out of it
            # otherwise we can add it to the path as a partial model
            if properties or len(elements) > 1:
                for element in elements:
                    property_id, prop = _element_to_property(element)
                    properties[property_id] = prop

    return properties


def _parse_element(node: _Element, models: list, source_path: str = "/") -> dict:
    """
    Parses an element. If it is a complete model, it will be added to the models list.
    """

    source_path = f"{source_path}/{node.get('name')}"

    parsed_model = {
        "type": "model",
        "description": "",
        "name": to_model_name(node.get('name')),
        "external": _get_external_info(name=source_path),
    }

    # for element in node:
    print("ELEMENT:", node)
    # element.tag = etree.QName(element).localname
    parsed_model["description"] = _get_description(node)
    parsed_model["properties"] = _get_properties(node, source_path)

    # if we have either description or


    # todo handle choices

    # 1. There is only one element in the sequence. Then we just go deeper and add this model to the next model's path.
    if node.xpath(f'./*[local-name() = "complexType"]'):
        complex_type_node = node.xpath(f'./*[local-name() = "complexType"]')[0]
        print("COMPLEX TYPE NODE:", complex_type_node)
        if complex_type_node.xpath(f'./*[local-name() = "sequence"]'):
            sequence_node = complex_type_node.xpath(f'./*[local-name() = "sequence"]')[0]
            sequence_node_length = len(sequence_node)
            if sequence_node_length == 1 and not sequence_node.xpath(f'./*[local-name() = "element"]')[0].get("ref"):
                print("SEQUENCE NODE:", sequence_node)
                element = sequence_node.xpath(f'./*[local-name() = "element"]')[0]
                _parse_element(element, models, source_path=source_path)
            else:
                models.append(parsed_model)
        else:
            models.append(parsed_model)
    else:
        # final model, we don't go any deeper
        # todo handle cases where we need to go deeper if the complex type is described separately,
        # todo or where it's a reference to another element but doesn't have anything of it's own

        models.append(parsed_model)

    # if we have sequences, we need to grab each element from this sequence and

    # print(element.xpath("xs:documentation", namespaces={'xs': 'http://www.w3.org/2001/XMLSchema'})[0].text)

    # print(etree.tostring(element, encoding="utf8"))
    # return parsed_model


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

    #  Dataset and resource info
    dataset_and_resource_info = {
        'type': 'dataset',
        'name': "dataset1",
        'resources': {
            "resource1": {
                'type': 'xml',
            },
        },
        'given_name': dataset_name
    }

    if path.startswith("http"):
        document = etree.parse(urlopen(path))
        objectify.deannotate(document, cleanup_namespaces=True)
        root = document.getroot()
        print(type(root))
    else:
        with open(path) as file:
            text = file.read()
            root = etree.fromstring(text)

    custom_types = _extract_custom_types(root)

    # Resource model
    resource_external_info = _get_external_info(path, root, name="Resource")

    resource_model = {
        "type": "model",
        # todo what if data has the Resource model also?
        "name": "Resource",
        "description": "Įvairūs duomenys",
        "properties": {},
        "external": resource_external_info,

        # todo ask where uri needs to be, here or in "external"
        "uri": "http://www.w3.org/2000/01/rdf-schema#Resource",
    }
    models = list()
    models.append(dataset_and_resource_info)
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
              # ((node.xpath(f'./*[local-name() = "annotation"]') and
              #     node.xpath(f'./*[local-name() = "simpleType"]') and len(node) == 2))):
              (node.xpath(f'./*[local-name() = "simpleType"]'))):

            property_id, prop = _element_to_property(node)
            resource_model["properties"][property_id] = {
                "type": _get_property_type(node),
                "external": {"name": property_id},
                "description": _get_description(node)
            }

    if resource_model["properties"]:
        resource_model["external"] = resource_external_info
        models.append(resource_model)

    for parsed_model in models:
        # parsed_model["external"] = resource_external_info
        pprint(parsed_model)

        yield None, parsed_model
