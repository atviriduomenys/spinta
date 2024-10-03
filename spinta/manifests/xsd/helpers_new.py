from __future__ import annotations

import logging
import os
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from urllib.request import urlopen

from lxml import etree, objectify
from lxml.etree import _Element, _Comment

from spinta.components import Context
from spinta.utils.naming import Deduplicator, to_dataset_name

xsd_example = """
    <xs:schema xmlns="http://eTaarPlat.ServiceContracts/2007/08/Messages" elementFormDefault="qualified" targetNamespace="http://eTaarPlat.ServiceContracts/2007/08/Messages" xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="getDocumentsByWagonResponse" nillable="true" type="getDocumentsByWagonResponse" />
        <xs:complexType name="getDocumentsByWagonResponse">
            <xs:sequence>
                <xs:element minOccurs="0" maxOccurs="1" name="searchParameters" type="getDocumentsByWagonSearchParams" />
                <xs:choice minOccurs="1" maxOccurs="1">
                    <xs:element minOccurs="0" maxOccurs="1" name="klaida" type="Klaida" />
                    <xs:element minOccurs="0" maxOccurs="1" name="extract" type="Extract" />
                </xs:choice>
            </xs:sequence>
        </xs:complexType>
        <xs:complexType name="getDocumentsByWagonSearchParams">
            <xs:sequence>
                <xs:element minOccurs="0" maxOccurs="1" name="searchType" type="xs:string" />
                <xs:element minOccurs="0" maxOccurs="1" name="code" type="xs:string" />
            </xs:sequence>
        </xs:complexType>
        <xs:complexType name="Klaida">
                    <xs:sequence>
                        <xs:element minOccurs="0" maxOccurs="1" name="Aprasymas" type="xs:string" />
                    </xs:sequence>
        </xs:complexType>
        <xs:complexType name="Extract">
                    <xs:sequence>
                        <xs:element minOccurs="1" maxOccurs="1" name="extractPreparationTime" type="xs:dateTime" />
                        <xs:element minOccurs="1" maxOccurs="1" name="lastUpdateTime" type="xs:dateTime" />
                    </xs:sequence>
        </xs:complexType>
    </xs:schema>
"""

DATATYPES_MAPPING = {
    "string": "string",
    "boolean": "boolean",
    "decimal": "number",
    "float": "number",
    "double": "number",

    # duration has to be mapped to integer. In addition to this, we need a prepare function.
    # This prepare function takes in given duration and turns it into integer (timedelta).
    # More about time entities here:
    # https://atviriduomenys.readthedocs.io/dsa/vienetai.html#laiko-vienetai
    # TODO: add prepare function for duration
    #  https://github.com/atviriduomenys/spinta/issues/594
    "duration": "string",

    "dateTime": "datetime",
    "time": "time",
    "date": "date",
    # format: target type, target column, value in the column
    "gYearMonth": "date;enum;M",
    "gYear": "date;enum;Y",
    "gMonthDay": "string",
    "gDay": "string",
    "gMonth": "string",
    "hexBinary": "string",
    "base64Binary": "binary;prepare;base64",
    "anyURI": "uri",
    "QName": "string",
    "NOTATION": "string",
    "normalizedString": "string",
    "token": "string",
    "language": "string",
    "NMTOKEN": "string",
    "NMTOKENS": "string",
    "Name": "string",
    "NCName": "string",
    "ID": "string",
    "IDREF": "string",
    "IDREFS": "string",
    "ENTITY": "string",
    "ENTITIES": "string",
    "integer": "integer",
    "nonPositiveInteger": "integer",
    "negativeInteger": "integer",
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
    "yearMonthDuration": "integer",
    "dayTimeDuration": "integer",
    "dateTimeStamp": "datetime",
    "": "string",
}

NODE_TYPES = [
    "schema",
    "element",
    "complexType",
    "simpleType",
    "complexContent",
    "simpleContent",
    "attribute",
    "sequence",
    "group",
    "all",
    "choice",
    "annotation",
    "documentation",
    "extension",
    "restriction",
    "enumeration",
    "union",  # pavyzdys NTR95
    "length",  # pavyzdys NTR95
    "pattern",
    "maxLength",
    "minLength",
    "whiteSpace",
    "totalDigits",
    "fractionDigits",
    "minInclusive",
    "maxInclusive",
    "appinfo",
    "Relationship",  # custom RC element
]

NODE_ATTRIBUTE_TYPES = ["minOccurs", "maxOccurs", "type", "use", "name", "ref"]
IGNORED_NODE_ATTRIBUTE_TYPES = ["maxLength", "minLength"]


class XSDProperty:
    id: str
    id_raw: str  # id before deduplication on the model
    source: str  # converts to ["external"]["name"]
    type: str
    enum: str
    enums: dict[str, str]
    prepare: str
    required: bool
    is_array: bool
    model: XSDModel
    description: str | None = None
    uri: str | None = None
    xsd_ref_to: str | None = None  # if it is a ref to another model, here is it's name
    xsd_type_to: str | None = None  # if it's type is of another complexType, so ref to that type

    def get_data(self):
        pass

class XSDModel:
    dataset_resource: XSDDatasetResource
    name: str | None = None
    basename: str | None = None
    source: str  # converts to ["external"]["name"]
    properties: dict[str, XSDProperty]
    uri: str | None = None
    description: str | None = None
    referred_from: list[tuple[XSDModel, str]] | None = None  # tuple - model, property id
    is_root_model: bool | None = None
    deduplicate_property_name: Deduplicator
    xsd_type: str  # from complexType or from element

    def __init__(self) -> None:
        self.properties = {}

    def set_name(self, name: str):
        self.basename = name
        self.name = f"{self.dataset_resource.dataset_name}/{name}"

    def get_data(self):
        pass

@dataclass
class XSDDatasetResource:
    dataset_name: str | None = None
    resource_name: str | None = None
    dataset_given_name: str | None = None

    def get_data(self):
        return {
            'type': 'dataset',
            'name': self.dataset_name,
            'resources': {
                "resource1": {
                    'type': 'xml',
                },
            },
            'given_name': self.dataset_given_name
        }


class XMLNode:
    parent: XMLNode | None = None
    children: dict[str, XMLNode] | None = None
    type: str | None = None
    text: str | None = None
    is_top_level: bool | None = None
    attributes: dict[str, str] | None = None

    def __init__(self):
        self.attributes = {}
        self.children = {}

    def read_attributes(self, node: _Element):
        attributes = node.attrib
        for name, value in attributes.items():
            if name in IGNORED_NODE_ATTRIBUTE_TYPES:
                continue
            elif name in NODE_ATTRIBUTE_TYPES:
                self.attributes[name] = value
            else:
                raise RuntimeError(f'unknown attribute: {name}')

class XMLTree:
    nodes = list[XMLNode]

    def __init__(self):
        self.nodes = []


class XSDReader:
    dataset_resource: XSDDatasetResource
    models: dict[str, XSDModel]
    _path: str
    root: _Element
    deduplicate_model_name: Deduplicator
    xml_tree: XMLTree
    custom_types: dict[str, str] | None = None

    def __init__(self, path: str, dataset_name) -> None:
        self._path = path
        self.dataset_resource = XSDDatasetResource(dataset_given_name=dataset_name, resource_name="resource1")
        self.xml_tree = XMLTree()
        self.custom_types = {}

    def read_root(self, node, state):
        state.is_top_level = True
        for child_node in node.getchildren():
            if not isinstance(child_node, _Comment):
                self.read_node(child_node, state)

    def read_node(self, node: _Element, state: State, parent: XMLNode = None) -> None:
        for child_node in node.getchildren():
            if isinstance(child_node, _Comment):
                continue
            xml_node = XMLNode()
            xml_node.type = etree.QName(child_node).localname
            xml_node.text = child_node.text
            if xml_node.type not in NODE_TYPES:
                raise RuntimeError(f"Unsupported node type: {xml_node.type}")
            #      TODO: maybe create new error type
            xml_node.read_attributes(child_node)
            if state.is_top_level:
                xml_node.is_top_level = True
            else:
                xml_node.is_top_level = False
                xml_node.parent = parent
            state.is_top_level = False
            if parent:
                parent.children[xml_node.type] = xml_node
            self.xml_tree.nodes.append(xml_node)
            self.read_node(child_node, state, parent=xml_node)

    def register_simple_types(self, state: State) -> None:
        for node in self.xml_tree.nodes:
            if node.is_top_level:
                if node.type is "simpleType":
                    name, custom_type = self.process_simple_type(node, state)
                    self.custom_types[name] = custom_type


    def start(self):
        # general part
        state = State()
        self._extract_root()
        dataset_name = to_dataset_name(os.path.splitext(os.path.basename(self._path))[0])
        self.dataset_resource.dataset_name = dataset_name

        # registering custom simpleType



        # reading XML and registering nodes

        self.read_root(self.root, state)

        self._create_resource_model()

        # reading XSD and registering models and properties

        state = State()

        self.register_simple_types(state)

        # TODO: two options: -
        #  1. we register all top level things first and then use when we need them
        #  2. we register every model (from element or separate complexType) when we meet them, but check if we already have processed them before (by XSD name)
        #  I like the second option better. I think it will be more

        self.process_top_level()

        self.process_xml_tree(state)

        # post processing

        # we need to add this here, because only now we will know if it has properties and if we need to create it
        self._post_process_resource_model()

    def process_xml_tree(self, state):
        for node in self.xml_tree.nodes:
            if node.is_top_level:
                self.process_node(node, state)

    def process_node(self, node: XMLNode, state: State):

        if node.type == "element":
            return self.process_element(node, state)
        if node.type == "complexType":
            return self.process_complex_type(node, state)
        if node.type == "simpleType":
            pass
            # return self.process_simple_type(node, state)

        # if node.type == "complexContent":
        #     return self.process_complex_content(node, state)
        # if node.type == "simpleContent":
        #     return self.process_simple_content(node, state)
        # if node.type == "attribute":
        #     return self.process_attribute(node, state)
        # if node.type == "sequence":
        #     return self.process_sequence(node, state)
        # if node.type == "group":
        #     return self.process_group(node, state)
        # if node.type == "all":
        #     return self.process_all(node, state)
        # if node.type == "choice":
        #     return self.process_choice(node, state)
        # if node.type == "annotation":
        #     return self.process_annotation(node, state)
        # if node.type == "documentation":
        #     return self.process_documentation(node, state)
        # if node.type == "extension":
        #     return self.process_extension(node, state)
        # if node.type == "restriction":
        #     return self.process_restriction(node, state)
        # if node.type == "enumeration":
        #     return self.process_enumeration(node, state)
        # if node.type == "union":
        #     return self.process_union(node, state)
        # if node.type == "length":
        #     return self.process_length(node, state)
        # if node.type == "pattern":
        #     return self.process_pattern(node, state)
        # if node.type == "maxLength":
        #     return self.process_max_length(node, state)
        # if node.type == "minLength":
        #     return self.process_min_length(node, state)
        # if node.type == "whiteSpace":
        #     return self.process_white_space(node, state)
        # if node.type == "totalDigits":
        #     return self.process_total_digits(node, state)
        # if node.type == "fractionDigits":
        #     return self.process_fraction_digits(node, state)
        # if node.type == "minInclusive":
        #     return self.process_min_inclusive(node, state)
        # if node.type == "appinfo":
        #     return self.process_appinfo(node, state)

    def _extract_root(self):
        if self._path.startswith("http"):
            document = etree.parse(urlopen(self._path))
            objectify.deannotate(document, cleanup_namespaces=True)
            self.root = document.getroot()
        else:
            with open(self._path) as file:
                text = file.read()
                self.root = etree.fromstring(bytes(text, encoding='utf-8'))

    def _create_resource_model(self):
        self.resource_model = XSDModel()
        self.resource_model.type = "model"
        self.resource_model.description = "Įvairūs duomenys"
        self.resource_model.uri = "http://www.w3.org/2000/01/rdf-schema#Resource"
    #     resource model will be added to models at the end, if it has any peoperties

    def _post_process_resource_model(self):
        if self.resource_model.properties:
            self.resource_model.set_name(self.deduplicate_model_name(f"Resource"))
            self.models[self.resource_model.name] = self.resource_model

    #  XSD nodes processors

    def process_element(self, node: XMLNode, state: State) -> None:
        if node.is_top_level:
            for child in node.children:
                result = self.process_node(child, state)

    def process_complex_type(self, node: XMLNode, state: State) -> None:

        if state.process_only_top_level:
            if node.is_top_level:
                for child in node.children:
                    if child.type == "sequence":
                        self.process_sequence(child, state)
            else:
                pass

        else:
            if node.is_top_level:
                pass
            else:
                for child in node.children:
                    if child.type == "sequence":
                        self.process_sequence(child, state)



    def process_attribute(self, node: XMLNode, state: State) -> None:
        prop = XSDProperty()
        prop.source = f"@{node.attributes.get('name')}"
        prop.id_raw = node.attributes.get('name')

        attribute_type = node.attributes.get("type")

        if attribute_type:
            if attribute_type in self.custom_types:
                prop.type = self.custom_types[attribute_type]
            elif attribute_type in DATATYPES_MAPPING:
                property_type = DATATYPES_MAPPING[attribute_type]
                if ";" in property_type:
                    property_type, target, value = property_type.split(";")
                    prop.type = property_type
                    if target == "enum":
                        prop.enum = value
                    if target == "prepare":
                        prop.prepare = value

        prop.type = node.attributes.get("type")
    #     transfer logic from get_property_type

    #     TODO: we need to register at least simple types first
    #      maybe do separate rounds:
    #      1. register simple types
    #      2. create models only from root elements and complexTypes
    #      3. Do everything else

    #     transfer logic from attributes_to_properties

    #     do things with attribute

        for child in node.children:
            if child.type == "simpleType":
                result = self.process_simple_type(child, state)
                prop.type = result.type
                if result.enums:
                    prop.enums = result.enum
            # TODO: think sof a better way to deal with enums
            elif child.type == "annotation":
                result = self.process_annotation(child, state)
                prop.description = result
            else:
                raise RuntimeError(f"Unexpected element type inside attribute element: {child.type}")

        return prop

    def process_enumeration(self, node: XMLNode, state: State) -> None:
        pass

    def process_simple_type(self, node: XMLNode, state: State) -> tuple[str | None, str]:
        """
        Returns the name of the type if it has one (separately declared simple types)
        and they DSA type to which it corresponds.
        also returns enums if finds.

        return result
        """
        for child in node.children:
            if child.type == "restriction":
                self.process_restriction(child, state)
            else:
                raise RuntimeError(f"Unexpected element type inside simpleType element: {child.type}")
        pass

    def process_sequence(self, node: XMLNode, state: State) -> None:
        pass

    def process_choice(self, node: XMLNode, state: State) -> None:
        pass

    def process_group(self, node: XMLNode, state: State) -> None:
        pass

    def process_all(self, node: XMLNode, state: State) -> None:
        pass

    def process_simple_content(self, node: XMLNode, state: State) -> None:
        pass

    def process_complex_content(self, node: XMLNode, state: State) -> None:
        pass

    def process_annotation(self, node: XMLNode, state: State) -> str:
        description = ""
        for child in node.children:
            if child.type == "documentation":
                description += self.process_documentation(node, state)
            else:
                raise RuntimeError(f"Unexpected element type inside annotation element: {child.type}")
        return description

    def process_documentation(self, node: XMLNode, state: State) -> str:
        return node.text

    def process_extension(self, node: XMLNode, state: State) -> None:
        pass

    def process_restriction(self, node: XMLNode, state: State) -> None:
        for child in node.children:
            if child.type == "enumeration":
                self.process_enumeration(child, state)
            else:
                raise RuntimeError(f"Unexpected element type inside restriction element: {child.type}")
        pass

    def process_union(self, node: XMLNode, state: State) -> None:
        pass

    def process_length(self, node: XMLNode, state: State) -> None:
        pass

    def process_pattern(self, node: XMLNode, state: State) -> None:
        pass

    def process_max_length(self, node: XMLNode, state: State) -> None:
        pass

    def process_min_length(self, node: XMLNode, state: State) -> None:
        pass

    def process_white_space(self, node: XMLNode, state: State) -> None:
        pass

    def process_total_digits(self, node: XMLNode, state: State) -> None:
        raise Exception("Unsupported element")
        # TODO: create specific error

    def process_fraction_digits(self, node: XMLNode, state: State) -> None:
        logging.log(logging.INFO, "met an unsupported type fractionDigits")
        # TODO: configure logger

    def process_min_inclusive(self, node: XMLNode, state: State) -> None:
        pass

    def process_max_inclusive(self, node: XMLNode, state: State) -> None:
        pass

    def process_appinfo(self, node: XMLNode, state: State) -> None:
        pass

class State:
    path = list[str]
    is_top_level: bool
    after_processing_children: bool
    XSD_model_has_properties: bool

#
#
# state = ElementState()
#
# # pirmas praėjimas - XML ir XSD logika
# def process_element(state: State, node: _Element):
#     name = ""
#     state.path.append(node.g)
#     return {"name": "getDocumentsByWagonResponse", "type": "model", "xsd":{"nillable": True, "node_type": "element", "type": "getDocumentsByWagonSearchParams"}}
#
# def process_complex_type(state: State, node: _Element):
#     state.complex_types[node.get("name")] = {}  # sudedam info
#     # jei turi pavadinim1 - dedam pavadinima, jei ne - kitas atvejis (vidinis complexType)
#     return {"xsd":{"name"}}
#
# def process_sequence(state: State, node: _Element):
#     pass
#
#
#
# # antras praėjimas - DSA logika
# def process_element_2(state: State, node: _Element):
#     xsd_type = node.get("type")
#     type = state.complex_types[xsd_type]
#     # pagal simple type
#
#
#
#     return {"type": "property", "name": node.get("name"), "dtype": "string"}
#
#     if state._is_root:
#
#
#     return {"type": "model", "name": node.get("name")}
#
#
#
#     return {"type": "property", "name": node.get("name"), "dtype": "ref"}
#


def read_schema(
        context: Context,
        path: str,
        prepare: str = None,
        dataset_name: str = ''
) -> dict[Any, dict[str, str | dict[str, str | bool | dict[str, str | dict[str, Any]]]]]:

    xsd = XSDReader(path, dataset_name)

    xsd.start()

    yield None, xsd.dataset_resource.get_data()

    # for model_name, parsed_model in xsd.models.items():
    #     yield None, parsed_model.get_data()
