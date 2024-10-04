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
from spinta.core.ufuncs import Expr
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

NODE_ATTRIBUTE_TYPES = ["minOccurs", "maxOccurs", "type", "use", "name", "ref", "base", "value"]
IGNORED_NODE_ATTRIBUTE_TYPES = ["maxLength", "minLength"]

class XSDType:
    name: str | None = None
    type: str
    enum: str | None = None
    enums: dict[str, dict[str, dict[str, str]]] | None = None
    prepare: Expr | None = None


"""
Example of a property:

    'properties': {
        'id': {
            'type': 'integer',
            'type_args': None,
            'required': True,
            'unique': True,
            'enums': {
                "" {
                    "1": {
                        "source": "1"
                    }
                }
            }
            'external': {
                'name': 'ID',
                'prepare': NA,
            }
        },
     },
"""
class XSDProperty:
    id: str
    id_raw: str  # id before deduplication on the model
    source: str  # converts to ["external"]["name"]
    type: XSDType
    required: bool | None = None
    unique: bool |None = None
    is_array: bool
    ref_model: XSDModel
    description: str | None = None
    uri: str | None = None
    xsd_ref_to: str | None = None  # if it is a ref to another model, here is it's name
    xsd_type_to: str | None = None  # if it's type is of another complexType, so ref to that type

    def __init__(self):
        self.required = False
        self.enums = {}

    def get_data(self) -> dict[str, Any]:
        data = {
            "type": self.type.type,
            "external":
                {
                    "name": self.source,
                }
        }

        if self.required is not None:
            data["required"] = self.required
        if self.unique is not None:
            data["unique"] = True

        if self.type.prepare is not None:
            data["external"]["prepare"] = self.type.prepare

        if self.type.enums is not None:
                data["enums"] = self.type.enums,

        return data

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
        model_data: dict = {
            "type": "model",
            "name": self.name
        }
        if self.description is not None:
            model_data["description"] = self.description
        if self.properties is not None:
            properties = {}
            for prop_id, prop in self.properties.items():
                properties[prop_id] = prop.get_data()
            model_data["properties"] = self.properties
        if self.source is not None:
            model_data["external"] = {"name": self.source}
        if self.uri is not None:
            model_data["uri"] = self.uri

        return model_data
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
    children: list[XMLNode] | None = None
    type: str | None = None
    text: str | None = None
    is_top_level: bool | None = None
    attributes: dict[str, str] | None = None

    def __init__(self):
        self.attributes = {}
        self.children = []

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
    custom_types: dict[str, XSDType] | None = None
    top_level_element_models: dict[str, str]
    top_level_complex_type_models: dict[str, str]

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
                parent.children.append(xml_node)
            self.xml_tree.nodes.append(xml_node)
            self.read_node(child_node, state, parent=xml_node)

    def register_simple_types(self, state: State) -> None:
        for node in self.xml_tree.nodes:
            if node.is_top_level:
                if node.type is "simpleType":
                    custom_type = self.process_simple_type(node, state)
                    self.custom_types[custom_type.name] = custom_type

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

    def _post_process_refs(self):
        pass

    def start(self):
        # general part
        state = State()
        self._extract_root()
        dataset_name = to_dataset_name(os.path.splitext(os.path.basename(self._path))[0])
        self.dataset_resource.dataset_name = dataset_name

        # registering custom simpleType

        self.register_simple_types(state)

        # reading XML and registering nodes

        self.read_root(self.root, state)

        self._create_resource_model()

        # reading XSD and registering models and properties

        state = State()

        self.register_simple_types(state)

        # TODO: two options: -
        #  1. we register all top level things first and then use when we need them
        #  2. we register every model (from element or separate complexType) when we meet them, but check if we already have processed them before (by XSD name)

        self.process_xml_tree(state)

        # post processing

        self._post_process_refs()

        # we need to add this here, because only now we will know if it has properties and if we need to create it
        self._post_process_resource_model()

    def process_xml_tree(self, state):
        state.model_deduplicate = Deduplicator()
        for node in self.xml_tree.nodes:
            if node.is_top_level:
                self.process_node(node, state)

    def process_node(self, node: XMLNode, state: State):

        if node.type == "element":
            return self.process_element(node, state)
        elif node.type == "complexType":
            return self.process_complex_type(node, state)
        elif node.type == "simpleType":
            pass
        else:
            raise RuntimeError(f'This node type cannot be at the top level: {node.type}')

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


    #  XSD nodes processors

    def process_element(self, node: XMLNode, state: State) -> list[XSDProperty]:
        model = None
        if node.attributes.get("name"):
            property_name = node.attributes["name"]
        elif node.attributes.get("ref"):
            property_name = node.attributes["ref"]
        else:
            raise RuntimeError(f'Element has to have either name or ref')
        prop = XSDProperty()
        prop.name = property_name
        if node.children:
            for child in node.children:
                if child.type == "complexType":
                    models = self.process_complex_type(child, state)
                    for model in models:
                        model.set_name(self.deduplicate_model_name(child.attributes.get("name")))
                        prop.ref_model = model
                elif child.type == "simpleType":
                    result = self.process_simple_type(child, state)
                else:
                    raise RuntimeError(f"This node type cannot be in the complex type: {node.type}")

        # todo if element has choice inside, we will create multiple models, so multiple properties also

        # todo: if it's a top level, we only register a model and don't return anything.
        #  if it's not a top level, we register a model and return a property

        # todo decide where to deal with placeholder elements, which are not turned into a model
        #  talk to Mantas if a model is considered a placeholder model if it has only one ref or even if it has more refs but nothing else

        # todo If it's top level, we need to know if we need to add it to the resource model or not. Maybe after we return from this, we need to check if the property is `ref`. If it's top level and not ref, we add it to the "resource" model

        #  todo factor in minoccurs and maxoccurs

        self.models[model.basename] = model

        return [prop]

    def process_complex_type(self, node: XMLNode, state: State) -> list[XSDModel]:

        model = XSDModel()
        name = node.attributes.get("name")
        if name:
            model.name = self.deduplicate_model_name(name)
        state.property_deduplicate = Deduplicator()
        properties = {}
        choice_props = None
        for child in node.children:
            if child.type == "attribute":
                prop = self.process_attribute(child, state)
                prop.id = state.property_deduplicate(prop.id_raw)
                properties[prop.id] = prop

            elif child.type == "sequence":
                sequence_props = self.process_sequence(child, state)
                properties.update(sequence_props)
            elif child.type == "choice":
                choice_props = self.process_sequence(child, state)

        model.properties = properties
        if choice_props:
            pass
            # todo duplicate the model many times, each with each choice_prop and
        return [model]

    def _map_type(self, xsd_type: str) -> XSDType:
        """Gets XSD Type, returns DSA type (XSDType class)"""
        property_type = DATATYPES_MAPPING[xsd_type]
        dsa_type = XSDType()
        dsa_type.name = xsd_type
        if ";" in property_type:
            property_type, target, value = property_type.split(";")
            dsa_type.type = property_type
            if target == "enum":
                dsa_type.enum = value
            if target == "prepare":
                dsa_type.prepare = value
        return dsa_type

    def process_attribute(self, node: XMLNode, state: State) -> XSDProperty:
        prop = XSDProperty()
        prop.source = f"@{node.attributes.get('name')}"
        prop.id_raw = node.attributes.get('name')

        attribute_type = node.attributes.get("type")

        if attribute_type:
            if attribute_type in DATATYPES_MAPPING:
                prop.type = self._map_type(attribute_type)
            elif attribute_type in self.custom_types:
                prop.type = self.custom_types[attribute_type]

        prop.type = node.attributes.get("type")

        for child in node.children:
            if child.type == "simpleType":
                prop.type = self.process_simple_type(child, state)
            elif child.type == "annotation":
                prop.description = self.process_annotation(child, state)
            else:
                raise RuntimeError(f"Unexpected element type inside attribute element: {child.type}")
        return prop

    def process_enumeration(self, node: XMLNode, state: State) -> dict[str, dict[str, str]]:
        enum_value = node.attributes.get("value")
        enum_item = {enum_value: {"source": enum_value}}
        return enum_item

    def process_simple_type(self, node: XMLNode, state: State) -> XSDType:
        for child in node.children:
            if child.type == "restriction":
                # get everything from restriction, just add name if exists
                property_type = self.process_restriction(child, state)
                if node.attributes.get("name"):
                    property_type.name = node.attributes.get("name")

                return property_type

            else:
                raise RuntimeError(f"Unexpected element type inside simpleType element: {child.type}")

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

    def process_restriction(self, node: XMLNode, state: State) -> XSDType:

        property_type = XSDType()
        base = node.attributes.get("base")
        property_type.type = self._map_type(base)
        enumerations = {}
        for child in node.children:
            if child.type == "enumeration":
                enum = self.process_enumeration(child, state)
                enumerations.update(enum)
            else:
                raise RuntimeError(f"Unexpected element type inside restriction element: {child.type}")
        if enumerations:
            enums = {"": enumerations}
            property_type.enums = enums
        return property_type


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
        # raise Exception("Unsupported element")
        # TODO: create specific error
        pass

    def process_fraction_digits(self, node: XMLNode, state: State) -> None:
        # logging.log(logging.INFO, "met an unsupported type fractionDigits")
        # TODO: configure logger
        pass

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

    for model_name, parsed_model in xsd.models.items():
        yield None, parsed_model.get_data()
