from __future__ import annotations

import os
from copy import deepcopy
from typing import Any

from lxml.etree import _Element

from spinta.components import Context
from lxml import etree, objectify
from urllib.request import urlopen

from spinta.core.ufuncs import Expr
from spinta.utils.naming import to_property_name, to_model_name, Deduplicator, to_dataset_name


# mapping of XSD datatypes to DSA datatypes
# XSD datatypes: https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes
# DSA datatypes: https://atviriduomenys.readthedocs.io/dsa/duomenu-tipai.html#duomenu-tipai
# TODO: finish mapping and make sure all things are mapped correctly
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


class XSDModel:
    """
    Class for creating and handling DSA models from XSD files.

    Example of model data:
    {
        "type": "model",
        "name": "",
        "description": "Įvairūs duomenys",
        "properties": {},
        "external": resource_model_external_info,
        "uri": "http://www.w3.org/2000/01/rdf-schema#Resource"
    }
    """

    def __init__(self, xsd: 'XSDReader', node: _Element = None):
        self.deduplicate = Deduplicator()

        self.xsd: 'XSDReader' = xsd
        self.dataset_name: str = xsd.dataset_name
        self.node: _Element = node
        self.type: str = "model"
        self.name: str | None = None
        self.basename: str | None = None
        self.external: dict | None = None
        self.properties: dict | None = None
        self.uri: str | None = None
        self.description: str | None = None
        self.root_properties: dict | None = None
        self.parent_model: XSDModel | None = None
        self.is_root_model: bool | None = None

    def get_data(self):
        model_data: dict = {
            "type": "model",
            "name": self.name
        }
        if self.description is not None:
            model_data["description"] = self.description
        if self.properties is not None:
            model_data["properties"] = self.properties
        if self.external is not None:
            model_data["external"] = self.external
        if self.uri is not None:
            model_data["uri"] = self.uri

        return model_data

    def add_external_info(self, external_name: str):
        self.external = {
            "dataset": self.dataset_name,
            "resource": "resource1",
            "name": external_name
        }

    def set_name(self, name: str):
        self.basename = name
        self.name = f"{self.dataset_name}/{name}"

    def _get_property_type(self, node: _Element) -> str:
        if node.get("ref"):
            return "ref"
        property_type: str = node.get("type")
        if not property_type:
            # this is a self defined simple type, so we take it's base as type
            restrictions: list = node.xpath(f'./*[local-name() = "simpleType"]/*[local-name() = "restriction"]')
            if restrictions:
                property_type = restrictions[0].get("base", "")
            else:
                property_type = ""
        # getting rid of the prefix
        if ":" in property_type:
            property_type = property_type.split(":")[1]

        if property_type in self.xsd.custom_types:
            property_type = self.xsd.custom_types.get(property_type).get("base", "")
        if property_type in DATATYPES_MAPPING:
            property_type = DATATYPES_MAPPING[property_type]
        else:
            property_type = "string"

        return property_type

    def _get_enums(self, node: _Element) -> dict[str, dict[str, Any]]:
        enums = {}
        simple_type = node.xpath(f'./*[local-name() = "simpleType"]')
        if simple_type:
            enums = self.xsd.get_enums_from_simple_type(simple_type[0])
        else:
            node_type = node.get("type")
            if node_type and ":" in node_type:
                node_type = node_type.split(":")[1]
            if node_type in self.xsd.custom_types:
                enums = self.xsd.custom_types[node_type]["enums"]

        return enums

    def _node_to_partial_property(self, node: _Element) -> tuple[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]:
        """Node can be either element or attribute.
        This function only processes things common to attributes and elements"""
        prop = dict()

        prop["description"] = XSDReader.get_description(node)
        property_name = node.get("name")
        prop["external"] = {"name": property_name}
        property_id = to_property_name(property_name)
        prop["type"] = self._get_property_type(node)
        if ";" in prop["type"]:
            prop_type, target, value = prop["type"].split(";")
            prop["type"] = prop_type
            if target == "enum":
                prop[target] = value
            if target == "prepare":
                prop["external"][target] = Expr(value)

        prop["enums"] = self._get_enums(node)

        return self.deduplicate(property_id), prop

    def attributes_to_properties(
        self,
        element: _Element
    ) -> dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]:
        properties = {}
        attributes = element.xpath(f'./*[local-name() = "attribute"]')
        complex_type = element.xpath(f'./*[local-name() = "complexType"]')
        if complex_type:
            properties.update(self.attributes_to_properties(complex_type[0]))
        for attribute in attributes:

            property_id, prop = self._node_to_partial_property(attribute)
            if not prop["type"]:
                prop["type"] = "string"
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

        # TODO: attribute can be a ref to an externally defined attribute also. Not in RC though
        #  https://github.com/atviriduomenys/spinta/issues/605
        return properties

    def simple_element_to_property(
        self,
        element: _Element,
        is_array: bool = False
    ) -> tuple[str, dict[str, str | bool | dict[str, Any]]]:
        """
        simple element is an element which is either
        an inline or simple type element and doesn't have a ref

        Example of a property:

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

        property_id, prop = self._node_to_partial_property(element)
        if XSDReader.node_is_ref(element):
            ref: str = element.get("ref")
            if ":" in ref:
                ref = ref.split(":")[1]
            prop["external"]["name"] = ref
            property_id = self.deduplicate(to_property_name(ref))
        prop["external"]["name"] = f'{prop["external"]["name"]}/text()'
        if prop.get("type") == "":
            prop["type"] = "string"
        if XSDReader.is_array(element) or is_array:
            property_id += "[]"
        if XSDReader.is_required(element):
            prop["required"] = True
        else:
            prop["required"] = False
        return property_id, prop

    def properties_from_simple_elements(
        self,
        node: _Element,
        from_root: bool = False,
        properties_required: bool = None
    ) -> dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]:

        properties = {}
        elements = node.xpath(f'./*[local-name() = "element"]')
        for element in elements:
            if (self.xsd.node_is_simple_type_or_inline(element) and
                    not XSDReader.node_is_ref(element) and
                    not (self.xsd._node_is_referenced(element) and from_root)):
                property_id, prop = self.simple_element_to_property(element)
                if from_root:
                    prop["required"] = False
                if properties_required is True:
                    prop["required"] = True
                if properties_required is False:
                    prop["required"] = False
                properties[property_id] = prop
        return properties

    def get_text_property(self, property_type = None) -> dict[str, dict[str, str | dict[str, str]]]:
        if property_type is None:
            property_type = "string"
        return {
            self.deduplicate('text'): {
                'type': property_type,
                'external': {
                    'name': 'text()'
                }
            }}


class XSDReader:

    def __init__(self, path, dataset_name: str):
        self._path: str = path
        self.models: list[XSDModel] = []
        self.custom_types: dict = {}
        self._dataset_given_name: str = dataset_name
        self._set_dataset_and_resource_info()
        self.deduplicate: Deduplicator = Deduplicator()

    @staticmethod
    def get_enums_from_simple_type(node: _Element) -> dict[str, dict[str, Any]]:
        enums = {}
        enum_value = {}
        restrictions = node.xpath(f'./*[local-name() = "restriction"]')
        if restrictions:
            # can be enum
            enumerations = restrictions[0].xpath('./*[local-name() = "enumeration"]')
            for enumeration in enumerations:
                enum_item = {
                    "source": enumeration.get("value")
                }
                enum_value.update({enumeration.get("value"): enum_item})
            enums[""] = enum_value

        return enums

    def _extract_custom_types(self, node: _Element):
        """
        format of custom types:
        {
            "type_name": {
                "base": "type_base"
            }
        }
        """
        custom_types_nodes = node.xpath(f'./*[local-name() = "simpleType"]')
        custom_types = {}
        for type_node in custom_types_nodes:
            type_name = type_node.get("name")
            restrictions = type_node.xpath(f'./*[local-name() = "restriction"]')
            property_type_base = restrictions[0].get("base", "")
            property_type_base = property_type_base.split(":")[1]

            # enums
            enums = self.get_enums_from_simple_type(type_node)

            custom_types[type_name] = {
                "base": property_type_base,
                "enums": enums
            }
        self.custom_types = custom_types

    @staticmethod
    def get_description(element: _Element) -> str:
        annotation = element.xpath(f'./*[local-name() = "annotation"]')
        if not annotation:
            annotation = element.xpath(f'./*[local-name() = "simpleType"]/*[local-name() = "annotation"]')
        description = ""
        if annotation:
            documentation = annotation[0].xpath(f'./*[local-name() = "documentation"]')
            for documentation_part in documentation:
                if documentation_part.text is not None:
                    description = f"{description}{documentation_part.text} "
        return description.strip()

    def _node_is_referenced(self, node):
        # if this node is referenced by some other node
        node_name = node.get('name')
        xpath_search_string = f'//*[@ref="{node_name}"]'
        references = self.root.xpath(xpath_search_string)
        if references:
            return True
        return False

    def _get_referenced_node(self, node):
        ref = node.get("ref")
        if ":" in ref:
            ref = ref.split(":")[1]
        xpath_query = f"//*[@name='{ref}']"
        referenced_node = self.root.xpath(xpath_query)[0]
        return referenced_node

    @staticmethod
    def node_is_ref(node: _Element) -> bool:
        if node.get("ref"):
            return True
        return False

    @staticmethod
    def is_array(element: _Element) -> bool:
        max_occurs: str = element.get("maxOccurs", "1")
        return max_occurs == "unbounded" or int(max_occurs) > 1
    
    def _extract_root(self):
        if self._path.startswith("http"):
            document = etree.parse(urlopen(self._path))
            objectify.deannotate(document, cleanup_namespaces=True)
            self.root = document.getroot()
        else:
            with open(self._path) as file:
                text = file.read()
                self.root = etree.fromstring(bytes(text, encoding='utf-8'))

    def _set_dataset_and_resource_info(self):
        self.dataset_name = to_dataset_name(os.path.splitext(os.path.basename(self._path))[0])
        self.dataset_and_resource_info = {
            'type': 'dataset',
            'name': self.dataset_name,
            'resources': {
                "resource1": {
                    'type': 'xml',
                },
            },
            'given_name': self._dataset_given_name
        }

    def _get_separate_complex_type_node_by_type(self, node_type: str) -> _Element:
        if node_type is not None:
            node_type = node_type.split(":")
            if len(node_type) > 1:
                node_type = node_type[1]
            else:
                node_type = node_type[0]
        if node_type not in DATATYPES_MAPPING:
            complex_types = self.root.xpath(f'./*[local-name() = "complexType"]')
            for node in complex_types:
                if node.get("name") == node_type:
                    return node

    def _get_separate_complex_type_node(self, node: _Element) -> _Element:
        node_type: str | list = node.get('type')
        return self._get_separate_complex_type_node_by_type(node_type)

    def _node_has_separate_complex_type(self, node: _Element) -> bool:
        node_type: str | list = node.get('type')
        if node_type is not None:
            node_type = node_type.split(":")
            if len(node_type) > 1:
                node_type = node_type[1]
            else:
                node_type = node_type[0]
            if node_type not in DATATYPES_MAPPING:
                complex_types = self.root.xpath(f'./*[local-name() = "complexType"]')
                for node in complex_types:
                    if node.get("name") == node_type:
                        return True
        return False

    def node_is_simple_type_or_inline(self, node: _Element) -> bool:
        if self._node_has_separate_complex_type(node):
            return False
        return bool(
            (node.xpath(f'./*[local-name() = "annotation"]') and len(node.getchildren()) == 1) or
            (node.xpath(f'./*[local-name() = "simpleType"]')) or
            (len(node.getchildren()) == 0)
        )

    @staticmethod
    def _is_element(node: _Element) -> bool:
        if node.xpath('local-name()') == "element":
            return True
        return False

    @staticmethod
    def is_required(element: _Element) -> bool:
        min_occurs = int(element.get("minOccurs", 1))
        if min_occurs > 0:
            nillable = element.get("nillable", False)
            if nillable == "true":
                return False
            return True
        return False

    def _properties_from_references(
        self,
        node: _Element,
        model: XSDModel,
        parent_model: XSDModel,
        source_path: str = ""
    ) -> tuple[
            dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]],
            dict[str, dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]]]:

        properties = {}

        # nested properties for the root model
        root_properties = {}

        for ref_element in node.xpath("./*[@ref]"):
            referenced_element = self._get_referenced_node(ref_element)

            if self.node_is_simple_type_or_inline(referenced_element):
                property_id, prop = model.simple_element_to_property(referenced_element)
                if not XSDReader.is_required(ref_element):
                    prop["required"] = False
                properties[property_id] = prop
            else:
                is_array = False
                    # TODO fix this because it probably doesn't cover all cases, only something like <complexType><sequence><item>
                    #  https://github.com/atviriduomenys/spinta/issues/613
                complex_type = referenced_element.xpath("./*[local-name() = 'complexType']")[0]
                sequences = complex_type.xpath("./*[local-name() = 'sequence']")
                if sequences:
                    sequence = sequences[0]
                else:
                    sequence = None
                if sequence is not None and len(sequence) == 1 and self.node_is_ref(sequence[0]):
                    is_array = XSDReader.is_array(referenced_element)
                    if not is_array:
                        is_array = XSDReader.is_array(complex_type[0][0])
                    new_referenced_element = self._get_referenced_node(complex_type[0][0])
                    referenced_element = new_referenced_element
                    if ref_element.get("name") is not None:
                        source_path += f'/{ref_element.get("name")}'

                if not (XSDReader.is_array(ref_element) or is_array):
                    referenced_model_names, new_root_properties = self._create_model(
                        referenced_element,
                        source_path=source_path,
                        parent_model=model
                    )
                    property_type = "ref"
                else:
                    referenced_element_properties = {
                        to_property_name(model.basename):
                        {
                            "type": "ref",
                            "model": f"{model.name}"
                        }
                    }
                    property_type = "backref"
                    referenced_model_names, new_root_properties = self._create_model(
                        referenced_element,
                        source_path=source_path,
                        parent_model=model,
                        additional_properties=referenced_element_properties
                    )

                root_properties.update(new_root_properties)

                for referenced_model_name in referenced_model_names:
                    property_id, prop = model.simple_element_to_property(ref_element, is_array=is_array)

                    prop["external"]["name"] = ""
                    prop["type"] = property_type
                    prop["model"] = f"{referenced_model_name}"
                    properties[property_id] = prop

        return properties, root_properties

    def _split_choice(
        self,
        node: _Element,
        source_path: str,
        parent_model: XSDModel,
        additional_properties: dict[str, dict[str, str | bool | dict[str, str]]]
    ) -> tuple[list[str], dict[str, str | bool | dict[str, str | dict[str, Any]]]]:
        """
        If there are choices in the element,
        we need to split it and create a separate model per each choice
        """

        # nested properties for the root model
        root_properties = {}

        model_names = []
        node_copy = deepcopy(node)
        if self._node_has_separate_complex_type(node_copy):
            complex_type_node = self._get_separate_complex_type_node(node_copy)
        else:
            complex_type_node = node_copy.xpath(f'./*[local-name() = "complexType"]')[0]

        choice_nodes = complex_type_node.xpath(f'./*[local-name() = "choice"]')

        # if maxOccurs=unbound and there's a second `choice` inside, we have to split that one
        if choice_nodes and choice_nodes[0].get("maxOccurs") == "unbounded":
            choice_nodes = choice_nodes[0].xpath(f'./*[local-name() = "choice"]')
        if choice_nodes:
            choice_node = choice_nodes[0]
        else:
            choice_node = complex_type_node.xpath(f'./*[local-name() = "sequence"]/*[local-name() = "choice"]')[0]
        if len(choice_node) > 0:
            choice_node_copy = deepcopy(choice_node)
            choice_node_parent = choice_node.getparent()
            choice_node_parent.remove(choice_node)

            for choice in choice_node_copy:
                if complex_type_node.xpath(f'./*[local-name() = "sequence"]') and choice_node_copy.xpath(
                        f'./*[local-name() = "sequence"]'):
                    choice_copy = deepcopy(choice)
                    for node_in_choice in choice:
                        choice_node_parent.insert(0, node_in_choice)
                    returned_model_names, new_root_properties = self._create_model(
                        node_copy,
                        source_path=source_path,
                        parent_model=parent_model,
                        additional_properties=additional_properties)
                    root_properties.update(new_root_properties)
                    model_names.extend(returned_model_names)

                    for node_in_choice in choice_copy:
                        node_in_choice = choice_node_parent.xpath(f"./*[@name=\'{node_in_choice.get('name')}\']")[0]
                        choice_node_parent.remove(node_in_choice)
                else:
                    choice_node_parent.insert(0, choice)
                    returned_model_names, new_root_properties = self._create_model(
                        node_copy,
                        source_path=source_path,
                        parent_model=parent_model,
                        additional_properties=additional_properties)
                    model_names.extend(returned_model_names)
                    root_properties.update(new_root_properties)

                    choice_node_parent.remove(choice)
        return model_names, root_properties

    def _create_model(
        self,
        node: _Element,
        source_path: str = "",
        is_root_model: bool = False,
        parent_model: XSDModel = None,
        additional_properties: dict[str, str | bool | dict[str, str | dict[str, Any]]] = None
    ) -> tuple[list[str], dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]]:
        """
        Parses an element and makes a model out of it. If it is a complete model, it will be added to the models list.
        """
        model = XSDModel(self)
        model.parent_model = parent_model

        if additional_properties is None:
            additional_properties = {}

        # properties to add to the root model
        root_properties = {}

        # properties of this model
        properties = {}
        properties.update(additional_properties)
        properties.update(model.attributes_to_properties(node))

        new_source_path = f"{source_path}/{node.get('name')}"

        model.set_name(self.deduplicate(to_model_name(node.get("name"))))

        if node.xpath(f'./*[local-name() = "complexType"]') or self._node_has_separate_complex_type(node):

            if self._node_has_separate_complex_type(node):
                complex_type_node = self._get_separate_complex_type_node(node)
            else:
                complex_type_node = node.xpath(f'./*[local-name() = "complexType"]')[0]

            # if there is choices, we need to create a separate model for each choice
            choices = complex_type_node.xpath(f'./*[local-name() = "choice"]')
            # if choices is unbounded, we treat it like sequence
            if not choices or choices[0].get("maxOccurs") == "unbounded":
                if choices:
                    choices = complex_type_node.xpath(f'./*[local-name() = "choice"]/*[local-name() = "choice"]')
                else:
                    choices = complex_type_node.xpath(f'./*[local-name() = "sequence"]/*[local-name() = "choice"]')
            if choices:
                if choices[0].get("maxOccurs") != "unbounded":
                    return self._split_choice(
                        node,
                        source_path=source_path,
                        parent_model=model,
                        additional_properties=additional_properties)

            # if complextype node's property mixed is true, it allows text inside
            if complex_type_node.get("mixed") == "true":
                properties.update(model.get_text_property())

            # if this is complexType node which has complexContent, with a separate
            # node, we need to join the contents of them both
            if complex_type_node.xpath(f'./*[local-name() = "complexContent"]'):
                complex_type_node = complex_type_node.xpath(f'./*[local-name() = "complexContent"]/*[local-name() = "extension"]')[0]
                complex_content_base_name = complex_type_node.get("base")
                complex_content_base_node = self._get_separate_complex_type_node_by_type(complex_content_base_name)
                if complex_content_base_node.xpath(f'./*[local-name() = "sequence"]'):
                    sequence_node = complex_content_base_node.xpath(f'./*[local-name() = "sequence"]')[0]
                    properties.update(model.properties_from_simple_elements(sequence_node))
                # TODO: in this case, it might be something else, not sequence too

            if complex_type_node.xpath(f'./*[local-name() = "sequence"]') \
                    or complex_type_node.xpath(f'./*[local-name() = "all"]')\
                    or complex_type_node.xpath(f'./*[local-name() = "simpleContent"]')\
                    or len(complex_type_node) > 0:
                """
                source: https://stackoverflow.com/questions/36286056/the-difference-between-all-sequence-choice-and-group-in-xsd
                    When to use xsd:all, xsd:sequence, xsd:choice, or xsd:group:

                    Use xsd:all when all child elements must be present, independent of order.
                    Use xsd:sequence when child elements must be present per their occurrence constraints and order does matters.
                    Use xsd:choice when one of the child element must be present.
                    Use xsd:group as a way to wrap any of the above in order to name and reuse in multiple locations within an XSD.
                    Note that occurrence constraints can appear on xsd:all, xsd:sequence, or xsd:choice in addition to the 
                    child elements to achieve various cardinality effects. For example, if minOccurs="0" were added to xsd:element children of xsd:all, element order would be insignificant,
                    but not all child elements would have to be present.
                """
                properties_required = None
                if complex_type_node.xpath(f'./*[local-name() = "sequence"]'):
                    sequence_or_all_nodes = complex_type_node.xpath(f'./*[local-name() = "sequence"]')
                elif complex_type_node.xpath(f'./*[local-name() = "all"]'):
                    sequence_or_all_nodes = complex_type_node.xpath(f'./*[local-name() = "all"]')
                elif complex_type_node.xpath(f'./*[local-name() = "choice"]'):
                    sequence_or_all_nodes = complex_type_node.xpath(f'./*[local-name() = "choice"]')
                    properties_required = False
                elif complex_type_node.xpath(f'./*[local-name() = "simpleContent"]'):
                    sequence_or_all_nodes = complex_type_node.xpath(f'./*[local-name() = "simpleContent"]/*[local-name() = "extension"]')

                    text_property_type = sequence_or_all_nodes[0].get("base")
                    if text_property_type is not None:
                        if ":" in text_property_type:
                            text_property_type = text_property_type.split(":")[1]
                        text_property_type = DATATYPES_MAPPING[text_property_type]
                    text_property = model.get_text_property(text_property_type)
                    properties.update(text_property)
                    # additionally extract properties from attributes, if it's let's say, simpleContent node
                    properties.update(model.attributes_to_properties(sequence_or_all_nodes[0]))
                else:
                    sequence_or_all_nodes = None
                if sequence_or_all_nodes:
                    sequence_or_all_node = sequence_or_all_nodes[0]
                else:
                    sequence_or_all_node = complex_type_node
                sequence_or_all_node_length = len(sequence_or_all_node)
                # There is only one element in the complex node sequence, and it doesn't have annotation.
                # Then we just go deeper and add this model to the next model's path.
                if sequence_or_all_node_length == 1 and not properties:

                    if sequence_or_all_node.xpath(f'./*[local-name() = "element"]'):
                        if not sequence_or_all_node.xpath(f'./*[local-name() = "element"]')[0].get("ref"):
                            element = sequence_or_all_node.xpath(f'./*[local-name() = "element"]')[0]
                            if self.node_is_simple_type_or_inline(element) and not self.node_is_ref(element):
                                properties.update(model.properties_from_simple_elements(sequence_or_all_node, properties_required=properties_required))
                            # check for recursion
                            # TODO: maybe move this to a separate function
                            # TODO: recursion not fully working
                            #  https://github.com/atviriduomenys/spinta/issues/602
                            else:
                                paths = new_source_path.split("/")
                                if not element.get("name") in paths:

                                    # this can sometimes happen when choice node has been split or maybe in some other cases too
                                    return self._create_model(
                                        element,
                                        source_path=new_source_path,
                                        parent_model=model)
                                else:
                                    for index, path in enumerate(paths):
                                        if path == element.get("name"):
                                            paths[index] = f"/{path}"
                                    new_source_path = "/".join(paths)

                        else:
                            # TODO: if reference is to an inline or simpleType element,
                            #  and maxOccurs of it is 1,
                            #  then do not create reference, but add to the same

                            element = sequence_or_all_node.xpath(f'./*[local-name() = "element"]')[0]
                            element = self._get_referenced_node(element)
                            if not is_root_model:
                                return self._create_model(
                                    element,
                                    source_path=new_source_path,
                                    parent_model=model,
                                    additional_properties=additional_properties)
                            else:
                                _, new_root_properties = self._create_model(
                                    element,
                                    source_path=new_source_path,
                                    parent_model=model,
                                    additional_properties=additional_properties)
                                root_properties.update(new_root_properties)

                elif sequence_or_all_node_length > 1 or properties:
                    # properties from simple type or inline elements without references
                    # properties are required for choice where maxOccurs=unbound and maybe some other cases
                    properties.update(model.properties_from_simple_elements(
                        sequence_or_all_node,
                        properties_required=properties_required))

                    # references
                    properties_from_references, new_root_properties = self._properties_from_references(
                        sequence_or_all_node,
                        model=model,
                        parent_model=parent_model,
                        source_path=new_source_path)
                    properties.update(properties_from_references)
                    root_properties.update(new_root_properties)

                    # complex type child nodes - to models
                    for child_node in sequence_or_all_node:
                        if child_node.xpath(f'./*[local-name() = "complexType"]') \
                                or self._node_has_separate_complex_type(child_node):
                            # check for recursion
                            paths = new_source_path.split("/")
                            if not child_node.get("name") in paths:
                                _, new_root_properties = self._create_model(
                                    child_node,
                                    source_path=new_source_path,
                                    parent_model=model
                                )
                                root_properties.update(new_root_properties)
                            else:
                                for index, path in enumerate(paths):
                                    if path == child_node.get("name"):
                                        paths[index] = f"/{path}"
                                new_source_path = "/".join(paths)

        model.properties = properties

        # DEALING WITH NESTED ROOT PROPERTIES ---------------------

        if properties:
            # new_root_properties are to pass up to the root model and to add to it
            new_root_properties = {}
            # add the model prefix to every property name and source
            for model_name, model_properties in root_properties.items():
                for root_property_id, root_property in model_properties.items():

                    # we don't need to add refs and backrefs, only actual fields
                    # TODO: I don't know if we need this check. I would assume that we don't.
                    #  But if we delete this check, then it doesn't find the referenced model
                    #  if there is a model in between
                    #  Example: Objektai has faktai[].faktu_naudotojai[].naudotojo_id then
                    #  Faktu_Naudotojai has referencce to Faktai but not Objektai
                    if not root_property.get("type") == "ref" and not root_property.get("type") == "backref":

                        # we need to find out the name of the property that corresponds the model,
                        #  because we need to use that if we used it in ref properties, otherwise use
                        prefix = None
                        stripped_model_name = model_name.rstrip("[]")
                        for property_id, prop in properties.items():
                            if "model" in prop:
                                property_model_name = prop.get("model").split("/")[-1]
                                if property_model_name == stripped_model_name:
                                    prefix = property_id
                                    break

                        array_sign = ""
                        if prefix is None:
                            if model_name.endswith("[]"):
                                array_sign = "[]"

                            prefix = to_property_name(stripped_model_name.split("/")[-1])

                        root_property_id = f"{prefix}{array_sign}.{root_property_id}"
                        new_root_properties[root_property_id] = root_property

            model.root_properties = new_root_properties

            new_root_properties.update(deepcopy(properties))

            # adding node source to the source path here, before passing further,
            # because we can't retrieve it later otherwise
            # returned_root_properties = {}
            # for root_property_id, root_property in new_root_properties.items():
            #     new_root_property = deepcopy(root_property)
            #
            #     if "external" in root_property:
            #         new_root_property["external"] = {"name": f"{node.get('name')}/{root_property['external']['name']}"}
            #     returned_root_properties[root_property_id] = new_root_property
            returned_root_properties = new_root_properties

            model.add_external_info(external_name=new_source_path)
            model.description = self.get_description(node)
            self.models.append(model)

            if additional_properties:
                model_name = f"{model.name}[]"
            else:
                model_name = model.name

            return [model.name, ], {model_name: returned_root_properties}

        return [], root_properties

    def _add_resource_model(self):
        resource_model = XSDModel(self)
        resource_model.add_external_info(external_name="/")
        resource_model.type = "model"
        resource_model.description = "Įvairūs duomenys"
        resource_model.uri = "http://www.w3.org/2000/01/rdf-schema#Resource"
        resource_model.properties = resource_model.properties_from_simple_elements(self.root, from_root=True)
        resource_model.root_properties = {}
        if resource_model.properties:
            resource_model.set_name(self.deduplicate(f"Resource"))
            self.models.append(resource_model)

    def _parse_root_node(self):
        for node in self.root:
            if (
                    self._is_element(node) and
                    (not self.node_is_simple_type_or_inline(node) or self.node_is_ref(node)) and
                    not self._node_is_referenced(node)
            ):
                self._create_model(node, is_root_model=True)

    def start(self):
        self._extract_root()
        self._extract_custom_types(self.root)
        self._add_resource_model()

        self._parse_root_node()


def read_schema(
    context: Context,
    path: str,
    prepare: str = None,
    dataset_name: str = ''
) -> dict[Any, dict[str, str | dict[str, str | bool | dict[str, str | dict[str, Any]]]]]:
    """
    This reads XSD schema from the url provided in path and yields asd schema models

    For now this is adjusted for XSD schemas of Registrų centras

    Elements can be:
    1. Simple type inline
    2. Simple type
    3. Complex type
    4. Complex type described separately

    Elements can have annotations, which we add as description to either a model or a property.

    1. Simple type.
        a) not a reference. It's a property
        b) a reference
            i. maxOccurs = 1
                A. both referencing and referenced elements has annotations. A ref property
                B. Only one or none of the elements has annotations. A part of the path
            ii. maxOccurs > 1 - a ref property
    2. Inline type.

        a) inline type or a custom type referencing to simple type. A property
        b) complex type defined separately of this element.

        If it's a root element, it's then added as a property to a special Resource model.

        Simple type can have annotation too.

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

    4. complex type described separately

    We will build a list of models
    (another option was to make a generator for parsing models, and going deeper, but that would
    be more complex when returning models. although this option is also possible, but it can
    be reworked into this from an option with a list)

    If some model has references in the sequence, we need to also add those as models,
    mark their type as backref. In this case, we add them at the moment we meet them
    as xsd refs, because this way we will know that they are backrefs. We also need to add refs on
    those models to the current model.

    Element can be as a ref in more than one other element.

    Other things to handle: Resource model, custom types, enumerations, choices

    Element can be turned into:
        1. A property (including reference properties)
        2. A model (including referenced models)
        3. A part of another model (as a path)
        4. A part of another property (as a path)

    Attribute can only be turned into a property

    A property can also be text()
    """
    xsd = XSDReader(path, dataset_name)

    xsd.start()

    yield None, xsd.dataset_and_resource_info

    for parsed_model in xsd.models:

        # we need to add root properties to properties if it's a root model
        if parsed_model.parent_model is None or parsed_model.parent_model not in xsd.models:
            parsed_model.properties.update(parsed_model.root_properties)

        parsed_model.properties = dict(sorted(parsed_model.properties.items()))

        yield None, parsed_model.get_data()
