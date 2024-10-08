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
    deduplicate: Deduplicator
    xsd: 'XSDReader'
    dataset_name: str
    node: _Element
    type: str = "model"
    name: str | None = None
    basename: str | None = None
    external: dict | None = None
    properties: dict | None = None
    uri: str | None = None
    description: str | None = None
    root_properties: dict | None = None
    parent_model: XSDModel | None = None
    is_root_model: bool | None = None

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

    def __eq__(self, other: XSDModel) -> bool:
        if self.properties == other.properties:
            return True
        return False

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
        prop["type"] = self.xsd.get_property_type(node)
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
        is_array: bool = False,
        source_path: str = None
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
        if source_path:
            prop["external"]["name"] = f'{source_path}/{prop["external"]["name"]}'
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
        is_array = False
        if self.xsd.is_array(node):
            is_array = True

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
                if is_array:
                    property_id = f"{property_id}[]"
                properties[property_id] = prop
        return properties

    def get_text_property(self, property_type=None) -> dict[str, dict[str, str | dict[str, str]]]:
        if property_type is None:
            property_type = "string"
        return {
            self.deduplicate('text'): {
                'type': property_type,
                'external': {
                    'name': 'text()'
                }
            }}

    def has_non_ref_properties(self) -> bool:
        return any([prop["type"] not in ("ref", "backerf") for prop in self.properties.values()])

    def add_ref_property(self, ref_model):
        property_id = self.deduplicate(to_property_name(ref_model.basename))
        prop = {"type": "ref", "model": ref_model.name}
        self.properties.update({property_id: prop})


class XSDReader:

    def __init__(self, path, dataset_name: str):
        self._path: str = path
        self.models: dict[str, XSDModel] = {}
        self.custom_types: dict = {}
        self._dataset_given_name: str = dataset_name
        self._set_dataset_and_resource_info()
        self.deduplicate: Deduplicator = Deduplicator()

    def get_property_type(self, node: _Element) -> str:
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

        if property_type in self.custom_types:
            property_type = self.custom_types.get(property_type).get("base", "")
        if property_type in DATATYPES_MAPPING:
            property_type = DATATYPES_MAPPING[property_type]
        else:
            property_type = "string"

        return property_type

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

    def _extract_custom_simple_types(self, node: _Element):
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

        # also check with namespace prefixes.
        # Though, it is possible that this isn't correct XSD behaviour, but it seems common in RC
        if not references:
            for prefix in self.namespaces:
                prefixed_node_name = f"{prefix}:{node_name}"
                xpath_search_string = f'//*[@ref="{prefixed_node_name}"]'
                references = self.root.xpath(xpath_search_string)
                if references:
                    return True
        if references:
            return True
        return False

    def _get_referenced_node(self, node):
        ref = node.get("ref")
        if ":" in ref:
            ref = ref.split(":")[1]
        xpath_query = f"/*/*[@name='{ref}']"
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

    def _build_ref_properties(self, complex_type: _Element, is_array: bool, model: XSDModel, new_source_path: str, node: _Element, referenced_element: _Element) -> dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]:
        """
        Helper method for methods properties_from_references and properties_from_type_references
        """

        properties = {}

        sequences = complex_type.xpath("./*[local-name() = 'sequence']")
        if not sequences:
            choices = complex_type.xpath("./*[local-name() = 'choice']")
            if choices and XSDReader.is_array(choices[0]):
                if len(choices[0]) == 1 and not complex_type.get("mixed") == "true":
                    is_array = True

        # NOTE: it's not fully clear, if it's a ref or a backref if `choice` `maxOccurs="unbounded" always, or only
        #  when there's only one element inside
        # if we only have one ref element and if it's inside a choice/sequence (this node) which is maxOccurs = unbounded then it's array
        # if XSDReader.is_array(node) and len(node) == 1:
        if XSDReader.is_array(node):
            is_array = True
        if XSDReader.is_array(referenced_element):
            is_array = True
        if is_array:
            property_type = "backref"
        else:
            property_type = "ref"
        referenced_model_names = self._create_model(
            referenced_element,
            source_path=new_source_path,
            parent_model=model,
        )
        for referenced_model_name in referenced_model_names:
            property_id, prop = model.simple_element_to_property(referenced_element, is_array=is_array)
            prop["external"]["name"] = prop["external"]["name"].replace("/text()", '')
            if is_array:
                if not property_id.endswith("[]"):
                    property_id += "[]"
                property_type = "backref"
            prop["type"] = property_type
            prop["model"] = f"{referenced_model_name}"
            properties[property_id] = prop

        return properties

    def _properties_from_type_references(
        self,
        node: _Element,
        model: XSDModel,
        source_path: str = ""
    ) -> dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]:

        properties = {}

        for typed_element in node.xpath('./*[local-name() = "element"]'):

            new_source_path = source_path

            if typed_element.xpath(f'./*[local-name() = "complexType"]') \
                    or self._node_has_separate_complex_type(typed_element):

                if typed_element.xpath(f'./*[local-name() = "complexType"]'):
                    complex_type = typed_element.xpath(f'./*[local-name() = "complexType"]')[0]
                if self._node_has_separate_complex_type(typed_element):
                    complex_type = self._get_separate_complex_type_node(typed_element)

                is_array = False

                referenced_element = typed_element

                # avoiding recursion
                if referenced_element.get("name") in source_path.split("/"):
                    continue

                built_properties = self._build_ref_properties(complex_type, is_array, model, new_source_path, node,
                                                              referenced_element)
                properties.update(built_properties)

        return properties

    def _properties_from_references(
        self,
        node: _Element,
        model: XSDModel,
        source_path: str = ""
    ) -> dict[str, dict[str, str | bool | dict[str, str | dict[str, Any]]]]:

        properties = {}

        for ref_element in node.xpath("./*[@ref]"):
            new_source_path = source_path
            referenced_element = self._get_referenced_node(ref_element)

            # avoiding recursion
            if referenced_element.get("name") in source_path.split("/"):
                continue

            if self.node_is_simple_type_or_inline(referenced_element):
                is_array = XSDReader.is_array(ref_element)
                property_id, prop = model.simple_element_to_property(referenced_element, is_array=is_array)
                if not XSDReader.is_required(ref_element):
                    prop["required"] = False
                properties[property_id] = prop
            else:
                is_array = False

                if XSDReader.is_array(ref_element):
                    is_array = True

                if XSDReader.is_array(node):
                    is_array = True

                complex_type = referenced_element.xpath("./*[local-name() = 'complexType']")[0]

                built_properties = self._build_ref_properties(complex_type, is_array, model, new_source_path, node, referenced_element)
                for prop in built_properties.values():
                    if not XSDReader.is_required(ref_element):
                        prop["required"] = False
                properties.update(built_properties)

        return properties

    def _split_choice(
        self,
        node: _Element,
        source_path: str,
        parent_model: XSDModel,
        is_root_model: bool = False
    ) -> list[str]:
        """
        If there are choices in the element,
        we need to split it and create a separate model per each choice
        """

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
                    returned_model_names = self._create_model(
                        node_copy,
                        source_path=source_path,
                        parent_model=parent_model,
                        is_root_model=is_root_model
                    )

                    model_names.extend(returned_model_names)

                    for node_in_choice in choice_copy:
                        node_in_choice = choice_node_parent.xpath(f"./*[@name=\'{node_in_choice.get('name')}\']")[0]
                        choice_node_parent.remove(node_in_choice)
                else:
                    choice_node_parent.insert(0, choice)
                    returned_model_names = self._create_model(
                        node_copy,
                        source_path=source_path,
                        parent_model=parent_model,
                        is_root_model=is_root_model
                    )
                    model_names.extend(returned_model_names)

                    choice_node_parent.remove(choice)
        return model_names

    def _create_model(
        self,
        node: _Element,
        source_path: str = "",
        is_root_model: bool = False,
        parent_model: XSDModel = None,
    ) -> list[str]:
        """
        Parses an element and makes a model out of it. If it is a complete model, it will be added to the models list.
        """
        model = XSDModel(self)
        model.parent_model = parent_model

        # properties of this model
        properties = {}
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
            if choices and choices[0].get("maxOccurs") != "unbounded":
                return self._split_choice(
                    node,
                    source_path=source_path,
                    parent_model=parent_model,
                    is_root_model=is_root_model
                )

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

            if (
                complex_type_node.xpath(f'./*[local-name() = "sequence"]') or
                complex_type_node.xpath(f'./*[local-name() = "all"]') or
                complex_type_node.xpath(f'./*[local-name() = "simpleContent"]') or
                choices or
                len(complex_type_node) > 0
            ):
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

                properties.update(model.properties_from_simple_elements(
                    sequence_or_all_node,
                    properties_required=properties_required))

                # references
                properties_from_references = self._properties_from_references(
                    sequence_or_all_node,
                    model=model,
                    source_path=new_source_path)
                if properties_required is False:
                    for prop in properties_from_references.values():
                        prop["required"] = False
                properties.update(properties_from_references)

                # complex type child nodes - to models
                properties_from_references = self._properties_from_type_references(
                    sequence_or_all_node,
                    model=model,
                    source_path=new_source_path)
                if properties_required is False:
                    for prop in properties_from_references.values():
                        prop["required"] = False
                properties.update(properties_from_references)

        model.properties = properties

        if properties:

            model.add_external_info(external_name=new_source_path)
            model.description = self.get_description(node)
            self.models[model.name] = model

            return [model.name, ]

        return []

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
            self.models[resource_model.name] = resource_model

    def _parse_root_node(self):
        for node in self.root:
            if (
                    self._is_element(node) and
                    (not self.node_is_simple_type_or_inline(node) or self.node_is_ref(node)) and
                    not self._node_is_referenced(node)
            ):
                self._create_model(node, is_root_model=True)

    def _extract_namespaces(self):
        self.namespaces = self.root.nsmap

    def _add_refs_for_backrefs(self):
        for model in self.models.values():
            for property_id, prop in model.properties.items():
                if prop["type"] == "backref":
                    referenced_model = self.models[prop["model"]]
                    # checking if the ref already exists.
                    # They can exist multiple times, but refs should be added only once
                    prop_added = False
                    for prop in referenced_model.properties.values():
                        if "model" in prop and prop["model"] == model.name:
                            prop_added = True
                    if not prop_added:
                        referenced_model.add_ref_property(model)

    def _sort_properties_alphabetically(self):
        for model in self.models.values():
            model.properties = dict(sorted(model.properties.items()))

    def _remove_sources_from_secondary_models(self):
        """
        Only models which represent root elements for XML need to have source
        """

        for parsed_model in self.models.values():

            # we need to add root properties to properties if it's a root model
            if parsed_model.parent_model is not None and parsed_model.parent_model.name in self.models:
                parsed_model.external["name"] = ""

    def _remove_duplicate_models(self):
        """removes models that are exactly the same"""

        do_loop = True

        do_not_remove = []

        while do_loop:

            model_pairs = {}

            for model_name, model in self.models.items():
                for another_model_name, another_model in self.models.items():
                    if model is not another_model and model == another_model and another_model_name not in do_not_remove:
                        if (
                            another_model_name not in model_pairs.values() and
                            another_model_name not in model_pairs
                        ):
                            model_pairs[another_model_name] = model_name

            for another_model_name, model_name in model_pairs.items():
                parent_model = self.models[another_model_name].parent_model
                if parent_model and parent_model.name in self.models:
                    for property_id, prop in parent_model.properties.items():
                        if "model" in prop and prop["model"] == another_model_name:
                            prop["model"] = model_name
                    self.models.pop(another_model_name)
                else:
                    do_not_remove.append(another_model_name)
                do_not_remove.append(model_name)

            if not model_pairs:
                do_loop = False
            else:
                print(self.dataset_name)
                for old_model, new_model in model_pairs.items():
                    print(f"{old_model.split('/')[1]} -> {new_model.split('/')[1]}")

    def start(self):
        self._extract_root()

        # preparation part

        self._extract_namespaces()
        self._extract_custom_simple_types(self.root)

        # main part

        self._add_resource_model()

        self._parse_root_node()

        # models transformations

        self._remove_unneeded_models()

        self.models = dict(sorted(self.models.items()))

        self._remove_duplicate_models()

        self._compile_nested_properties()

        self._remove_sources_from_secondary_models()

        self._add_refs_for_backrefs()

        self._sort_properties_alphabetically()

    def remove_extra_root_models(self, model: XSDModel) -> XSDModel:
        """
        removes root models that have only one property from the root
        """
        stop_removing = False

        while not stop_removing:
            # remove the model itself if it's a root proxy model
            if (len(model.properties) == 1) and (list(model.properties.values())[0]["type"] in ("ref", "backref")):
                model = self.models[list(model.properties.values())[0]["model"]]
                model.parent_model = None
            else:
                stop_removing = True

        return model

    def _remove_proxy_models(self, model: XSDModel):
        """ Removes models which have only one property
            Usually these are proxy models to indicate arrays, but there can be other situations
            Removes the models that are in the middle of other models
            or at the end and have one property, then this property is joined to the referring model.
        """

        self.new_models[model.name] = model

        new_properties = {}
        for property_id, prop in model.properties.items():
            if prop["type"] in ("ref", "backref"):
                referee = self.models[prop["model"]]
                parse_referee = True
                while len(referee.properties) == 1:
                    ref_property_id, ref_prop = list(referee.properties.items())[0]

                    # if it's not a ref, this means that it's a final property, and we add it as a property itself
                    if ref_prop["type"] not in ("ref", "backref"):
                        prop["external"]["name"] = f'{prop["external"]["name"]}/{ref_prop["external"]["name"]}'

                        # also transfer all attributes of the property
                        prop["required"] = ref_prop["required"]
                        if ref_prop["description"]:
                            prop["description"] = ref_prop["description"]

                        is_array = False
                        if prop["type"] == "backref":
                            is_array = True

                        prop["type"] = ref_prop["type"]
                        del prop["model"]
                        property_id = ref_property_id

                        if is_array:
                            property_id = f"{property_id}[]"

                        parse_referee = False
                        break

                    if prop["type"] == "backref" and ref_prop["type"] == "backref":
                        # basically, do nothing
                        break
                    else:
                        referee = self.models[ref_prop["model"]]
                        referee.parent_model = model
                        if prop["type"] == "ref" and ref_prop["type"] == "backref":
                            prop["type"] = "backref"
                            property_id = f"{property_id}[]"
                        if "external" in prop and "external" in ref_prop:
                            prop["external"]["name"] = f'{prop["external"]["name"]}/{ref_prop["external"]["name"]}'
                            prop["model"] = ref_prop["model"]


                if not self._has_backref(model, referee) and parse_referee:
                    self._remove_proxy_models(referee)

            new_properties[property_id] = prop
        model.properties = new_properties

    def _remove_unneeded_models(self):
        """
        Proxy models are those that have only one property which is a ref to another model.
        They can act as placeholders, or as array indicators.
        If either one of them is an array, drop the proxy model and replace the reference to point to the new model
        If both referencing properties are not arrays, the resulting model shouldn't be an array, and if any of them is an array, the resulting ref is an array (backref).
        If both models, the referrer and the referee are arrays, do not drop them, because this means that it's an array of arrays.
        """
        self.new_models = {}
        for model_name, model in self.models.items():

            # we need to start from root models
            if model.parent_model is None:
                model = self.remove_extra_root_models(model)
                self._remove_proxy_models(model)

        self.models = self.new_models

    def _has_backref(self, model: XSDModel, ref_model: XSDModel) -> bool:
        has_backref = False
        for ref_model_property in ref_model.properties.values():
            if (ref_model_property.get("type") == "backref") and (ref_model_property.get('model') == model.name):
                has_backref = True
        return has_backref

    def _add_model_nested_properties(self, root_model: XSDModel, model: XSDModel, property_prefix: str = "", source_path: str = ""):
        """recursively gather nested properties or root model"""
        # go forward, add property prefix, which is constructed rom properties that came rom beore models, and construct pathh orward also
        # probably will need to cut beginning for path sometimes

        source_path = source_path.lstrip("/")

        root_properties = {}

        properties = deepcopy(model.properties)

        for property_id, prop in properties.items():

            if (model != root_model and
                    not ("model" in prop and prop["model"] == model.parent_model.name)):

                # update property source and name and add it to the root properties
                if property_prefix:
                    property_id = f"{property_prefix}.{property_id}"

                if "external" in prop and source_path:
                    prop["external"]["name"] = f"{source_path}/{prop['external']['name']}"

                root_properties[property_id] = prop

            # if this property is ref or backref, gather the properties of the model to which it points
            # (if it's not the root model or back pointing ref)
            if "model" in prop:
                ref_model = self.models[prop['model']]

                # there are two types of ref - direct, and for backref. We don't want to traverse the ones or backref,
                # because it will create an infinite loop.
                # If it's a ref, we need to build the path from the root of the model. If it's backref (array) -
                # it's relative to array
                if prop["type"] == "ref":
                    if self._has_backref(model, ref_model):
                        continue
                    if source_path:
                        new_source_path = f"{source_path}/{ref_model.external['name'].replace(model.external['name'], '').lstrip('/')}"
                    else:
                        new_source_path = ref_model.external['name'].replace(model.external['name'], '')
                    new_source_path = new_source_path.lstrip('/')

                # property type is backref
                else:
                    new_source_path = ""

                self._add_model_nested_properties(root_model, ref_model, property_prefix=property_id, source_path=new_source_path)

        root_model.properties.update(root_properties)

    def _compile_nested_properties(self):
        for parsed_model in self.models.values():

            # we need to add root properties to properties if it's a root model
            if parsed_model.parent_model is None or parsed_model.parent_model.name not in self.models:

                self._add_model_nested_properties(parsed_model, parsed_model)


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


            c) element has a choice.

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

    -----------Nested properties-------------

    Root model or models can have nested properties if they have any properties that point to other models.

    """
    xsd = XSDReader(path, dataset_name)

    xsd.start()

    yield None, xsd.dataset_and_resource_info

    for model_name, parsed_model in xsd.models.items():

        yield None, parsed_model.get_data()
