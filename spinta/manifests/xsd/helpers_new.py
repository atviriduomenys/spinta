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
    properties: dict = {}
    uri: str | None = None
    description: str | None = None
    root_properties: dict | None = None
    parent_model: XSDModel | None = None
    is_root_model: bool | None = None
    element_name: str | None = None
    complex_type_name: str | None = None


class XSDReader:

    def __init__(self, path, dataset_name: str):
        self._path: str = path
        self.models: dict[str, XSDModel] = {}
        self.custom_types: dict = {}
        self._dataset_given_name: str = dataset_name
        self._set_dataset_and_resource_info()
        self.deduplicate: Deduplicator = Deduplicator()

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


    def _register_element_model(self, node):
        """The node that comes here has a complexType inside"""
        complex_type_node = node.xpath(f'./*[local-name() = "complexType"]')[0]
        model = self._register_complex_type_model(complex_type_node)
        model.set_name(self.deduplicate(to_model_name(node.get("name"))))
        model.element_name = node.get("name")
        model.properties.update(model.attributes_to_properties(node))

    def _register_complex_type_model(self, node):
        model = XSDModel(self)
        model.complex_type_name = node.get("name")



        model.properties.update(model.attributes_to_properties(node))
        model.properties.update(model.properties_from_simple_elements(node))
        model.properties.update(model._properties_from_references(node))
        model.properties.update(model.properties_from_tyope_references(node))
        return model

        # for node



    #         itertree pagooglint

    def _register_global_models(self):
        for node in self.root:
            if (
                    self._is_element(node) and
                    not self.node_is_simple_type_or_inline(node)
            ):
                self._register_element_model(node)

            elif self._is_complex_type(node):
                self._register_complex_type_model(node)




    def start(self):
        self._extract_root()

        # preparation part

        self._extract_custom_simple_types(self.root)

        # main part

        self._add_resource_model()

        self._register_global_models()

        # self._parse_root_node()

        # models transformations

        self._remove_unneeded_models()

        self.models = dict(sorted(self.models.items()))

        self._remove_duplicate_models()

        self._compile_nested_properties()

        self._remove_sources_from_secondary_models()

        self._add_refs_for_backrefs()

        self._sort_properties_alphabetically()


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



