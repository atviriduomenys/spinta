from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, List
from urllib.request import urlopen

from lxml import etree, objectify
from lxml.etree import _Element, QName

from spinta.components import Context
from spinta.core.ufuncs import Expr
from spinta.utils.naming import Deduplicator, to_dataset_name, to_model_name, to_property_name

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
    xsd_type: str | None = None
    name: str
    enum: str | None = None
    enums: dict[str, dict[str, dict[str, str]]] | None = None
    prepare: Expr | None = None
    description: str = ""

    def __init__(self, name: str = None):
        self.name = name

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
    name: str
    xsd_name: str
    source: str  # converts to ["external"]["name"]
    type: XSDType
    required: bool | None = None
    unique: bool | None = None
    is_array: bool | None = None
    ref_model: XSDModel | None = None
    description: str = ""
    uri: str | None = None
    xsd_ref_to: str | None = None  # if it is a ref to another model, here is it's xsd element name
    xsd_type_to: str | None = None  # if it's type is of another complexType, so ref to that type

    def __init__(
            self,
            required: bool = False,
            enums: dict[str, dict[str, str]] | None = None,
            xsd_name: str | None = None,
            property_type: XSDType | None = None,
            source: str | None = None,
            is_array: bool = False,
    ):
        self.required = required
        self.enums = {}
        self.xsd_name = xsd_name
        self.type = property_type
        self.required = required
        self.source = source
        self.is_array = is_array

    def get_data(self) -> dict[str, Any]:
        data = {
            "type": self.type.name,
            "external":
                {
                    "name": self.source,
                }
        }

        if self.type.name == "ref" or self.type.name == "backref":
            data["model"] = self.ref_model.name
            data["external"]["prepare"] = f'expand()'

        if self.required is not None:
            data["required"] = self.required
        if self.unique is not None:
            data["unique"] = True

        if self.type.prepare is not None:
            if "prepare" in data["external"]:
                data["external"]["prepare"] += f";{self.type.prepare}"
            else:
                data["external"]["prepare"] = self.type.prepare

        if self.type.enums is not None:
                data["enums"] = self.type.enums

        if self.type.description:
            self.description += f" - {self.type.description}"

        data["description"] = self.description

        property_name = self.name

        if self.is_array:
            property_name = f"{property_name}[]"

        return {property_name: data}


class XSDModel:
    dataset_resource: XSDDatasetResource
    xsd_name: str | None = None
    name: str | None = None
    basename: str | None = None
    source: str  # converts to ["external"]["name"]
    properties: dict[str, XSDProperty]
    uri: str | None = None
    description: str | None = None
    referred_from: list[tuple[XSDModel, str]] | None = None  # tuple - model, property id
    is_root_model: bool = False
    deduplicate_property_name: Deduplicator
    xsd_node_type: str | None = None  # from complexType or from element
    models_by_ref: str | None = None
    extends_model: XSDModel | None = None

    def __init__(self, dataset_resource) -> None:
        self.properties = {}
        self.deduplicate_property_name = Deduplicator()
        self.dataset_resource = dataset_resource
        self.source = ""

    def set_name(self, name: str):
        self.basename = name
        self.name = f"{self.dataset_resource.dataset_name}/{name}"

    def get_data(self):
        model_data: dict = {
            "type": "model",
            "name": self.name,
            "external":
                {
                    "name": self.source,
                    "dataset": self.dataset_resource.dataset_name,
                    "resource": self.dataset_resource.resource_name,
                }

        }
        if self.description is not None:
            model_data["description"] = self.description
        if self.properties is not None:
            properties = {}
            for prop_name, prop in self.properties.items():
                properties.update(prop.get_data())
            model_data["properties"] = properties
        if self.uri is not None:
            model_data["uri"] = self.uri

        if self.extends_model:
            model_data["external"]["prepare"] = f'extends("{self.extends_model.basename}")'

        return model_data


@dataclass
class XSDDatasetResource:
    dataset_name: str | None = None
    resource_name: str | None = None
    dataset_given_name: str | None = None

    def __init__(self, dataset_name: str = None, resource_name: str = "resource1", dataset_given_name: str | None = None):
        self.dataset_name = dataset_name
        self.resource_name = resource_name
        self.dataset_given_name = dataset_given_name

    def get_data(self):
        return {
            'type': 'dataset',
            'name': self.dataset_name,
            'resources': {
                self.resource_name: {
                    'type': 'xml',
                },
            },
            'given_name': self.dataset_given_name
        }


def _is_array(node: _Element) -> bool:
    return node.attrib.get("maxOccurs", 1) == "unbounded" or int(node.attrib.get("maxOccurs", 1)) > 1

class XSDReader:
    dataset_resource: XSDDatasetResource
    models: list[XSDModel]
    _path: str
    root: _Element
    deduplicate_model_name: Deduplicator
    custom_types: dict[str, XSDType] | None = None
    top_level_element_models: dict[str, XSDModel]
    top_level_complex_type_models: dict[str, XSDModel]

    def __init__(self, path: str, dataset_name) -> None:
        self._path = path
        self.dataset_resource = XSDDatasetResource(dataset_given_name=dataset_name, resource_name="resource1")
        self.custom_types = {}
        self.models = []
        self.deduplicate_model_name = Deduplicator()
        self.top_level_element_models = {}
        self.top_level_complex_type_models = {}

    def register_simple_types(self, state: State) -> None:
        custom_types_nodes = self.root.xpath(f'./*[local-name() = "simpleType"]')
        for node in custom_types_nodes:
            custom_type = self.process_simple_type(node, state)
            self.custom_types[custom_type.xsd_type] = custom_type

    def _extract_root(self):
        if self._path.startswith("http"):
            document = etree.parse(urlopen(self._path))
            objectify.deannotate(document, cleanup_namespaces=True)
            self.root = document.getroot()
        else:
            path = self._path.split("://")[-1]
            with open(path) as file:
                text = file.read()
                self.root = etree.fromstring(bytes(text, encoding='utf-8'))

    def _create_resource_model(self):
        self.resource_model = XSDModel(dataset_resource=self.dataset_resource)
        self.resource_model.type = "model"
        self.resource_model.source = "/"
        self.resource_model.description = "Įvairūs duomenys"
        self.resource_model.uri = "http://www.w3.org/2000/01/rdf-schema#Resource"
    #     resource model will be added to models at the end, if it has any peoperties

    def _post_process_resource_model(self):
        if self.resource_model.properties:
            self.resource_model.set_name(self.deduplicate_model_name(f"Resource"))
            self.models.append(self.resource_model)

    def _post_process_refs(self):
        """
        Links properties in all models to their target models based on xsd_ref_to and xsd_type_to.
        Also links models to their base models based on the 'prepare' attribute (extend statements).
        """
        for model in self.models:
            for prop in model.properties.values():
                if prop.xsd_ref_to:
                    try:
                        prop.ref_model = self.top_level_element_models[prop.xsd_ref_to]
                    except KeyError:
                        raise KeyError(f"Reference to a non-existing model: {prop.xsd_ref_to}")
                    
                elif prop.xsd_type_to:
                    try:
                        prop.ref_model = self.top_level_complex_type_models[prop.xsd_type_to]
                    except KeyError:
                        raise KeyError(f"Reference to a non-existing model: {prop.xsd_type_to}")

            if model.extends_model:
                # Assume the prepare statement is in the format 'extend("BaseType")'
                try:
                    extends_model: XSDModel = self.top_level_complex_type_models[model.extends_model]
                except KeyError:
                    raise KeyError(f"Parent model '{extends_model}' not found for model '{model.name}'")

                if extends_model.properties or extends_model.extends_model:
                    model.extends_model = extends_model
                else:
                    model.extends_model = None

    def _add_refs_for_backrefs(self):
        for model in self.models:
            for property_id, prop in model.properties.items():
                if prop.type.name == "backref":
                    referenced_model = prop.ref_model
                    # checking if the ref already exists.
                    # They can exist multiple times, but refs should be added only once
                    prop_added = False
                    for prop in referenced_model.properties.values():
                        if prop.ref_model and prop.ref_model == model:
                            prop_added = True
                    if not prop_added:
                        prop = XSDProperty()
                        prop.name = to_property_name(model.name)
                        prop.type = XSDType(name="ref")
                        prop.ref_model = model
                        referenced_model.properties[prop.name] = prop

    def _sort_alphabetically(self):
        self.models = sorted(self.models, key=lambda model: model.name)

        for model in self.models:
            model.properties = dict(sorted(model.properties.items()))

    def start(self):
        # general part
        state = State()
        self._extract_root()
        dataset_name = to_dataset_name(os.path.splitext(os.path.basename(self._path))[0])
        self.dataset_resource.dataset_name = dataset_name

        # registering custom simpleType

        self.register_simple_types(state)

        # reading XML and registering nodes

        self._create_resource_model()

        # reading XSD and registering models and properties

        state = State()

        self.register_simple_types(state)

        # todo maybe add a function to check attributes of xml nodes

        self.process_root(state)

        # post processing

        self._post_process_refs()

        self._add_refs_for_backrefs()

        # we need to add this here, because only now we will know if it has properties and if we need to create it
        self._post_process_resource_model()

        self._sort_alphabetically()

    def process_root(self, state: State):
        # todo add sources to models later, when we know that they are not referenced
        #  by properties from other models.
        for node in self.root.getchildren():
            # We don't care about comments
            if isinstance(node, etree._Comment):
                continue
            if QName(node).localname == "element":
                # todo add task for this and finish this
                properties = self.process_element(node, state, is_root=True)
                for prop in properties:
                    if prop.type.name not in ("ref", "backref"):
                        prop.name = self.resource_model.deduplicate_property_name(to_property_name(prop.xsd_name))
                        self.resource_model.properties[prop.name] = prop
            elif QName(node).localname == "complexType":
                models = self.process_complex_type(node, state)
                for model in models:
                    model.is_root_model = True
            elif QName(node).localname == "simpleType":
                # simple types are processed in self.register_simple_types
                pass
            else:
                raise RuntimeError(f'This node type cannot be at the top level: {QName(node).localname}')

    #  XSD nodes processors
    def process_element(self, node: _Element, state: State, is_array=False, is_root=False) -> list[XSDProperty]:
        """
        Element should return a property. It can return multiple properties if there is a choice somewhere down the way.
        property name is set after returning all properties, because we need to do a deduplication first.
        """
        # todo add more explanatory comments
        is_array = is_array or _is_array(node)
        is_required = int(node.attrib.get("minOccurs", 1)) > 0
        props = []
        property_type_to = None
        models = []

        # ref - a reference to separately defined element
        if node.attrib.get("ref"):
            property_name = node.attrib["ref"]
            property_ref_to = property_name
            if is_array:
                property_type = XSDType(name="backref")
            else:
                property_type = XSDType(name="ref")
            prop = XSDProperty(xsd_name=property_name, property_type=property_type, required=is_required,
                               source=property_name, is_array=is_array)
            prop.xsd_ref_to = property_ref_to
            return [prop]

        elif node.attrib.get("name"):
            property_name = node.attrib["name"]
        else:
            raise RuntimeError(f'Element has to have either name or ref')

        if not (node.xpath('./*[local-name()="complexType"]') or node.xpath('./*[local-name()="simpleType"]')):
            # XSD rules are that element should have type, but life's not always what we want it to be
            if node.attrib.get("type"):
                element_type = node.attrib["type"].split(":")[-1]
            else:
                element_type = "string"

            # separately defined simpleType
            if element_type in self.custom_types:
                property_type = self.custom_types[element_type]

            # inline type
            elif element_type in DATATYPES_MAPPING:
                property_type = XSDType()
                property_type.name = DATATYPES_MAPPING[element_type]

            # separately defined complexType
            else:
                property_type = XSDType()
                property_type_to = element_type
                property_type.name = "backref" if is_array else "ref"

            if property_type.name in ("ref", "backref"):
                source = property_name
            else:
                source = f"{property_name}/text()"

            prop = XSDProperty(xsd_name=property_name, property_type=property_type, required=is_required, source=source, is_array=is_array)
            prop.xsd_type_to = property_type_to
            props.append(prop)


        # else:
        #     if not node.attrib.get("type") and not node.xpath('./*[local-name() = ""]'):
        #         raise RuntimeError(f'Element has to have either ref or type')

        for child in node.getchildren():
            # We don't care about comments
            if isinstance(child, etree._Comment):
                continue
            if QName(child).localname == "complexType":
                models = self.process_complex_type(child, state)
                # usually it's one model, but in case of choice, can be multiple models
                for model in models:
                    if property_name != model.xsd_name:
                        model.xsd_name = property_name
                        model.set_name(self.deduplicate_model_name(to_model_name(property_name)))
                    if is_root:
                        self.top_level_element_models[property_name] = model
                        model.is_root_model = True
                        model.source = f"/{property_name}"
                    prop = XSDProperty(xsd_name=property_name, required=is_required, source=property_name, is_array=is_array)
                    prop.ref_model = model
                    if is_array:
                        property_type = "backref"
                    else:
                        property_type = "ref"
                    prop.type = XSDType()
                    prop.type.name = property_type

                    props.append(prop)
            elif QName(child).localname == "simpleType":
                source = f"{property_name}/text()"
                prop = XSDProperty(xsd_name=property_name, required=is_required, source=source, is_array=is_array)
                prop.type = self.process_simple_type(child, state)
                props.append(prop)

            elif QName(child).localname == "annotation":
                description = self.process_annotation(child, state)

            else:
                raise RuntimeError(f"This node type cannot be in the element: {QName(node).localname}")

            if "description" in locals() and description:
                for prop in props:
                    prop.description = description

            if "description" in locals() and description:
                for model in models:
                    model.description = f"{model.description}{description}"

        if not props:
            raise RuntimeError(f"Element couldn't be turned into a property: {node.get('name') or node.get('ref')}")

        return props

        # todo there is a case in RC with a type name that doesn't exist (or exists externally)
        #  todo factor in minoccurs and maxoccurs everywhere
        # todo If it's top level, we need to know if we need to add it to the resource model or not.
        #  Maybe after we return from this, we need to check if the property is `ref`. If it's top level and not ref, we add it to the "resource" model
        # todo decide where to deal with placeholder elements, which are not turned into a model
        #  talk to Mantas if a model is considered a placeholder model if it has only one ref or even if it has more refs but nothing else
        # todo handle unique (though it doesn't exist in RC)
    def process_complex_type(self, node: _Element, state: State) -> list[XSDModel]:

        # preparation for building model
        models = []
        name = node.attrib.get("name")

        property_groups = [[]]

        if node.attrib.get("mixed", "false") == "true":
            text_prop = XSDProperty(
                xsd_name="text",
                required=False,
                source="text()",
                is_array=False
            )
            for group in property_groups:
                group.append(text_prop)

        # collecting data from child elements
        for child in node.getchildren():
            # We don't care about comments
            if isinstance(child, etree._Comment):
                continue

            local_name = QName(child).localname

            if local_name == "attribute":
                prop: XSDProperty = self.process_attribute(child, state)
                for group in property_groups:
                    group.append(prop)

            elif local_name == "sequence":
                sequence_property_groups: list[list[XSDProperty]] = self.process_sequence(child, state)
                new_property_groups = []
                for group in property_groups:
                    for seq_group in sequence_property_groups:
                        combined_group = group.copy()
                        combined_group.extend(seq_group)
                        new_property_groups.append(combined_group)
                property_groups = new_property_groups

            elif local_name == "choice":
                choice_property_groups: list[list[XSDProperty]] = self.process_choice(child, state)
                new_property_groups = []
                for group in property_groups:
                    for choice_group in choice_property_groups:
                        combined_group = group.copy()
                        combined_group.extend(choice_group)
                        new_property_groups.append(combined_group)
                property_groups = new_property_groups

            elif local_name == "complexContent":
                complex_content_property_groups: List[List[XSDModel]] = self.process_complex_content(child, state)
                new_property_groups = []
                for group in property_groups:
                    for cc_group in complex_content_property_groups:
                        combined_group = group.copy()
                        combined_group.extend(cc_group)
                        new_property_groups.append(combined_group)
                property_groups = new_property_groups

                if hasattr(state, 'extends_model'):
                    extends_model: str = state.extends_model
                    del state.extends_model
                else:
                    prepare_statement = None

            elif local_name == "all":
                all_properties: list[XSDProperty] = self.process_all(child, state)
                for group in property_groups:
                    group.extend(all_properties)

            elif local_name == "simpleContent":
                simple_content_properties: List[XSDProperty] = self.process_simple_content(child, state)
                for group in property_groups:
                    group.extend(simple_content_properties)

            elif local_name == "annotation":
                description = self.process_annotation(child, state)

            else:
                raise RuntimeError(f"This node type cannot be in the complex type: {local_name}")

        for group in property_groups:
            model = XSDModel(dataset_resource=self.dataset_resource)
            if "description" in locals() and description:
                model.description = description
            property_deduplicate = Deduplicator()
            for prop in group:
                prop.name = property_deduplicate(to_property_name(prop.xsd_name))
                model.properties[prop.name] = prop

            if name:
                model.xsd_name = name
                model.set_name(self.deduplicate_model_name(to_model_name(name)))
                self.top_level_complex_type_models[model.xsd_name] = model

            if 'extends_model' in locals() and extends_model:
                model.extends_model = extends_model

            models.append(model)

        self.models.extend(models)
        return models

    def _map_type(self, xsd_type: str) -> XSDType:
        """Gets XSD Type, returns DSA type (XSDType class)"""
        xsd_type = xsd_type.split(":")[-1]
        property_type = DATATYPES_MAPPING[xsd_type]
        dsa_type = XSDType()
        if ";" in property_type:
            property_type, target, value = property_type.split(";")
            if target == "enum":
                dsa_type.enum = value
            if target == "prepare":
                dsa_type.prepare = value
        dsa_type.name = property_type
        return dsa_type

    def process_attribute(self, node: _Element, state: State) -> XSDProperty:
        prop = XSDProperty()
        prop.source = f"@{node.attrib.get('name')}"
        prop.xsd_name = node.attrib.get('name')

        attribute_type = node.attrib.get("type").split(":")[-1]

        if attribute_type:
            if attribute_type in DATATYPES_MAPPING:
                prop.type = self._map_type(attribute_type)
            elif attribute_type in self.custom_types:
                prop.type = self.custom_types[attribute_type]
            else:
                raise RuntimeError(f"Unknown attribute type: {attribute_type}")

        if node.attrib.get("use") == "required":
            prop.required = True

        for child in node.getchildren():
            # We don't care about comments
            if isinstance(child, etree._Comment):
                continue
            if QName(child).localname == "simpleType":
                prop.type = self.process_simple_type(child, state)
            elif QName(child).localname == "annotation":
                prop.description = self.process_annotation(child, state)
            else:
                raise RuntimeError(f"Unexpected element type inside attribute element: {QName(child).localname}")
        return prop

    def process_enumeration(self, node: _Element, state: State) -> dict[str, dict[str, str]]:
        enum_value = node.attrib.get("value")
        enum_item = {enum_value: {"source": enum_value}}
        return enum_item

    def process_simple_type(self, node: _Element, state: State) -> XSDType:
        property_type = None
        description = None
        for child in node.getchildren():
            # We don't care about comments
            if isinstance(child, etree._Comment):
                continue
            if QName(child).localname == "restriction":
                # get everything from restriction, just add name if exists
                property_type = self.process_restriction(child, state)
                if node.attrib.get("name"):
                    property_type.xsd_type = node.attrib.get("name")
            elif QName(child).localname == "annotation":
                description = self.process_annotation(child, state)
            else:
                raise RuntimeError(f"Unexpected element type inside simpleType element: {QName(child).localname}")
        property_type.description = description
        return property_type

    def process_sequence(self, node: _Element, state: State) -> list[list[XSDProperty]]:
        """
        Processes an XSD <sequence> element and returns a list of property groups.
        Each group is a list of XSDProperty instances representing a possible combination.
        """
        property_groups = [[]]

        for child in node.getchildren():
            if isinstance(child, etree._Comment):
                continue

            local_name = QName(child).localname

            if local_name == "element":
                props: list[XSDProperty] = self.process_element(child, state)
                for group in property_groups:
                    group.extend(props)
            elif local_name == "choice":
                choice_groups: list[list[XSDProperty]] = self.process_choice(child, state)
                new_property_groups = []
                for group in property_groups:
                    for choice_group in choice_groups:
                        # Create a new group combining the current group with the choice group
                        new_group = group.copy()
                        new_group.extend(choice_group)
                        new_property_groups.append(new_group)
                property_groups = new_property_groups
            else:
                raise RuntimeError(f"Unexpected element type inside <sequence>: {local_name}")

        return property_groups
    
    def process_choice(self, node: _Element, state: State) -> list[list[XSDProperty]]:
        """
        Returns a list of lists. Each list inside the main list is for the separate choice.
        Those lists can also have other lists inside
        """
        choice_groups = []

        if _is_array(node):
            choice_groups: list[list[XSDProperty]] = self.process_sequence(node, state)
            for property_group in choice_groups:
                for prop in property_group:
                    prop.is_array = True
                    if prop.type.name == "ref":
                        prop.type.name = "backref"
            return choice_groups

        for child in node.getchildren():
            if isinstance(child, etree._Comment):
                continue

            local_name = etree.QName(child).localname

            if local_name == "element":
                properties: list[XSDProperty] = self.process_element(child, state)
                choice_groups.append(properties)
            elif QName(child).localname == "sequence":
                nested_groups: list[list[XSDProperty]] = self.process_sequence(child, state)
                choice_groups.extend(nested_groups)
            elif local_name == "choice":
                nested_choice_groups: list[list[XSDProperty]] = self.process_choice(child, state)
                choice_groups.extend(nested_choice_groups)
            else:
                raise RuntimeError(f"Unexpected element type inside <choice>: {local_name}")

        return choice_groups

    def process_group(self, node: _Element, state: State) -> None:
        pass

    def process_all(self, node: _Element, state: State) -> list[XSDProperty]:
        """
        Processes an XSD <all> element, ensuring each child element has minOccurs=0|1 and maxOccurs=1,
        and returns a list of property groups. Since <all> allows any order, this method does not produce combinations.
        """
        properties: list = []

        for child in node.getchildren():
            if isinstance(child, etree._Comment):
                continue

            local_name = QName(child).localname

            if local_name == "element":
                min_occurs = child.attrib.get("minOccurs", "1")
                max_occurs = child.attrib.get("maxOccurs", "1")
                
                if min_occurs not in {"0", "1"} or max_occurs != "1":
                    raise ValueError(
                        f"Invalid occurrence in <all> element: minOccurs='{min_occurs}', maxOccurs='{max_occurs}'"
                    )
                
                props: list[XSDProperty] = self.process_element(child, state)
                properties.extend(props)

            else:
                raise RuntimeError(f"Unexpected element type inside <all>: {local_name}")

        return properties

    def process_simple_content(self, node: _Element, state: State) -> list[XSDProperty]:
        properties: list = []

        for child in node:
            if isinstance(child, etree._Comment):
                continue

            local_name = QName(child).localname

            if local_name == "extension":
                extension_properties: list[XSDProperty] = self.process_simple_type_extension(child, state)
                properties.extend(extension_properties)

            elif local_name == "restriction":
                restriction_prop_type: XSDType = self.process_restriction(child, state)
                prop_name = node.attrib.get("name", "value")
                restriction_prop = XSDProperty(
                    xsd_name=prop_name,
                    property_type=restriction_prop_type,
                )
                properties.append(restriction_prop)

            else:
                raise RuntimeError(f"Unexpected element '{local_name}' in simpleContent")

        return properties

    def process_complex_content(self, node: _Element, state: State) -> list[list[XSDModel]]:
        property_groups: list[list] = [[]]

        for child in node:
            if isinstance(child, etree._Comment):
                continue

            local_name = QName(child).localname

            if local_name == "extension":
                extension_property_groups: list[list[XSDProperty]] = self.process_complex_type_extension(child, state)
                new_property_groups = []
                for group in property_groups:
                    for ext_group in extension_property_groups:
                        combined_group = group.copy()
                        combined_group.extend(ext_group)
                        new_property_groups.append(combined_group)
                property_groups = new_property_groups

            elif local_name == "restriction":
                restriction_prop_type: XSDType = self.process_restriction(child, state)
                prop_name = node.attrib.get("name", "value")
                restriction_prop = XSDProperty(
                    xsd_name=prop_name,
                    property_type=restriction_prop_type,
                )
                for group in property_groups:
                    group.append(restriction_prop)

            elif local_name == "attribute":
                prop = self.process_attribute(child, state)
                for group in property_groups:
                    group.append(prop)

            else:
                raise RuntimeError(f"Unexpected element '{local_name}' in complexContent")

        if node.attrib.get("mixed", "false") == "true":
            property_name = "text"
            text_prop = XSDProperty(
                xsd_name=property_name,
                property_type=XSDType(name="string"),
                required=False,
                source="text()",
                is_array=False
            )
            for group in property_groups:
                group.append(text_prop)

        return property_groups

    def process_annotation(self, node: _Element, state: State) -> str:
        description = ""
        for child in node.getchildren():
            # We don't care about comments
            if isinstance(child, etree._Comment):
                continue
            # appinfo is very app-specific tag, which we don't have possibility to process
            if QName(child).localname == "appinfo":
                continue
            if QName(child).localname == "documentation":
                description += self.process_documentation(child, state)
            else:
                raise RuntimeError(f"Unexpected element type inside annotation element: {QName(child).localname}")
        return description

    def process_documentation(self, node: _Element, state: State) -> str:
        return node.text or ""

    def process_simple_type_extension(self, node: _Element, state: State) -> list[XSDProperty]:
        base = node.attrib.get("base")
        if not base:
            raise RuntimeError("Extension must have a 'base' attribute.")

        properties = []

        base_type_name = base.split(":")[-1]
        base_type: XSDType = self._map_type(base_type_name)
        text_prop = XSDProperty(
            xsd_name="text",
            property_type=base_type,
        )
        properties.append(text_prop)

        for child in node:
            if isinstance(child, etree._Comment):
                continue
            local_name = QName(child).localname
            if local_name == "attribute":
                prop: XSDProperty = self.process_attribute(child, state)
                properties.append(prop)
            elif local_name == "annotation":
                # Add annotation description to text_prop
                text_prop.description = self.process_annotation(child, state)
            else:
                raise RuntimeError(f"Unexpected element '{local_name}' in simpleType extension")

        return properties

    def process_complex_type_extension(self, node: _Element, state: State) -> List[List[XSDProperty]]:
        base = node.attrib.get("base")
        if not base:
            raise RuntimeError("Extension must have a 'base' attribute.")

        base_type_name = base.split(":")[-1]

        property_groups: list[list] = [[]]

        for child in node.getchildren():
            if isinstance(child, etree._Comment):
                continue

            local_name = QName(child).localname

            if local_name == "element":
                prop: List[XSDProperty] = self.process_element(child, state)
                for group in property_groups:
                    group.append(prop)

            elif local_name == "attribute":
                prop: XSDProperty = self.process_attribute(child, state)
                for group in property_groups:
                    group.append(prop)

            elif local_name == "sequence":
                sequence_property_groups: List[List[XSDProperty]] = self.process_sequence(child, state)
                new_property_groups = []
                for group in property_groups:
                    for seq_group in sequence_property_groups:
                        combined_group = group.copy()
                        combined_group.extend(seq_group)
                        new_property_groups.append(combined_group)
                property_groups = new_property_groups

            elif local_name == "choice":
                choice_property_groups: List[List[XSDProperty]] = self.process_choice(child, state)
                new_property_groups = []
                for group in property_groups:
                    for choice_group in choice_property_groups:
                        combined_group = group.copy()
                        combined_group.extend(choice_group)
                        new_property_groups.append(combined_group)
                property_groups = new_property_groups

            else:
                raise RuntimeError(f"Unexpected element '{local_name}' in extension")

        state.extends_model = base_type_name

        return property_groups

    def process_restriction(self, node: _Element, state: State) -> XSDType:

        base = node.attrib.get("base")
        property_type = self._map_type(base)
        enumerations = {}
        for child in node.getchildren():
            # We don't care about comments
            if isinstance(child, etree._Comment):
                continue
            if QName(child).localname == "enumeration":
                enum = self.process_enumeration(child, state)
                enumerations.update(enum)
            elif QName(child).localname == "minInclusive":
                logging.log(logging.INFO, f"met a tag {QName(child).localname}")
            elif QName(child).localname == "maxInclusive":
                logging.log(logging.INFO, f"met a tag {QName(child).localname}")
            elif QName(child).localname == "pattern":
                logging.log(logging.INFO, f"met a tag {QName(child).localname}")
            elif QName(child).localname == "maxLength":
                logging.log(logging.INFO, f"met a tag {QName(child).localname}")
            elif QName(child).localname == "totalDigits":
                logging.log(logging.INFO, f"met a tag {QName(child).localname}")
            else:
                raise RuntimeError(f"Unexpected element type inside restriction element: {child}")
        if enumerations:
            enums = {"": enumerations}
            property_type.enums = enums
        return property_type

    def process_union(self, node: _Element, state: State) -> None:
        pass

    def process_length(self, node: _Element, state: State) -> None:
        pass

    def process_pattern(self, node: _Element, state: State) -> None:
        pass

    def process_max_length(self, node: _Element, state: State) -> None:
        pass

    def process_min_length(self, node: _Element, state: State) -> None:
        pass

    def process_white_space(self, node: _Element, state: State) -> None:
        pass

    def process_total_digits(self, node: _Element, state: State) -> None:
        # raise Exception("Unsupported element")
        # TODO: create specific error
        pass

    def process_fraction_digits(self, node: _Element, state: State) -> None:
        # logging.log(logging.INFO, "met an unsupported type fractionDigits")
        # TODO: configure logger
        pass

    def process_min_inclusive(self, node: _Element, state: State) -> None:
        pass

    def process_max_inclusive(self, node: _Element, state: State) -> None:
        pass

    def process_appinfo(self, node: _Element, state: State) -> None:
        pass

class State:
    path = list[str]
    after_processing_children: bool
    XSD_model_has_properties: bool


def read_schema(
        context: Context,
        path: str,
        prepare: str = None,
        dataset_name: str = ''
) -> dict[Any, dict[str, str | dict[str, str | bool | dict[str, str | dict[str, Any]]]]]:

    xsd = XSDReader(path, dataset_name)

    xsd.start()

    yield None, xsd.dataset_resource.get_data()

    for model in xsd.models:
        yield None, model.get_data()
