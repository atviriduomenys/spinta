import os

from lxml.etree import _Element

from spinta.components import Context
from lxml import etree, objectify
from urllib.request import urlopen
from pprint import pprint

from spinta.utils.naming import to_property_name, to_model_name, Deduplicator


class XSDReader:

    def __init__(self, path, dataset_name):
        self._path = path
        self.models = []
        self._custom_types = {}
        self._dataset_given_name = dataset_name
        self._set_dataset_and_resource_info()


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
        "gYearMonth": "date;ref:M",
        "gYear": "date;ref:Y",
        "gMonthDay": "string",
        "gDay": "string",
        "gMonth": "string",
        "hexBinary": "string",
        "base64Binary": "string",
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

    """
    format of custom types:
    {
        "type_name": {
            "base": "type_base"
        }
    }
    """

    deduplicate_model_name = Deduplicator()
    deduplicate_property_name = Deduplicator()

    def _get_model_external_info(self, name) -> dict:
        external = {
            "dataset": self.dataset_name,
            "resource": "resource1",
            "name": name
        }
        return external

    def _extract_custom_types(self, node: _Element) -> dict:
        custom_types_nodes = node.xpath(f'./*[local-name() = "simpleType"]')
        custom_types = {}
        for type_node in custom_types_nodes:
            type_name = type_node.get("name")
            restrictions = node.xpath(f'./*[local-name() = "simpleType"]/*[local-name() = "restriction"]')
            property_type_base = restrictions[0].get("base", "")
            property_type_base = property_type_base.split(":")[1]

            custom_types[type_name] = {
                "base": property_type_base
            }
        return custom_types

    def _get_text_property(self):
        return {
            'text': {
                'type': 'string',
                'external': {
                    'name': 'text()',
                }
            }}

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
        self.dataset_name = os.path.splitext(os.path.basename(self._path))[0]
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

    def _get_description(self, element: etree.Element) -> str:
        annotation = element.xpath(f'./*[local-name() = "annotation"]')
        description = ""
        if annotation:
            documentation = annotation[0].xpath(f'./*[local-name() = "documentation"]')
            for documentation_part in documentation:
                if documentation_part.text is not None:
                    description = f"{description}{documentation_part.text} "
        return description.strip()

    def _get_separate_complex_type_node(self, node):
        # todo move this to a separate function
        node_type = node.get('type')
        if node_type is not None:
            node_type = node_type.split(":")
            if len(node_type) > 1:
                node_type = node_type[1]
            else:
                node_type = node_type[0]
        if node_type not in self.DATATYPES_MAPPING:
            complex_types = self.root.xpath(f'./*[local-name() = "complexType"]')
            for node in complex_types:
                if node.get("name") == node_type:
                    return node

    def _node_has_separate_complex_type(self, node: _Element):
        node_type = node.get('type')
        if node_type is not None:
            node_type = node_type.split(":")
            if len(node_type) > 1:
                node_type = node_type[1]
            else:
                node_type = node_type[0]
            if node_type not in self.DATATYPES_MAPPING:
                complex_types = self.root.xpath(f'./*[local-name() = "complexType"]')
                for node in complex_types:
                    if node.get("name") == node_type:
                        return True
        return False

    def _node_is_simple_type_or_inline(self, node: _Element):
        if self._node_has_separate_complex_type(node):
            return False
        return bool(
            (node.xpath(f'./*[local-name() = "annotation"]') and len(node.getchildren()) == 1) or
            (node.xpath(f'./*[local-name() = "simpleType"]')) or
            (len(node.getchildren()) == 0)
        )

    def _node_is_ref(self, node):
        if node.get("ref"):
            return True
        return False

    def _properties_from_simple_elements(self, node: _Element, from_sequence: bool = True):
        properties = {}
        elements = node.xpath(f'./*[local-name() = "element"]')
        for element in elements:
            if self._node_is_simple_type_or_inline(element) and not self._node_is_ref(element):
                property_id, prop = self._simple_element_to_property(element)
                if not from_sequence:
                    prop["required"] = False
                properties[property_id] = prop
        return properties

    def _get_enums(self, node: _Element):
        enums = {}
        enum_value = {}
        restrictions = node.xpath(f'./*[local-name() = "simpleType"]/*[local-name() = "restriction"]')
        if restrictions:
            # can be enum
            enumerations = restrictions[0].xpath('./*[local-name() = "enumeration"]')
            for enumeration in enumerations:
                enum_item = {
                    "source": enumeration.get("value")
                }
                enum_value.update( {enumeration.get("value"): enum_item})
            enums[""] = enum_value
        return enums

    def _get_property_type(self, node: _Element) -> str:
        if node.get("ref"):
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

        if property_type in self._custom_types:
            property_type = self._custom_types.get(property_type).get("base", "")
        if property_type in self.DATATYPES_MAPPING:
            property_type = self.DATATYPES_MAPPING[property_type]
        else:
            property_type = "string"

        return property_type

    def _node_to_partial_property(self, node: etree.Element) -> tuple[str, dict]:
        """Node can be either element or attribute.
        This function only processes things common to attributes and elements"""
        prop = dict()

        prop["description"] = self._get_description(node)
        property_name = node.get("name")
        prop["external"] = {"name": property_name}
        property_id = to_property_name(property_name)
        prop["type"] = self._get_property_type(node)
        # todo prepare for base64binary
        if ";" in prop["type"]:
            prop["ref"] = prop["type"].split(";")[1].split(":")[1]
            prop["type"] = prop["type"].split(";")[0]
        prop["enums"] = self._get_enums(node)

        return property_id, prop

    def _attributes_to_properties(self, element: etree.Element) -> dict:
        properties = {}
        attributes = element.xpath(f'./*[local-name() = "attribute"]')
        complex_type = element.xpath(f'./*[local-name() = "complexType"]')
        if complex_type:
            properties.update(self._attributes_to_properties(complex_type[0]))
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

        # todo attribute can be a ref to an externally defined attribute also

        return properties

    def _is_array(self, element: _Element) -> bool:
        max_occurs = element.get("maxOccurs", 1)
        return max_occurs == "unbounded" or int(max_occurs) > 1

    def _is_element(self, node):
        if node.xpath('local-name()') == "element":
            return True
        return False

    def _is_required(self, element: _Element) -> bool:
        min_occurs = int(element.get("minOccurs", 1))
        if min_occurs > 0:
            return True
        return False

    def _simple_element_to_property(self, element: _Element):
        # simple element is an element which is either
        # an inline or simple type element and doesn't have a ref

        property_id, prop = self._node_to_partial_property(element)
        if self._node_is_ref(element):
            prop["external"]["name"] = element.get("ref")
            property_id = to_property_name(element.get("ref"))
        prop["external"]["name"] = f'{prop["external"]["name"]}/text()'
        if prop.get("type") == "":
            prop["type"] = "string"
        if self._is_array(element):
            property_id += "[]"
        if self._is_required(element):
            prop["required"] = True
        else:
            prop["required"] = False
        return property_id, prop

    """
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

    def _properties_from_references(self, node: _Element, model_name: str, source_path: str = ""):
        properties = {}
        for ref_element in node.xpath("./*[@ref]"):

            ref = ref_element.get("ref")
            if ":" in ref:
                ref = ref.split(":")[1]
            xpath_query = f"//*[@name='{ref}']"
            referenced_element = self.root.xpath(xpath_query)[0]

            if self._node_is_simple_type_or_inline(referenced_element):
                property_id, prop = self._simple_element_to_property(referenced_element)
                if not self._is_required(ref_element):
                    prop["required"] = False
                properties[property_id] = prop
            else:
                if not self._is_array(ref_element):
                    referenced_model_names = self._create_model(referenced_element, source_path)
                    property_type = "ref"
                else:
                    referenced_element_properties = {
                        to_property_name(model_name):
                            {
                                "type": "ref",
                                "model": f"{self.dataset_name}/{model_name}"
                            }
                    }
                    property_type = "backref"
                    referenced_model_names = self._create_model(referenced_element, source_path,
                                                                additional_properties=referenced_element_properties)

                for referenced_model_name in referenced_model_names:
                    property_id, prop = self._simple_element_to_property(ref_element)
                    if "[]" in property_id:
                        property_id = self.deduplicate_property_name(property_id.split("[")[0]) + "[]"

                    prop["external"]["name"] = ""
                    prop["type"] = property_type
                    prop["model"] = f"{self.dataset_name}/{referenced_model_name}"
                    properties[property_id] = prop

        return properties

    def _create_model(self, node: _Element, source_path: str = "", additional_properties: dict = None, choice: int = None) -> list:
        """
        Parses an element. If it is a complete model, it will be added to the models list.
        """
        final = False

        if additional_properties is None:
            additional_properties = {}

        properties = additional_properties
        new_source_path = f"{source_path}/{node.get('name')}"

        model_name = self.deduplicate_model_name(to_model_name(node.get("name")))

        description = self._get_description(node)
        properties.update(self._attributes_to_properties(node))

        if properties:
            final = True

        if node.xpath(f'./*[local-name() = "complexType"]') or self._node_has_separate_complex_type(node):

            if self._node_has_separate_complex_type(node):
                complex_type_node = self._get_separate_complex_type_node(node)
            else:
                complex_type_node = node.xpath(f'./*[local-name() = "complexType"]')[0]
            # if complextype node's property mixed is true, it allows text inside
            if complex_type_node.get("mixed") == "true":
                final = True
                properties.update(self._get_text_property())

            choices = None
            if complex_type_node.xpath(f'./*[local-name() = "choice"]'):
                complex_type_node = complex_type_node.xpath(f'./*[local-name() = "choice"]')[0]
                choices = complex_type_node.xpath(f'./*[local-name() = "sequence"]')

                # if we get choice, this means we are already in one of the split models
                # if we don't get choices, this means that we are in the initial model
                # and need to create a model for each choice
                # this is only for the case where each choice has sequence in it. It's the most common one.
                # todo cover other cases

            if choices and choice is None:
                model_names = []
                for index, choice in enumerate(choices):
                    # create options of this model
                    model_names = self._create_model(node, source_path, choice=index, additional_properties=additional_properties)
                    model_names.extend(model_names)
                return model_names

            if complex_type_node.xpath(f'./*[local-name() = "sequence"]') or complex_type_node.xpath(f'./*[local-name() = "all"]'):
                sequence_or_all_nodes = complex_type_node.xpath(f'./*[local-name() = "sequence"]')
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
                if sequence_or_all_nodes:
                    sequence_or_all_node = sequence_or_all_nodes[0]
                else:
                    sequence_or_all_node = complex_type_node.xpath(f'./*[local-name() = "all"]')[0]
                if choice is not None:
                    sequence_or_all_node = choices[choice]
                sequence_or_all_node_length = len(sequence_or_all_node)
                # There is only one element in the complex node sequence, and it doesn't have annotation.
                # Then we just go deeper and add this model to the next model's path.
                if sequence_or_all_node_length == 1 and not final:
                    # todo final is also decided by maxOccurs

                    if sequence_or_all_node.xpath(f'./*[local-name() = "element"]'):
                        # todo, cover option, where choice is inside sequence(437, line 49)
                        if not sequence_or_all_node.xpath(f'./*[local-name() = "element"]')[0].get("ref"):
                            element = sequence_or_all_node.xpath(f'./*[local-name() = "element"]')[0]

                            # check for recursion
                            # todo maybe move this to a separate function
                            paths = new_source_path.split("/")
                            if not element.get("name") in paths:
                                self._create_model(element, source_path=new_source_path)
                            else:
                                for index, path in enumerate(paths):
                                    if path == element.get("name"):
                                        paths[index] = f"/{path}"
                                new_source_path = "/".join(paths)

                        else:
                            # todo if reference is to an inline or simpleType element,
                            #  and maxOccurs of it is 1,
                            #  then do not create reference, but add to the same

                            properties.update(
                                self._properties_from_references(sequence_or_all_node, model_name, new_source_path))
                            final = True
                elif sequence_or_all_node_length > 1 or final:
                    # properties from simple type or inline elements without references
                    properties.update(self._properties_from_simple_elements(sequence_or_all_node))

                    # references
                    properties.update(
                        self._properties_from_references(sequence_or_all_node, model_name, new_source_path))

                    # complex type child nodes - to models
                    for child_node in sequence_or_all_node:
                        if child_node.xpath(f'./*[local-name() = "complexType"]') \
                                or self._node_has_separate_complex_type(child_node):
                            # check for recursion
                            # todo maybe move this to a separate function
                            paths = new_source_path.split("/")
                            if not child_node.get("name") in paths:
                                self._create_model(child_node, source_path=new_source_path)
                            else:
                                for index, path in enumerate(paths):
                                    if path == child_node.get("name"):
                                        paths[index] = f"/{path}"
                                new_source_path = "/".join(paths)

                    final = True
                else:
                    final = True

            else:
                final = True

        else:
            final = True

        if final:
            model = {
                "type": "model",
                "description": description,
                "properties": properties,
                "name": model_name,
                "external": self._get_model_external_info(name=new_source_path),
            }
            model["name"] = f'{model["external"]["dataset"]}/{model["name"]}'
            self.models.append(model)

        return [model_name, ]

    def _get_resource_model(self):
        resource_model_external_info = self._get_model_external_info(name="/")
        self.resource_model = {
            "type": "model",
            "name": "",
            "description": "Įvairūs duomenys",
            "properties": {},
            "external": resource_model_external_info,
            "uri": "http://www.w3.org/2000/01/rdf-schema#Resource"
        }
        self.resource_model["properties"] = self._properties_from_simple_elements(self.root, from_sequence=False)
        if self.resource_model["properties"]:
            self.resource_model["name"] = self.deduplicate_model_name(f"{self.dataset_name}/Resource")
            self.models.append(self.resource_model)

    def _parse_root_node(self):
        # get properties from elements

        # Resource model - special case

        for node in self.root:
            if self._is_element(node) and (not self._node_is_simple_type_or_inline(node) or self._node_is_ref(node)):
                self._create_model(node)

            # todo complexContent is also an option.
            # todo there is also an option where complex type
            #  is on the same level as element, referenced by type.
            #  This already works in some cases, but in more complex cases (like 914) it doesn't

    def start(self):
        self._extract_root()
        self._custom_types = self._extract_custom_types(self.root)
        self._get_resource_model()

        self._parse_root_node()


def read_schema(context: Context, path: str, prepare: str = None, dataset_name: str = '') -> dict:
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

        pprint(parsed_model)

        yield None, parsed_model
