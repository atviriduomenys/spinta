from __future__ import annotations

import enum
from dataclasses import dataclass

from spinta.components import Context
from spinta.core.enums import Access
from spinta.manifests.yaml.components import InlineManifest
from spinta.types.datatype import Ref, BackRef, ArrayBackRef, Inherit, PartialArray
from spinta.utils.naming import to_model_name


class MermaidClassDiagram:
    title: str | None
    classes: list[MermaidClass]
    relationships: list[MermaidRelationship]

    def __init__(self, title: str = None):
        self.title = title
        self.classes = []
        self.relationships = []

    def __str__(self):
        text = ""

        if self.title:
            text += f"---\n{self.title}\n---\n"

        text += "classDiagram\n"

        for clas in self.classes:
            text += f"{str(clas)}\n"

        for relationship in self.relationships:
            text += f"{str(relationship)}\n"

        return text

    def add_class(self, clas: MermaidClass) -> None:
        self.classes.append(clas)

    def add_relationship(self, relationship: MermaidRelationship) -> None:
        self.relationships.append(relationship)


class MermaidClass:
    name: str
    properties: list[MermaidProperty]

    def __init__(self, name: str, is_enum=False):
        self.name = name
        self.properties = []
        self.is_enum = is_enum

    def add_property(self, prop: MermaidProperty) -> None:
        self.properties.append(prop)

    def __str__(self):
        class_text = f"class {self.name} {{\n"
        if self.is_enum:
            class_text = f"{class_text}<<enumeration>>\n"
        properties_text = "".join(f"{str(prop)}\n" for prop in self.properties)
        class_text += properties_text
        class_text += "}"
        return class_text


@dataclass
class MermaidProperty:
    name: str
    access: Access | None = None
    type: str | None = None
    cardinality: bool | None = None
    multiplicity: str | None = None

    ACCESS_MAPPING = {
        Access.open: '+',
        Access.public: '#',
        Access.protected: '~',
        Access.private: '-'
    }

    def __str__(self):

        property_string = self.name

        if self.access is not None:
            access = self.ACCESS_MAPPING[self.access]
            property_string = f"{access} {property_string}"

        if self.type is not None:
            property_string = f"{property_string} : {self.type}"

        if self.cardinality or self.multiplicity:
            property_string = f"{property_string} [{int(self.cardinality) if self.cardinality is not None else ''}..{self.multiplicity if self.multiplicity is not None else ''}]"

        return property_string


class RelationshipDirection(enum.Enum):
    FORWARD = 'forward'
    BACKWARD = 'backward'


class RelationshipType(enum.Enum):
    ASSOCIATION = 'association'
    DEPENDENCY = 'dependency'
    INHERITANCE = 'inheritance'


@dataclass
class MermaidRelationship:
    node1: str
    node2: str
    type: RelationshipType
    direction: RelationshipDirection = RelationshipDirection.FORWARD
    cardinality: bool = False
    multiplicity: str = ""
    label: str = ""

    def __str__(self):

        if self.direction == RelationshipDirection.FORWARD:
            if self.type == RelationshipType.ASSOCIATION:
                arrow = '-->'
            if self.type == RelationshipType.DEPENDENCY:
                arrow = '..>'
            if self.type == RelationshipType.INHERITANCE:
                arrow = '--|>'
        else:
            if self.type == RelationshipType.ASSOCIATION:
                arrow = '<--'
            if self.type == RelationshipType.DEPENDENCY:
                arrow = '<..'
            if self.type == RelationshipType.INHERITANCE:
                arrow = '<|--'

        if self.cardinality or self.multiplicity:
            cardinality_multiplicity = f' "[{int(self.cardinality)}..{self.multiplicity}]"'
        else:
            cardinality_multiplicity = ""

        relationship_text = f"{self.node1} {arrow}{cardinality_multiplicity} {self.node2}"
        if self.label:
            relationship_text += f" : {self.label}"

        return relationship_text


def write_mermaid_manifest(context: Context, output: str, manifest: InlineManifest):

    mermaids = []

    for dataset_name, dataset in manifest.get_objects()["dataset"].items():
        mermaid = MermaidClassDiagram(title=dataset_name)

        models = manifest.get_objects()["model"]
        for model in models.values():
            model_dataset_name = model.external.dataset.name
            if model_dataset_name == dataset_name:
                mermaid_class = MermaidClass(name=model.basename)
                for model_property in model.get_given_properties().values():

                    if model_property.enum:
                        enum_class = MermaidClass(name=f"{model.basename}{to_model_name(model_property.name)}", is_enum=True)
                        for enum_property in model_property.enum:
                            enum_property = MermaidProperty(name=enum_property.strip('"'))
                            enum_class.add_property(enum_property)
                        mermaid.add_class(enum_class)
                        mermaid.add_relationship(
                            MermaidRelationship(
                                node1=mermaid_class.name,
                                node2=enum_class.name,
                                cardinality=True,
                                multiplicity="1",
                                type=RelationshipType.DEPENDENCY,
                                label=model_property.name
                            ))
                        
                    elif (
                        isinstance(model_property.dtype, Ref)
                        or isinstance(model_property.dtype, BackRef)
                    ):
                        multiplicity = '1'
                        mermaid.add_relationship(
                            MermaidRelationship(
                                node1=mermaid_class.name,
                                node2=model_property.dtype.model.basename,
                                cardinality=model_property.dtype.required,
                                multiplicity=multiplicity,
                                type=RelationshipType.ASSOCIATION,
                                label=model_property.name
                            )
                        )

                    # If it's a property that already is in a `base` model, we don't add it
                    elif isinstance(model_property.dtype, Inherit):
                        continue

                    elif (
                        isinstance(model_property.dtype, PartialArray)
                        or isinstance(model_property.dtype, ArrayBackRef)
                    ):
                        multiplicity = '*'

                        if hasattr(model_property.dtype, "items"):
                            if hasattr(model_property.dtype.items.dtype, "model"):
                                mermaid.add_relationship(
                                    MermaidRelationship(
                                        node1=mermaid_class.name,
                                        node2=model_property.dtype.items.dtype.model.basename,
                                        cardinality=model_property.dtype.items.dtype.required,
                                        multiplicity=multiplicity,
                                        type=RelationshipType.ASSOCIATION,
                                        label=model_property.name
                                    )
                                )
                            else:
                                mermaid_property = MermaidProperty(
                                    name=model_property.name,
                                    access=model_property.access,
                                    type=model_property.dtype.items.dtype.name,
                                    cardinality=model_property.dtype.items.dtype.required,
                                    multiplicity=multiplicity,
                                )
                                mermaid_class.add_property(mermaid_property)
                        else:

                            mermaid_property = MermaidProperty(
                                name=model_property.name,
                                access=model_property.access,
                                type=model_property.dtype.name,
                                cardinality=model_property.dtype.required,
                                multiplicity=multiplicity,
                            )
                            mermaid_class.add_property(mermaid_property)

                    else:

                        multiplicity = '1'

                        mermaid_property = MermaidProperty(
                            name=model_property.name,
                            access=model_property.access,
                            type=model_property.dtype.name,
                            cardinality=model_property.dtype.required,
                            multiplicity=multiplicity,
                        )
                        mermaid_class.add_property(mermaid_property)

                mermaid.add_class(mermaid_class)

                if model.base:
                    labels = []
                    for pk_property in model.base.pk:
                        labels.append(pk_property.name)
                    label = ", ".join(labels)
                    mermaid.add_relationship(
                        MermaidRelationship(
                            node1=mermaid_class.name,
                            node2=model.base.basename,
                            type=RelationshipType.INHERITANCE,
                            label=label
                        )
                    )

        mermaids.append(mermaid)

    with open(output, 'w') as file:

        for mermaid in mermaids:

            file.write(str(mermaid))
