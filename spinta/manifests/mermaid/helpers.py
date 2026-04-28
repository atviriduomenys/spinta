from __future__ import annotations

import enum
from dataclasses import dataclass

from spinta.components import Context
from spinta.core.enums import Visibility
from spinta.manifests.yaml.components import InlineManifest
from spinta.types.datatype import Ref, BackRef, ArrayBackRef, Inherit, PartialArray
from spinta.utils.naming import to_model_name


CONCEPT_STYLES = "stroke:#8FB58F,fill:#F0FDF0,color:#000000"
ENTITY_STYLES = "stroke:#9D8787,fill:#F5E8DF,color:#000000"

CONCEPT_NAME = "Concept"
ENTITY_NAME = "Entity"

MERMAID_CONFIG = """---
config:
  theme: base
  themeVariables:
    mainBkg: '#ffffff00' 
    clusterBkg: '#ffffde'
---
"""


class MermaidClassDiagram:
    namespaces: dict[str | None, MermaidNameSpace]
    relationships: list[MermaidRelationship]
    definitions: set[MermaidClassDef]

    def __init__(self) -> None:
        self.namespaces = {}
        self.relationships = []
        self.definitions = set()

    def __str__(self) -> str:
        text = f"{MERMAID_CONFIG}\n"
        text += "classDiagram\n"

        for namespace in self.namespaces.values():
            text += f"{str(namespace)}\n"

        for relationship in self.relationships:
            text += f"{str(relationship)}\n"

        for definition in sorted(self.definitions, key=lambda d: d.name):
            text += f"{str(definition)}\n"

        return text

    def add_namespace(self, name: str | None) -> MermaidNameSpace:
        if name not in self.namespaces:
            self.namespaces[name] = MermaidNameSpace(name)
        return self.namespaces[name]

    def add_relationship(self, relationship: MermaidRelationship) -> None:
        self.relationships.append(relationship)

    def collect_definitions(self) -> None:
        self.definitions = {cls.definition for namespace in self.namespaces.values() for cls in namespace.classes}


class MermaidNameSpace:
    name: str | None
    classes: list[MermaidClass]

    def __init__(self, name: str | None = None) -> None:
        self.name = name
        self.classes = []

    def __str__(self) -> str:
        text = f"namespace `{self.name}` {{\n" if self.name else ""
        for mermaid_class in self.classes:
            text += f"{str(mermaid_class)}\n"
        text += "}" if self.name else ""
        return text

    def add_class(self, mermaid_class: MermaidClass) -> None:
        self.classes.append(mermaid_class)


@dataclass(eq=False)
class MermaidClassDef:
    name: str
    styles: str

    def __str__(self) -> str:
        return f"classDef {self.name} {self.styles};"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, MermaidClassDef) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)


class MermaidClass:
    name: str
    label: str
    definition: MermaidClassDef
    properties: list[MermaidProperty]
    is_enum: bool

    def __init__(self, name: str, label: str, definition: MermaidClassDef, is_enum: bool = False) -> None:
        self.name = name
        self.label = label
        self.definition = definition
        self.properties = []
        self.is_enum = is_enum

    def add_property(self, prop: MermaidProperty) -> None:
        self.properties.append(prop)

    def __str__(self) -> str:
        class_text = f'class `{self.name}`["{self.label}"]:::{self.definition.name}'

        if not self.properties:
            return class_text

        class_text += " {\n"

        if self.is_enum:
            class_text = f"{class_text}<<enumeration>>\n"

        if mandatory_properties := self.mandatory_properties:
            class_text += "«mandatory»\n"
            class_text += "".join(f"{str(prop)}\n" for prop in mandatory_properties)

        if optional_properties := self.optional_properties:
            class_text += "«optional»\n"
            class_text += "".join(f"{str(prop)}\n" for prop in optional_properties)

        class_text += "}"
        return class_text

    @property
    def optional_properties(self) -> list[MermaidProperty]:
        return [prop for prop in self.properties if not prop.cardinality]

    @property
    def mandatory_properties(self) -> list[MermaidProperty]:
        return [prop for prop in self.properties if prop.cardinality]


@dataclass
class MermaidProperty:
    name: str
    visibility: Visibility | None = None
    type: str | None = None
    cardinality: bool | None = None
    multiplicity: str | None = None

    VISIBILITY_MAPPING = {
        Visibility.public: "+",
        Visibility.package: "~",
        Visibility.protected: "#",
        Visibility.private: "-",
    }

    def __str__(self) -> str:
        property_string = self.name

        if self.visibility is not None:
            visibility = self.VISIBILITY_MAPPING[self.visibility]
            property_string = f"{visibility} {property_string}"

        if self.type is not None:
            property_string = f"{property_string} : {self.type}"

        if self.cardinality or self.multiplicity:
            property_string = f"{property_string} [{int(self.cardinality) if self.cardinality is not None else ''}..{self.multiplicity if self.multiplicity is not None else ''}]"

        return property_string


class RelationshipDirection(enum.Enum):
    FORWARD = "forward"
    BACKWARD = "backward"


class RelationshipType(enum.Enum):
    ASSOCIATION = "association"
    DEPENDENCY = "dependency"
    INHERITANCE = "inheritance"


@dataclass
class MermaidRelationship:
    node1: str
    node2: str
    type: RelationshipType
    direction: RelationshipDirection = RelationshipDirection.FORWARD
    cardinality: bool = False
    multiplicity: str = ""
    label: str = ""

    def __str__(self) -> str:
        if self.direction == RelationshipDirection.FORWARD:
            if self.type == RelationshipType.ASSOCIATION:
                arrow = "-->"
            elif self.type == RelationshipType.DEPENDENCY:
                arrow = "..>"
            elif self.type == RelationshipType.INHERITANCE:
                arrow = "--|>"
            else:
                raise ValueError("Unknown relationship type.")
        else:
            if self.type == RelationshipType.ASSOCIATION:
                arrow = "<--"
            elif self.type == RelationshipType.DEPENDENCY:
                arrow = "<.."
            elif self.type == RelationshipType.INHERITANCE:
                arrow = "<|--"
            else:
                raise ValueError("Unknown relationship type.")

        if self.cardinality or self.multiplicity:
            cardinality_multiplicity = f' "[{int(self.cardinality)}..{self.multiplicity}]"'
        else:
            cardinality_multiplicity = ""

        relationship_text = f"`{self.node1}` {arrow}{cardinality_multiplicity} `{self.node2}`"
        if self.label:
            relationship_text += f" : {self.label}"
            relationship_text += "<br/>«mandatory»" if self.cardinality else "<br/>«optional»"

        return relationship_text


def write_mermaid_manifest(
    context: Context, output: str, manifest: InlineManifest, main_dataset: str | None = None
) -> None:
    mermaid = MermaidClassDiagram()
    concept_definition = MermaidClassDef(name=CONCEPT_NAME, styles=CONCEPT_STYLES)
    entity_definition = MermaidClassDef(name=ENTITY_NAME, styles=ENTITY_STYLES)

    models = manifest.get_objects()["model"]
    for model in models.values():
        dataset_name = model.external.dataset.name
        namespace_name = dataset_name if dataset_name != main_dataset else None
        namespace = mermaid.add_namespace(namespace_name)

        mermaid_class = MermaidClass(name=model.name, label=model.basename, definition=entity_definition)
        for model_property in model.get_given_properties().values():
            if model_property.enum:
                enum_class = MermaidClass(
                    name=f"{model.name}{to_model_name(model_property.name)}",
                    label=to_model_name(model_property.name),
                    definition=concept_definition,
                    is_enum=True,
                )
                for enum_property in model_property.enum:
                    mermaid_property = MermaidProperty(name=enum_property.strip('"'))
                    enum_class.add_property(mermaid_property)
                namespace.add_class(enum_class)
                mermaid.add_relationship(
                    MermaidRelationship(
                        node1=mermaid_class.name,
                        node2=enum_class.name,
                        cardinality=True,
                        multiplicity="1",
                        type=RelationshipType.DEPENDENCY,
                        label=model_property.name,
                    )
                )

            elif isinstance(model_property.dtype, Ref) or isinstance(model_property.dtype, BackRef):
                multiplicity = "1"
                mermaid.add_relationship(
                    MermaidRelationship(
                        node1=mermaid_class.name,
                        node2=model_property.dtype.model.name,
                        cardinality=model_property.dtype.required,
                        multiplicity=multiplicity,
                        type=RelationshipType.ASSOCIATION,
                        label=model_property.name,
                    )
                )

            # If it's a property that already is in a `base` model, we don't add it
            elif isinstance(model_property.dtype, Inherit):
                continue

            elif isinstance(model_property.dtype, PartialArray) or isinstance(model_property.dtype, ArrayBackRef):
                multiplicity = "*"

                if hasattr(model_property.dtype, "items"):
                    if hasattr(model_property.dtype.items.dtype, "model"):
                        mermaid.add_relationship(
                            MermaidRelationship(
                                node1=mermaid_class.name,
                                node2=model_property.dtype.items.dtype.model.name,
                                cardinality=model_property.dtype.items.dtype.required,
                                multiplicity=multiplicity,
                                type=RelationshipType.ASSOCIATION,
                                label=model_property.name,
                            )
                        )
                    else:
                        mermaid_property = MermaidProperty(
                            name=model_property.name,
                            visibility=model_property.dtype.items.visibility,
                            type=model_property.dtype.items.dtype.name,
                            cardinality=model_property.dtype.items.dtype.required,
                            multiplicity=multiplicity,
                        )
                        mermaid_class.add_property(mermaid_property)
                else:
                    mermaid_property = MermaidProperty(
                        name=model_property.name,
                        visibility=model_property.visibility,
                        type=model_property.dtype.name,
                        cardinality=model_property.dtype.required,
                        multiplicity=multiplicity,
                    )
                    mermaid_class.add_property(mermaid_property)

            else:
                multiplicity = "1"

                mermaid_property = MermaidProperty(
                    name=model_property.name,
                    visibility=model_property.visibility,
                    type=model_property.dtype.name,
                    cardinality=model_property.dtype.required,
                    multiplicity=multiplicity,
                )
                mermaid_class.add_property(mermaid_property)

        namespace.add_class(mermaid_class)

        if model.base:
            label = ", ".join(pk.name for pk in model.base.pk)
            mermaid.add_relationship(
                MermaidRelationship(
                    node1=mermaid_class.name,
                    node2=model.base.parent.name,
                    type=RelationshipType.INHERITANCE,
                    label=label,
                )
            )

    mermaid.collect_definitions()

    with open(output, "w") as file:
        file.write(str(mermaid))
