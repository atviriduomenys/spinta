from __future__ import annotations

import enum
from dataclasses import dataclass

from spinta.components import Context, Property, Model
from spinta.core.enums import Visibility
from spinta.manifests.yaml.components import InlineManifest
from spinta.types.datatype import Ref, BackRef, ArrayBackRef, Inherit, Array
from spinta.utils.naming import to_model_name


MERMAID_CONFIG = """---
config:
  theme: base
  themeVariables:
    mainBkg: '#ffffff00'
    clusterBkg: '#ffffde'
---
"""


class MermaidClassDef(enum.Enum):
    Concept = "stroke:#8FB58F,fill:#F0FDF0,color:#000000"
    Entity = "stroke:#9D8787,fill:#F5E8DF,color:#000000"

    def __str__(self) -> str:
        return f"classDef {self.name} {self.value};"


class MermaidClassDiagram:
    namespaces: dict[str | None, MermaidNameSpace]
    relationships: set[MermaidRelationship]

    def __init__(self) -> None:
        self.namespaces = {}
        self.relationships = set()

    def __str__(self) -> str:
        text = f"{MERMAID_CONFIG}\n"
        text += "classDiagram\n"

        for namespace in self.namespaces.values():
            text += f"{namespace}\n"

        for relationship in sorted(self.relationships, key=lambda r: (r.node1, r.node2)):
            text += f"{str(relationship)}\n"

        for definition in MermaidClassDef:
            text += f"{definition}\n"

        return text

    def get_namespace(self, name: str | None) -> MermaidNameSpace:
        if name not in self.namespaces:
            self.namespaces[name] = MermaidNameSpace(self, name)
        return self.namespaces[name]

    def add_relationship(self, relationship: MermaidRelationship) -> None:
        self.relationships.add(relationship)


class MermaidNameSpace:
    mermaid: MermaidClassDiagram
    name: str | None
    classes: dict[str, MermaidClass]

    def __init__(self, mermaid: MermaidClassDiagram, name: str | None = None) -> None:
        self.name = name
        self.mermaid = mermaid
        self.classes = {}

    def __str__(self) -> str:
        text = f"namespace `{self.name}` {{\n" if self.name else ""
        for mermaid_class in self.classes.values():
            text += f"{mermaid_class}\n"
        text += "}" if self.name else ""
        return text

    def get_or_create_class(self, name: str, **kwargs) -> MermaidClass:
        if name not in self.classes:
            self.classes[name] = MermaidClass(name=name, **kwargs)
        return self.classes[name]

    def process_property(
        self, model: Model, prop: Property, update: bool = True, mermaid_class: MermaidClass | None = None
    ) -> None:
        mermaid_class = mermaid_class or self.get_or_create_class(name=model.name, label=model.basename)
        if prop.enum:
            enum_class = self.get_or_create_class(
                name=f"{model.name}{to_model_name(prop.name)}",
                label=to_model_name(prop.name),
                definition=MermaidClassDef.Concept,
                is_enum=True,
            )

            self.mermaid.add_relationship(
                MermaidRelationship(
                    node1=model.name,
                    node2=enum_class.name,
                    required=prop.dtype.required,
                    type=RelationshipType.DEPENDENCY,
                    label=prop.name,
                )
            )

            for enum_property in prop.enum:
                mermaid_property = MermaidProperty(name=enum_property.strip('"'))
                enum_class.add_property(mermaid_property, update)
        elif isinstance(prop.dtype, (Array, ArrayBackRef)):
            if prop.dtype.items and isinstance(prop.dtype.items.dtype, (Ref, BackRef)):
                self.mermaid.add_relationship(
                    MermaidRelationship(
                        node1=model.name,
                        node2=prop.dtype.items.dtype.model.name,
                        required=prop.dtype.required,
                        many=True,
                        type=RelationshipType.ASSOCIATION,
                        label=prop.name,
                    )
                )
                for nested_prop in prop.dtype.items.dtype.properties.values():
                    self.process_property(prop.dtype.items.dtype.model, nested_prop, update=False)
            else:
                type_ = prop.dtype.items.dtype.name if prop.dtype.items else "?"
                visibility = prop.dtype.items.visibility if prop.dtype.items else prop.visibility
                mermaid_class.add_property(
                    MermaidProperty(
                        name=prop.name, visibility=visibility, type=type_, required=prop.dtype.required, many=True
                    ),
                    update,
                )

        elif isinstance(prop.dtype, (Ref, BackRef)):
            self.mermaid.add_relationship(
                MermaidRelationship(
                    node1=model.name,
                    node2=prop.dtype.model.name,
                    required=prop.dtype.required,
                    type=RelationshipType.ASSOCIATION,
                    label=prop.name,
                )
            )
            for nested_prop in prop.dtype.properties.values():
                self.process_property(prop.dtype.model, nested_prop, update=False)

        # If it's a property that already is in a `base` model, we don't add it
        elif isinstance(prop.dtype, Inherit):
            return

        else:
            mermaid_class.add_property(
                MermaidProperty(
                    name=prop.name, visibility=prop.visibility, type=prop.dtype.name, required=prop.dtype.required
                ),
                update,
            )


class MermaidClass:
    name: str
    label: str
    definition: MermaidClassDef
    properties: dict[str, MermaidProperty]
    is_enum: bool

    def __init__(
        self, name: str, label: str, definition: MermaidClassDef = MermaidClassDef.Entity, is_enum: bool = False
    ) -> None:
        self.name = name
        self.label = label
        self.definition = definition
        self.properties = {}
        self.is_enum = is_enum

    def add_property(self, prop: MermaidProperty, update: bool = False) -> None:
        if update or prop.name not in self.properties:
            self.properties[prop.name] = prop

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
            class_text += "«optional»\n" if not self.is_enum else ""
            class_text += "".join(f"{str(prop)}\n" for prop in optional_properties)

        class_text += "}"
        return class_text

    @property
    def optional_properties(self) -> list[MermaidProperty]:
        return [prop for prop in self.properties.values() if not prop.required]

    @property
    def mandatory_properties(self) -> list[MermaidProperty]:
        return [prop for prop in self.properties.values() if prop.required]


@dataclass
class MermaidProperty:
    name: str
    visibility: Visibility | None = None
    type: str | None = None
    required: bool | None = None
    many: bool = False

    VISIBILITY_MAPPING = {
        Visibility.public: "+",
        Visibility.package: "~",
        Visibility.protected: "#",
        Visibility.private: "-",
    }

    def __str__(self) -> str:
        property_string = ""

        if self.visibility is not None:
            visibility = self.VISIBILITY_MAPPING[self.visibility]
            property_string += f"{visibility} "

        property_string += self.name

        if self.type is not None:
            property_string = f"{property_string} : {self.type}"

        if self.required is not None:
            property_string = f"{property_string} [{int(self.required)}..{'*' if self.many else 1}]"

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
    _ARROWS = {
        (RelationshipDirection.FORWARD, RelationshipType.ASSOCIATION): "-->",
        (RelationshipDirection.FORWARD, RelationshipType.DEPENDENCY): "..>",
        (RelationshipDirection.FORWARD, RelationshipType.INHERITANCE): "--|>",
        (RelationshipDirection.BACKWARD, RelationshipType.ASSOCIATION): "<--",
        (RelationshipDirection.BACKWARD, RelationshipType.DEPENDENCY): "<..",
        (RelationshipDirection.BACKWARD, RelationshipType.INHERITANCE): "<|--",
    }
    node1: str
    node2: str
    type: RelationshipType
    direction: RelationshipDirection = RelationshipDirection.FORWARD
    required: bool | None = None
    many: bool = False
    label: str = ""

    def __str__(self) -> str:
        arrow = self._ARROWS[(self.direction, self.type)]

        if self.required is not None:
            cardinality = f' "[{int(self.required)}..{"*" if self.many else 1}]"'
        else:
            cardinality = ""

        relationship_text = f"`{self.node1}` {arrow}{cardinality} `{self.node2}`"
        if self.label:
            relationship_text += f" : {self.label}"
            relationship_text += "<br/>«mandatory»" if self.required else "<br/>«optional»"

        return relationship_text

    def __hash__(self):
        return hash((self.node1, self.node2))


def write_mermaid_manifest(
    context: Context,
    manifest: InlineManifest,
    main_dataset: str | None = None,
    output: str | None = None,
) -> str | None:
    """Generate a Mermaid class diagram from the given manifest.

    If `output` is specified, writes the diagram to that file and returns None.
    Otherwise returns the diagram as a string.
    """
    mermaid = MermaidClassDiagram()

    models: dict[str, Model] = manifest.get_objects()["model"]
    for model in models.values():
        dataset_name = model.external.dataset.name
        namespace_name = dataset_name if dataset_name != main_dataset else None
        namespace = mermaid.get_namespace(namespace_name)
        mermaid_class = namespace.get_or_create_class(name=model.name, label=model.basename)

        if model.base:
            label = ", ".join(pk.name for pk in model.base.pk)
            mermaid.add_relationship(
                MermaidRelationship(
                    node1=model.name,
                    node2=model.base.parent.name,
                    type=RelationshipType.INHERITANCE,
                    label=label,
                )
            )

        for model_property in model.get_given_properties().values():
            namespace.process_property(model, model_property, mermaid_class)

    if not output:
        return str(mermaid)

    with open(output, "w") as file:
        file.write(str(mermaid))
