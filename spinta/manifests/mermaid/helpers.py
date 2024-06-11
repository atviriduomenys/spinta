from __future__ import annotations

from dataclasses import dataclass

from spinta.components import Context
from spinta.core.enums import Access
from spinta.manifests.yaml.components import InlineManifest


class MermaidClassDiagram:
    title: str | None
    classes: list[MermaidClass] = []
    relationships: list[MermaidRelationship] = []

    def __init__(self, title: str = None):
        self.title = title

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
    properties: list[MermaidProperty] = []

    def __init__(self, name: str):
        self.name = name

    def add_property(self, prop: MermaidProperty) -> None:
        self.properties.append(prop)

    def __str__(self):
        return f'class {self.name} {{\n' + "".join(str(prop) for prop in self.properties) + f'}}'


@dataclass
class MermaidProperty:
    name: str
    access: Access | None = None
    type: str | None = None
    cardinality: bool | None = None
    multiplicity: str | None = None

    ACCESS_MAPPING = {
        Access.open: "+",
        Access.public: "#",
        Access.protected: "~",
        Access.private: "-"
    }

    def __str__(self):
        access = self.ACCESS_MAPPING[self.access]

        return f"{access} {self.name} : {self.type} [{int(self.cardinality)}..{self.multiplicity}]\n"


class MermaidEnum:
    pass


class MermaidRelationship:
    pass


def write_mermaid_manifest(context: Context, output: str, manifest: InlineManifest):

    mermaids = []

    for dataset_name, dataset in manifest.get_objects()["dataset"].items():
        mermaid = MermaidClassDiagram(title=dataset_name)

        models = manifest.get_objects()["model"]
        for model in models.values():
            model_dataset_name = model.external.dataset.name
            if model_dataset_name == dataset_name:
                mermaid_class = MermaidClass(name=model.basename)
                for model_property in model.get_defined_properties().values():
                    if model_property.name.endswith("[]"):
                        multiplicity = "*"
                    else:
                        multiplicity = "1"
                    mermaid_property = MermaidProperty(
                        name=model_property.name,
                        access=model_property.access,
                        type=model_property.dtype.name,
                        cardinality=model_property.dtype.required,
                        multiplicity=multiplicity,
                    )
                    mermaid_class.add_property(mermaid_property)
                mermaid.add_class(mermaid_class)

        mermaids.append(mermaid)

    with open(output, 'w') as file:

        for mermaid in mermaids:

            file.write(str(mermaid))
