import csv
from pathlib import Path

import pytest

from spinta import commands
from spinta.core.config import RawConfig
from spinta.core.enums import Access
from spinta.manifests.tabular.helpers import TabularManifestError
from spinta.testing.manifest import load_manifest_and_context


def test_csv_empty_column(rc: RawConfig, tmp_path: Path):
    csv = """\
    id,dataset,resource,base,model,property,type,ref,source,prepare,level,access,uri,title,description
    ,example,,,,,,,,,open,,Example,,
    ,,,,City,,,name,,,,open,,City,,error
    ,,,,,name,string,,,pavadinimas,3,open,,City name,
    """
    path = tmp_path / "manifest.csv"
    path.write_text(csv)
    with pytest.raises(TabularManifestError):
        load_manifest_and_context(rc, path)


@pytest.mark.parametrize("tags_access, expected", [("open", Access.open), ("private", Access.private)])
def test_array_items_inline_enum_inherits_access_from_property(
    rc: RawConfig, tmp_path: Path, tags_access: str, expected: Access
):
    columns = [
        "id",
        "dataset",
        "resource",
        "base",
        "model",
        "property",
        "type",
        "ref",
        "source",
        "prepare",
        "level",
        "access",
        "uri",
        "title",
        "description",
    ]
    rows = [
        {"dataset": "example"},
        {"model": "City", "access": "open"},
        {"property": "tags", "type": "array", "access": tags_access},
        {"property": "tags[]", "type": "string"},
        {"type": "enum", "prepare": "'A'", "title": "Atvira"},
    ]
    path = tmp_path / "manifest.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    context, manifest = load_manifest_and_context(rc, path)
    model = commands.get_model(context, manifest, "example/City")
    items = model.properties["tags"].dtype.items
    assert items.enums[""]["A"].access == expected


COLUMNS = [
    "id",
    "dataset",
    "resource",
    "base",
    "model",
    "property",
    "type",
    "ref",
    "source",
    "prepare",
    "level",
    "access",
    "uri",
    "title",
    "description",
]


def _load_array_manifest(rc: RawConfig, tmp_path: Path, rows: list[dict], **config):
    path = tmp_path / "manifest.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows([{"dataset": "example"}] + rows)
    context, manifest = load_manifest_and_context(rc.fork(config) if config else rc, path)
    return context, commands.get_model(context, manifest, "example/City")


@pytest.mark.parametrize("tags_access, expected", [("open", Access.open), ("private", Access.private)])
def test_array_items_inline_enum_inherits_access_from_property(
    rc: RawConfig, tmp_path: Path, tags_access: str, expected: Access
):
    _, model = _load_array_manifest(
        rc,
        tmp_path,
        [
            {"model": "City", "access": "open"},
            {"property": "tags", "type": "array", "access": tags_access},
            {"property": "tags[]", "type": "string"},
            {"type": "enum", "prepare": "'A'", "title": "Atvira"},
        ],
    )
    items = model.properties["tags"].dtype.items
    assert items.access == expected
    assert items.enums[""]["A"].access == expected


def test_array_items_without_access_inherit_array_access(rc: RawConfig, tmp_path: Path):
    _, model = _load_array_manifest(
        rc,
        tmp_path,
        [
            {"model": "City", "access": "open"},
            {"property": "tags", "type": "array", "access": "open"},
            {"property": "tags[]", "type": "string"},
        ],
    )
    assert model.properties["tags"].dtype.items.access == Access.open


@pytest.mark.parametrize(
    "default_access, expected",
    [("open", Access.open), ("protected", Access.protected), ("private", Access.private)],
)
def test_array_items_enum_without_access_uses_default_access_level(
    rc: RawConfig, tmp_path: Path, default_access: str, expected: Access
):
    _, model = _load_array_manifest(
        rc,
        tmp_path,
        [
            {"model": "City"},
            {"property": "tags", "type": "array"},
            {"property": "tags[]", "type": "string"},
            {"type": "enum", "prepare": "'A'", "title": "Atvira"},
        ],
        default_access_level=default_access,
    )
    assert model.properties["tags"].dtype.items.enums[""]["A"].access == expected
