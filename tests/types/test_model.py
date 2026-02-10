from unittest.mock import Mock

import pytest

from spinta import commands
from spinta.core.enums import Level
from spinta.exceptions import (
    ModelNotFound,
    PropertyNotFound,
    MissingConfigurationParameter,
    InvalidCustomPropertyTypeConfiguration,
)
from spinta.testing.manifest import load_manifest_and_context, load_manifest
from spinta.testing.context import create_test_context
from spinta.types.model import load_level


@pytest.mark.parametrize("level", [Level.open, 3, "3"])
def test_load_level(level, rc):
    context = create_test_context(rc)
    node = Mock()
    load_level(context, node, level)
    assert node.level is Level.open


def test_configure_unknown_model(tmp_path, rc):
    rc = rc.fork({"models": {"data/City": {"properties": {"code": {"type": "string"}}}}})
    context, manifest = load_manifest_and_context(
        rc,
        manifest=""" d | r | b | m | property | source      | prepare   | type       | ref     | level | access | uri | title   | description
     datasets/gov/example     |             |           |            |         |       | open   |     | Example |
       | data                 |             |           | postgresql | default |       | open   |     | Data    |
                              |             |           |            |         |       |        |     |         |
       |   |   | Country      |             | code='lt' |            | code    |       | open   |     | Country |
       |   |   |   | code     | kodas       | lower()   | string     |         | 3     | open   |     | Code    |
       |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
    """,
        tmp_path=tmp_path,
    )
    with pytest.raises(ModelNotFound, match="Model 'data/City' not found"):
        commands.check(context, manifest)


def test_configure_unknown_property(tmp_path, rc):
    rc = rc.fork(
        {
            "models": {
                "datasets/gov/example/Country": {
                    "properties": {
                        "test": {
                            "type": "string",
                        },
                    },
                },
            },
        },
    )
    context, manifest = load_manifest_and_context(
        rc,
        manifest=""" d | r | b | m | property | source      | prepare   | type       | ref     | level | access | uri | title   | description
     datasets/gov/example     |             |           |            |         |       | open   |     | Example |
       | data                 |             |           | postgresql | default |       | open   |     | Data    |
                              |             |           |            |         |       |        |     |         |
       |   |   | Country      |             | code='lt' |            | code    |       | open   |     | Country |
       |   |   |   | code     | kodas       | lower()   | string     |         | 3     | open   |     | Code    |
       |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
    """,
        tmp_path=tmp_path,
    )
    with pytest.raises(PropertyNotFound, match="Property 'test' not found"):
        commands.check(context, manifest)


def test_configure_invalid_type_import(tmp_path, rc):
    rc = rc.fork(
        {
            "models": {
                "datasets/gov/example/Country": {
                    "properties": {
                        "code": {
                            "type": "unknown_import",
                        },
                    },
                },
            },
        },
    )
    with pytest.raises(
        InvalidCustomPropertyTypeConfiguration, match="Unable to import custom property type: 'unknown_import'."
    ):
        load_manifest(
            rc,
            manifest=""" d | r | b | m | property | source      | prepare   | type       | ref     | level | access | uri | title   | description
         datasets/gov/example     |             |           |            |         |       | open   |     | Example |
           | data                 |             |           | postgresql | default |       | open   |     | Data    |
                                  |             |           |            |         |       |        |     |         |
           |   |   | Country      |             | code='lt' |            | code    |       | open   |     | Country |
           |   |   |   | code     | kodas       | lower()   | string     |         | 3     | open   |     | Code    |
           |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
        """,
            tmp_path=tmp_path,
        )


def test_configure_missing_type_parameter(tmp_path, rc):
    rc = rc.fork(
        {
            "models": {
                "datasets/gov/example/Country": {
                    "properties": {
                        "code": {
                            "type": {
                                "other": "test",
                            },
                        },
                    },
                },
            },
        },
    )
    with pytest.raises(
        MissingConfigurationParameter, match="Property 'code' configuration is missing parameter: 'type.name'."
    ):
        load_manifest(
            rc,
            manifest=""" d | r | b | m | property | source      | prepare   | type       | ref     | level | access | uri | title   | description
         datasets/gov/example     |             |           |            |         |       | open   |     | Example |
           | data                 |             |           | postgresql | default |       | open   |     | Data    |
                                  |             |           |            |         |       |        |     |         |
           |   |   | Country      |             | code='lt' |            | code    |       | open   |     | Country |
           |   |   |   | code     | kodas       | lower()   | string     |         | 3     | open   |     | Code    |
           |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
        """,
            tmp_path=tmp_path,
        )
