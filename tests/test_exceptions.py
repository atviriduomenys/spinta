# Register get_error_context commands.
import pytest

import spinta.commands  # noqa
import spinta.manifests.commands.error  # noqa
from spinta import commands

from spinta.exceptions import (
    BaseError,
    error_response,
    JSONError,
    MultipleRowsFound,
    ManagedProperty,
    RequiredProperty,
    BackendNotGiven,
    SRIDNotSetForGeometry,
)
from spinta.components import Node, Property


class Error(BaseError):
    status_code = 500
    template = "Error."
    context = {
        "bar": "this.deeply.nested.value",
        "baz": "this.deeply.nested.method()",
    }


def test_str_without_this():
    error = Error()
    assert str(error) == "Error.\n"
    assert error_response(error) == {
        "type": "system",
        "code": "Error",
        "template": "Error.",
        "message": "Error.",
        "context": {},
    }


def test_str_with_this():
    node = Node()
    node.type = "model"
    node.name = "country"
    error = Error(node)
    assert str(error) == ("Error.\n  Context:\n    component: spinta.components.Node\n    model: country\n")
    assert error_response(error) == {
        "type": "model",
        "code": "Error",
        "template": "Error.",
        "message": "Error.",
        "context": {
            "component": "spinta.components.Node",
            "model": "country",
        },
    }


def test_str_optional():
    error = Error(foo=42)
    assert str(error) == ("Error.\n  Context:\n    foo: 42\n")
    assert error_response(error) == {
        "type": "system",
        "code": "Error",
        "template": "Error.",
        "message": "Error.",
        "context": {
            "foo": 42,
        },
    }


def test_unknown_context_var():
    error = Error(unknown=42)
    assert str(error) == ("Error.\n  Context:\n    unknown: 42\n")
    assert error_response(error) == {
        "type": "system",
        "code": "Error",
        "template": "Error.",
        "message": "Error.",
        "context": {
            "unknown": 42,
        },
    }


def test_context_sorting():
    error = Error(
        bar=1,
        property=1,
        foo=1,
        resource=1,
        model=1,
        dataset=1,
        baz=1,
    )
    assert str(error) == (
        "Error.\n"
        "  Context:\n"
        "    dataset: 1\n"
        "    resource: 1\n"
        "    model: 1\n"
        "    property: 1\n"
        "    bar: 1\n"
        "    baz: 1\n"
        "    foo: 1\n"
    )


def test_missing_var_in_template():
    class Error(BaseError):
        template = "Error: {message}."

    error = Error()
    assert str(error) == "Error: [UNKNOWN].\n"
    assert error_response(error) == {
        "type": "system",
        "code": "Error",
        "template": "Error: {message}.",
        "message": "Error: [UNKNOWN].",
        "context": {},
    }


def test_this_model(context):
    model = commands.get_model(context, context.get("store").manifest, "Org")
    model.path = "manifest/models/org.yml"
    error = Error(model)
    assert str(error) == (
        "Error.\n"
        "  Context:\n"
        "    component: spinta.components.Model\n"
        "    manifest: default\n"
        "    schema: models/org.yml\n"
        "    model: Org\n"
    )


def test_this_model_property(context):
    prop = commands.get_model(context, context.get("store").manifest, "Org").properties["title"]
    prop.model.path = "manifest/models/org.yml"
    error = Error(prop)
    assert str(error) == (
        "Error.\n"
        "  Context:\n"
        "    component: spinta.components.Property\n"
        "    manifest: default\n"
        "    schema: models/org.yml\n"
        "    model: Org\n"
        "    property: title\n"
    )


def test_this_model_property_dtype(context):
    dtype = commands.get_model(context, context.get("store").manifest, "Org").properties["title"].dtype
    dtype.prop.model.path = "manifest/models/org.yml"
    error = Error(dtype)
    assert str(error) == (
        "Error.\n"
        "  Context:\n"
        "    component: spinta.types.datatype.String\n"
        "    manifest: default\n"
        "    schema: models/org.yml\n"
        "    model: Org\n"
        "    property: title\n"
        "    type: string\n"
    )


def test_this_dataset_model(context):
    model = commands.get_model(context, context.get("store").manifest, "datasets/backends/postgres/dataset/Report")
    model.path = "manifest/backends/postgres/dataset/report.yml"
    error = Error(model)
    assert str(error) == (
        "Error.\n"
        "  Context:\n"
        "    component: spinta.components.Model\n"
        "    manifest: default\n"
        "    schema: backends/postgres/dataset/report.yml\n"
        "    dataset: datasets/backends/postgres/dataset\n"
        "    resource: sql\n"
        "    model: datasets/backends/postgres/dataset/Report\n"
        "    entity: reports\n"
        "    resource.backend: datasets/backends/postgres/dataset/sql\n"
    )


def test_this_dataset_model_property(context):
    prop = commands.get_model(
        context, context.get("store").manifest, "datasets/backends/postgres/dataset/Report"
    ).properties["status"]
    prop.model.path = "manifest/backends/postgres/dataset/report.yml"
    error = Error(prop)
    assert str(error) == (
        "Error.\n"
        "  Context:\n"
        "    component: spinta.components.Property\n"
        "    manifest: default\n"
        "    schema: backends/postgres/dataset/report.yml\n"
        "    dataset: datasets/backends/postgres/dataset\n"
        "    resource: sql\n"
        "    model: datasets/backends/postgres/dataset/Report\n"
        "    entity: reports\n"
        "    property: status\n"
        "    resource.backend: datasets/backends/postgres/dataset/sql\n"
    )


def test_json_error_message(context):
    exception = JSONError(Node(), error="Expecting value: line 1 column 1 (char 0)")
    assert exception.message == (
        "Invalid JSON for <spinta.components.Node(name=None)>. "
        "Original error - Expecting value: line 1 column 1 (char 0)."
    )


def test_geometry_error_message(context):
    model = commands.get_model(context, context.get("store").manifest, "Org")
    property = model.properties["title"]
    error = SRIDNotSetForGeometry(property.dtype, property=property)
    assert error.message == (
        "<spinta.components.Property(name='title', type='string', model='Org')> Geometry SRID is required, but was given None."
    )


@pytest.mark.parametrize(
    argnames=["error", "params", "expected_message"],
    argvalues=[
        [
            MultipleRowsFound,
            {"this": "datasets/gov/push/file/Country", "number_of_rows": 3},
            "Multiple (3) rows were found under datasets/gov/push/file/Country.",
        ],
        [
            ManagedProperty,
            {"this": commands.get_model, "property": "_revision"},
            "Value of property (_revision) under <model "
            "name='datasets/backends/postgres/dataset/Report'> is managed automatically "
            "and cannot be set manually.",
        ],
        [RequiredProperty, {"this": Property()}, "Property ([UNKNOWN]) is required."],
        [
            BackendNotGiven,
            {"this": commands.get_model},
            "Model (<model name='datasets/backends/postgres/dataset/Report'>) is operating in external mode, "
            "yet it does not have an assigned backend to it.",
        ],
    ],
)
def test_error_messages(error: type[BaseError], params: dict, expected_message: str, context):
    this = params.pop("this")
    if callable(this):
        this = this(context, context.get("store").manifest, "datasets/backends/postgres/dataset/Report")
    exception = error(this, **params)
    assert exception.message == expected_message
