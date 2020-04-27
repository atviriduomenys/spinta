# Register get_error_context commands.
import spinta.commands  # noqa
import spinta.manifests.commands.error  # noqa

from spinta.exceptions import BaseError, error_response
from spinta.components import Node


class Error(BaseError):
    status_code = 500
    template = "Error."
    context = {
        'bar': 'this.deeply.nested.value',
        'baz': 'this.deeply.nested.method()',
    }


def test_str_without_this():
    error = Error()
    assert str(error) == 'Error.\n'
    assert error_response(error) == {
        'type': 'system',
        'code': 'Error',
        'template': 'Error.',
        'message': 'Error.',
        'context': {},
    }


def test_str_with_this():
    node = Node()
    node.type = 'model'
    node.name = 'country'
    error = Error(node)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    component: spinta.components.Node\n'
        '    model: country\n'
    )
    assert error_response(error) == {
        'type': 'model',
        'code': 'Error',
        'template': 'Error.',
        'message': 'Error.',
        'context': {
            'component': 'spinta.components.Node',
            'model': 'country',
        },
    }


def test_str_optional():
    error = Error(foo=42)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    foo: 42\n'
    )
    assert error_response(error) == {
        'type': 'system',
        'code': 'Error',
        'template': 'Error.',
        'message': 'Error.',
        'context': {
            'foo': 42,
        },
    }


def test_unknown_context_var():
    error = Error(unknown=42)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    unknown: 42\n'
    )
    assert error_response(error) == {
        'type': 'system',
        'code': 'Error',
        'template': 'Error.',
        'message': 'Error.',
        'context': {
            'unknown': 42,
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
        'Error.\n'
        '  Context:\n'
        '    dataset: 1\n'
        '    resource: 1\n'
        '    model: 1\n'
        '    property: 1\n'
        '    bar: 1\n'
        '    baz: 1\n'
        '    foo: 1\n'
    )


def test_missing_var_in_template():

    class Error(BaseError):
        template = "Error: {message}."

    error = Error()
    assert str(error) == 'Error: [UNKNOWN].\n'
    assert error_response(error) == {
        'type': 'system',
        'code': 'Error',
        'template': 'Error: {message}.',
        'message': 'Error: [UNKNOWN].',
        'context': {},
    }


def test_this_model(context):
    model = context.get('store').manifest.objects['model']['org']
    model.path = 'manifest/models/org.yml'
    error = Error(model)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    component: spinta.components.Model\n'
        '    manifest: default\n'
        '    schema: models/org.yml\n'
        '    model: org\n'
    )


def test_this_model_property(context):
    prop = context.get('store').manifest.objects['model']['org'].properties['title']
    prop.model.path = 'manifest/models/org.yml'
    error = Error(prop)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    component: spinta.components.Property\n'
        '    manifest: default\n'
        '    schema: models/org.yml\n'
        '    model: org\n'
        '    property: title\n'
    )


def test_this_model_property_dtype(context):
    dtype = context.get('store').manifest.objects['model']['org'].properties['title'].dtype
    dtype.prop.model.path = 'manifest/models/org.yml'
    error = Error(dtype)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    component: spinta.types.datatype.String\n'
        '    manifest: default\n'
        '    schema: models/org.yml\n'
        '    model: org\n'
        '    property: title\n'
        '    type: string\n'
    )


def test_this_dataset_model(context):
    model = context.get('store').manifest.objects['model']['datasets/backends/postgres/dataset/report']
    model.path = 'manifest/backends/postgres/dataset/report.yml'
    error = Error(model)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    component: spinta.components.Model\n'
        '    manifest: default\n'
        '    schema: backends/postgres/dataset/report.yml\n'
        '    dataset: datasets/backends/postgres/dataset\n'
        '    resource: sql\n'
        '    model: datasets/backends/postgres/dataset/report\n'
        '    entity: reports\n'
        '    resource.backend: default\n'
    )


def test_this_dataset_model_property(context):
    prop = context.get('store').manifest.objects['model']['datasets/backends/postgres/dataset/report'].properties['status']
    prop.model.path = 'manifest/backends/postgres/dataset/report.yml'
    error = Error(prop)
    assert str(error) == (
        'Error.\n'
        '  Context:\n'
        '    component: spinta.components.Property\n'
        '    manifest: default\n'
        '    schema: backends/postgres/dataset/report.yml\n'
        '    dataset: datasets/backends/postgres/dataset\n'
        '    resource: sql\n'
        '    model: datasets/backends/postgres/dataset/report\n'
        '    entity: reports\n'
        '    property: status\n'
        '    resource.backend: default\n'
    )
