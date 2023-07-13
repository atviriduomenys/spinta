import importlib
import pytest
from spinta.utils.imports import use


def test_use_correct_simple():
    sa = use('postgres', 'sqlalchemy')
    module = importlib.import_module('sqlalchemy')
    assert sa == module


def test_use_correct_specific_package():
    inspector = use('postgres', 'sqlalchemy.engine.reflection', 'Inspector')
    module = importlib.import_module('sqlalchemy.engine.reflection', package='Inspector')
    assert inspector == module


def test_use_correct_specific_package_ruamel():
    YAML = use('yaml', 'ruamel.yaml', 'YAML')
    module = importlib.import_module('ruamel.yaml', package='YAML')
    assert YAML == module


def test_use_correct_specific_package_starlette_request():
    from starlette.requests import Request
    module = importlib.import_module('starlette.requests', package='Request')
    assert Request == module


def test_use_non_existant_package():
    with pytest.raises(Exception) as e:
        use('postgres', 'test')
    assert "The module test does not belong to the group postgres." in str(e.value)

