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


def test_use_non_existant_package():
    with pytest.raises(Exception) as e:
        use('postgres', 'test')
    assert "The module test does not belong to the group postgres." in str(e.value)

