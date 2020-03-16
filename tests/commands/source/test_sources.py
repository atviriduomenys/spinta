import pytest

from spinta.testing.context import create_test_context


def test_unknown_dataset_in_config(rc):
    with pytest.raises(Exception) as e:
        create_test_context(rc).load({
            'datasets': {
                'default': {
                    'unknown': 'value',
                    'sql': 'value',
                }
            }
        })
    assert str(e.value) == "Unknown datasets in configuration parameter 'datasets': 'unknown'."
