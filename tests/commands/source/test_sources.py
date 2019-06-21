import pytest


def test_unknown_dataset_in_config(context):
    with pytest.raises(Exception) as e:
        context.load({
            'datasets': {
                'default': {
                    'unknown': 'value',
                    'sql': 'value',
                }
            }
        })
    assert str(e.value) == "Unknown datasets in configuration parameter 'datasets': 'unknown'."
