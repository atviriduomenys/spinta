import pytest

from spinta.utils.cli import update_config_from_cli


def test_update_config_from_cli():
    config = {
        'foo': 'bar',
        'baz': {
            'default': {
                'param': 'value',
            },
        },
        'num': 0,
    }
    custom = {
        ('baz',): 'default',
    }
    options = [
        'foo=abr',
        'baz.default.param=new',
        'baz.other.param=value',
        'num=42',
    ]
    update_config_from_cli(config, custom, options)
    assert config == {
        'foo': 'abr',
        'baz': {
            'default': {
                'param': 'new',
            },
            'other': {
                'param': 'value',
            },
        },
        'num': 42,
    }


def test_update_config_from_cli_unknown_option_error():
    config = {'nested': ''}
    custom = {}
    options = ['unknown=1']
    with pytest.raises(Exception) as e:
        update_config_from_cli(config, custom, options)
    assert str(e.value) == "Unknown configuration option 'unknown'."

    options = ['nested.param.unknown=1']
    with pytest.raises(Exception) as e:
        update_config_from_cli(config, custom, options)
    assert str(e.value) == "Unknown configuration option 'nested.param.unknown'."
