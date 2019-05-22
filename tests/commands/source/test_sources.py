import pytest

from spinta.config import RawConfig
from spinta.commands import load, check
from spinta.components import Config


def test_unknown_dataset_in_config(context):
    config = RawConfig()
    config.read(env_files=False, config={
        'datasets': {
            'default': {
                'unknown': 'value',
                'sql': 'value',
            }
        }
    })
    context.set('config', load(context, Config(), config))
    manifest = context.get('store').manifests['default']

    with pytest.raises(Exception) as e:
        check(context, manifest)

    assert str(e.value) == "Unknown datasets in configuration parameter 'datasets': 'unknown'."
