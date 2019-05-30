import traceback

import pytest

from spinta.config import RawConfig
from spinta.commands import load, check
from spinta.components import Config, Store
from spinta.testing.utils import create_manifest_files
from spinta.utils.commands import load_commands
from spinta.testing.context import ContextForTests
from spinta.utils.imports import importstr


def check_store(tmpdir, files):
    create_manifest_files(tmpdir, files)

    rc = RawConfig()
    rc.read(env_files=False, cli_args=[f'manifests.default.path={tmpdir}'])

    Context = rc.get('components', 'core', 'context', cast=importstr)
    Context = type('ContextForTests', (ContextForTests, Context), {})
    context = Context()
    config = context.set('config', Config())

    load_commands(rc.get('commands', 'modules', cast=list))
    load(context, config, rc)
    store = load(context, context.set('store', Store()), rc)
    check(context, store)


def test_engine_name_overshadow(tmpdir):
    with pytest.raises(Exception) as e:
        check_store(tmpdir, {
            'models/report.yml': {
                'type': 'model',
                'name': 'report',
                'endpoint': 'report',
            },
        })
    traceback.print_exception(e.type, e.value, e.tb)
    assert "Endpoint name can't overshadow existing model names and 'report' is already a model name." in str(e.value)


def test_engine_name_overshadow_other(tmpdir):
    with pytest.raises(Exception) as e:
        check_store(tmpdir, {
            'models/report.yml': {
                'type': 'model',
                'name': 'report',
            },
            'datasets/report.yml': {
                'type': 'dataset',
                'name': 'report',
                'version': 1,
                'date': '',
                'resources': {
                    'res': {
                        'objects': {
                            'rep': {
                                'endpoint': 'report',
                            }
                        }
                    }
                }
            },
        })
    traceback.print_exception(e.type, e.value, e.tb)
    assert "Endpoint name can't overshadow existing model names and 'report' is already a model name." in str(e.value)
