import traceback

import pytest

from spinta.testing.utils import create_manifest_files
from spinta.testing.context import create_test_context


def check_store(rc, tmp_path, files):
    create_manifest_files(tmp_path, files)
    context = create_test_context(rc)
    context.load({
        'manifests': {
            'default': {
                'path': str(tmp_path),
            }
        }
    })


def test_engine_name_overshadow(rc, tmp_path):
    with pytest.raises(Exception) as e:
        check_store(rc, tmp_path, {
            'models/report.yml': {
                'type': 'model',
                'name': 'report',
                'endpoint': 'report',
            },
        })
    assert str(e.value) == (
        "Endpoint name can't overshadow existing model names and 'report' is "
        "already a model name."
    )


@pytest.mark.skip('datasets')
def test_engine_name_overshadow_other(rc, tmp_path):
    with pytest.raises(Exception) as e:
        check_store(rc, tmp_path, {
            'models/report.yml': {
                'type': 'model',
                'name': 'report',
            },
            'datasets/report.yml': {
                'type': 'dataset',
                'name': 'report',
                'resources': {
                    'res': {
                        'objects': {
                            '': {
                                'rep': {
                                    'endpoint': 'report',
                                }
                            }
                        }
                    }
                }
            },
        })
    traceback.print_exception(e.type, e.value, e.tb)
    assert "Endpoint name can't overshadow existing model names and 'report' is already a model name." in str(e.value)
