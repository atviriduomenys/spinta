import traceback

import pytest

from spinta.testing.utils import create_manifest_files


def check_store(context, tmpdir, files):
    create_manifest_files(tmpdir, files)
    context.load({
        'manifests': {
            'default': {
                'path': str(tmpdir),
            }
        }
    })


def test_engine_name_overshadow(context, tmpdir):
    with pytest.raises(Exception) as e:
        check_store(context, tmpdir, {
            'models/report.yml': {
                'type': 'model',
                'name': 'report',
                'endpoint': 'report',
            },
        })
    traceback.print_exception(e.type, e.value, e.tb)
    assert "Endpoint name can't overshadow existing model names and 'report' is already a model name." in str(e.value)


def test_engine_name_overshadow_other(context, tmpdir):
    with pytest.raises(Exception) as e:
        check_store(context, tmpdir, {
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
