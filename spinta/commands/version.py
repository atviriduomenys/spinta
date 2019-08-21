from setuptools_scm import get_version as get_scm_version

from spinta.commands import get_version
from spinta.components import Context


@get_version.register()
def get_version(context: Context):
    return {
        'api': {
            'version': '0.0.1'
        },
        'implementation': {
            'name': 'Spinta',
            'version': get_scm_version(),
        },
        'build': None,
    }
