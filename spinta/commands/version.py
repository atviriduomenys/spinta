import importlib.metadata

from setuptools_scm import get_version as get_scm_version

from spinta.commands import get_version
from spinta.components import Context


@get_version.register(Context)
def get_version(context: Context):
    version = importlib.metadata.version('spinta')
    if version is None:
        version = get_scm_version()

    return {
        'implementation': {
            'name': 'Spinta',
            'version': version,
        },
        'uapi': {
            # Supported UAPI version:
            # https://ivpk.github.io/uapi/
            'version': '0.1.0',
        },
        'dsa': {
            # Supported DSA version:
            # https://ivpk.github.io/dsa/
            'version': '0.1.0',
        }
    }
