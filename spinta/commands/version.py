from pkg_resources import get_distribution, DistributionNotFound

from setuptools_scm import get_version as get_scm_version

from spinta.commands import get_version
from spinta.components import Context


@get_version.register(Context)
def get_version(context: Context):
    try:
        version = get_distribution('spinta').version
    except DistributionNotFound:
        version = get_scm_version()

    return {
        'api': {
            'version': '0.0.1'
        },
        'implementation': {
            'name': 'Spinta',
            'version': version,
        },
        'build': None,
    }
