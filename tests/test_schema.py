import pkg_resources as pres

from spinta.types import NA
from spinta.types.manifest import Manifest


def test_schema_loader():
    manifest = Manifest().discover()
    manifest.run(manifest, {'manifest.load': pres.resource_filename('spinta', 'manifest')})
    manifest.run(manifest, {'manifest.link': NA})
    manifest.run(manifest, {'backend.prepare': NA})
    assert False
