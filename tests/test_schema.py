import pkg_resources as pres

from spinta.types.manifest import Manifest


def test_schema_loader():
    manifest = Manifest().discover()
    manifest.run(manifest, {
        'manifest.load': pres.resource_filename('spinta', 'manifest'),
    }, ns='internal')
    manifest.run(manifest, {'manifest.link': None}, ns='internal')
    manifest.run(manifest, {'backend.prepare': None}, ns='internal')
    import pp; pp(manifest.objects)
    assert False
