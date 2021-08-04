from pathlib import Path

from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.config import ResourceTuple
from spinta.core.context import configure_context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import create_test_context
from spinta.testing.tabular import create_tabular_manifest


def test_configure(tmpdir: Path, rc: RawConfig):
    create_tabular_manifest(tmpdir / 'm1.csv', striptable('''
    d | r | b | m | property | type   | source
    datasets/1               |        | 
      | data                 | sql    | 
                             |        | 
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    '''))

    create_tabular_manifest(tmpdir / 'm2.csv', striptable('''
    d | r | b | m | property | type   | source
    datasets/2               |        | 
      | data                 | sql    | 
                             |        | 
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    '''))

    context: Context = create_test_context(rc)
    context = configure_context(context, [
        str(tmpdir / 'm1.csv'),
        str(tmpdir / 'm2.csv'),
    ])
    store = prepare_manifest(context, verbose=False)
    assert store.manifest == '''
    d | r | b | m | property | type   | source
    datasets/1               |        |
      | data                 | sql    |
                             |        |
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    datasets/2               |        |
      | data                 | sql    |
                             |        |
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    '''


def test_configure_with_resource(rc: RawConfig):
    context: Context = create_test_context(rc)
    context = configure_context(context, resources=[
        ResourceTuple('sql', 'sqlite://'),
    ])
    store = prepare_manifest(context, verbose=False)
    assert store.manifest == '''
    d | r | b | m | property       | type   | source
    datasets/gov/example/resources |        |
      | resource1                  | sql    | sqlite://
    '''


def test_ascii_manifest(tmpdir: Path, rc: RawConfig):
    manifest = '''
    d | r | b | m | property | type   | source
    datasets/1               |        |
      | data                 | sql    |
                             |        |
      |   |   | Country      |        | SALIS
      |   |   |   | name     | string | PAVADINIMAS
    '''
    manifest_file = tmpdir / 'ascii.txt'
    manifest_file.write_text(striptable(manifest), encoding='utf-8')
    context: Context = create_test_context(rc)
    context = configure_context(context, [str(manifest_file)])
    store = prepare_manifest(context, verbose=False)
    assert store.manifest == manifest
