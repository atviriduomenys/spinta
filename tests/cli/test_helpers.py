from pathlib import Path

from spinta.cli.helpers.store import prepare_manifest
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.context import configure_context
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import create_test_context
from spinta.testing.manifest import compare_manifest
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.workarounds import fix_s3_backend_issue


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

    rc = fix_s3_backend_issue(rc)
    context: Context = create_test_context(rc)
    context = configure_context(context, [
        str(tmpdir / 'm1.csv'),
        str(tmpdir / 'm2.csv'),
    ])
    store = prepare_manifest(context, verbose=False)
    a, b = compare_manifest(store.manifest, '''
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
    ''')
    assert a == b
