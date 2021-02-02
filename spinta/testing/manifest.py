from typing import Tuple

from spinta.manifests.components import Manifest
from spinta.manifests.tabular.helpers import SHORT_NAMES
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import striptable


def compare_manifest(manifest: Manifest, expected: str) -> Tuple[str, str]:
    expected = striptable(expected)
    cols = [c.strip() for c in expected.splitlines()[0].split('|')]
    cols = [SHORT_NAMES.get(c, c) for c in cols]
    actual = render_tabular_manifest(manifest, cols)
    return actual, expected
