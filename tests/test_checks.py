import pytest

from spinta.testing.manifest import load_manifest_and_context
from spinta.manifests.tabular.helpers import TabularManifestError


def test_enum_level(tmpdir, rc):
    with pytest.raises(TabularManifestError) as e:
        context, manifest = load_manifest_and_context(rc, '''
        d | r | b | m | property | type    | prepare | level | title
        datasets/gov/example     |         |         |       |
                                 |         |         |       |
          |   |   | Data         |         |         |       |
          |   |   |   | value    | integer |         |       |
                                 | enum    | 1       | 3     | Positive
                                 |         | 2       | 3     | Negative
        ''')
    assert str(e.value) == (
        "None:6: Enum's do not have a level, but level '3' is given."
    )
