import pytest

from spinta.exceptions import MissingRefModel
from spinta.testing.manifest import load_manifest


@pytest.mark.manifests('internal_sql', 'csv')
def test_raises_missing_ref_model(manifest_type, tmp_path, rc):
    manifest = '''
    d | r | b | m | property | type       | ref     | uri
    datasets/gov/example     |            |         |
                             |            |         |
      | data                 | postgresql | default |
                             |            |         |
      |   |   | Country      |            | code    |
      |   |   |   | code     | string     |         |
      |   |   |   | name     | string     |         | locn:geographicName
                             |            |         |
      |   |   | City         |            | name    |
      |   |   |   | name     | string     |         | locn:geographicName
      |   |   |   | country  | ref        |         |
    '''

    with pytest.raises(MissingRefModel) as e:
        load_manifest(rc, manifest=manifest, manifest_type=manifest_type, tmp_path=tmp_path)
    assert str(e.value) == 'Property of type `ref` or `backref` should have a model in the `ref` column\n'
