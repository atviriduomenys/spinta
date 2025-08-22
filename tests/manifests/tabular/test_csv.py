from pathlib import Path

import pytest

from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.manifests.tabular.helpers import TabularManifestError


def test_csv_empty_column(rc: RawConfig, tmp_path: Path):
    csv = """\
    id,dataset,resource,base,model,property,type,ref,source,prepare,level,access,uri,title,description
    ,example,,,,,,,,,open,,Example,,
    ,,,,City,,,name,,,,open,,City,,error
    ,,,,,name,string,,,pavadinimas,3,open,,City name,
    """
    path = tmp_path / "manifest.csv"
    path.write_text(csv)
    with pytest.raises(TabularManifestError):
        load_manifest_and_context(rc, path)
