from pathlib import Path

from ruamel.yaml import YAML

yaml = YAML(typ='safe')


def create_manifest_files(tmpdir, manifest):
    for file, data in manifest.items():
        path = Path(tmpdir) / file
        path.parent.mkdir(parents=True, exist_ok=True)
        yaml.dump(data, path)
