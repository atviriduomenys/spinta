import logging
import pathlib


from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError
from ruamel.yaml.error import YAMLError

from spinta.components import Context, Manifest
from spinta.utils.path import is_ignored
from spinta import exceptions

log = logging.getLogger(__name__)

yaml = YAML(typ='safe')


class YamlManifest(Manifest):

    def load(self, config):
        self.path = config.rc.get(
            'manifests', self.name, 'path',
            cast=pathlib.Path,
            required=True,
        )

    def read(self, context: Context):
        for file, data, versions in iter_yaml_files(context, self):
            data['path'] = file
            versions = list(versions)
            for v in versions:
                assert 'function' not in v['version']['id'], file
            yield data, versions


def iter_yaml_files(context: Context, manifest: Manifest):
    config = context.get('config')
    ignore = config.rc.get('ignore', default=[], cast=list)

    for file in manifest.path.glob('**/*.yml'):
        if is_ignored(ignore, manifest.path, file):
            continue

        versions = yaml.load_all(file.read_text())

        try:
            data = next(versions)
        except (ParserError, ScannerError, YAMLError) as e:
            raise exceptions.InvalidManifestFile(
                manifest=manifest.name,
                filename=file,
                error=str(e),
            )
        yield file, data, versions
