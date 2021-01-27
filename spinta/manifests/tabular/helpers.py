from __future__ import annotations

import csv
import pathlib
import textwrap
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set
from typing import TextIO
from typing import Tuple
from typing import TypedDict

from spinta import commands
from spinta import spyna
from spinta.backends import Backend
from spinta.components import Context
from spinta.components import Model
from spinta.core.enums import Access
from spinta.core.ufuncs import unparse
from spinta.datasets.components import Dataset
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.constants import DATASET
from spinta.types.datatype import Ref
from spinta.utils.data import take
from spinta.utils.schema import NA

ParsedRow = Tuple[int, Dict[str, Any]]


MAIN_DIMENSIONS = [
    'dataset',
    'resource',
    'base',
    'model',
    'property',
]
EXTRA_DIMENSIONS = [
    'prefix',
    'choice',
    'param',
    'comment',
    '',
]


class TabularManifestError(Exception):
    pass


def _detect_header(
    path: Optional[pathlib.Path],
    line: int,  # Line number
    row: Iterable[str],
) -> List[str]:
    header = [h.strip().lower() for h in row]
    unknown_columns = set(header[:len(DATASET)]) - set(DATASET)
    if unknown_columns:
        unknown_columns = ', '.join(sorted(unknown_columns, key=header.index))
        raise TabularManifestError(
            f"{path}:{line}: Unknown columns: {unknown_columns}."
        )
    return header


def _detect_dimension(
    path: Optional[pathlib.Path],
    line: int,  # Line number
    row: Dict[str, str],
) -> Optional[str]:
    dimensions = [k for k in MAIN_DIMENSIONS if row[k]]

    if len(dimensions) == 1:
        return dimensions[0]

    if len(dimensions) > 1:
        dimensions = ', '.join(dimensions)
        raise TabularManifestError(
            f"{path}:{line}: In one row only single dimension can be used, "
            f"but found more than one: {dimensions}"
        )

    if row['type']:
        if row['type'] not in EXTRA_DIMENSIONS:
            raise TabularManifestError(
                f"{path}:{line}:type: Unknown additional dimension name "
                f"{row['type']}."
            )
        return row['type']

    return ''


class TabularReader:
    state: State
    path: pathlib.Path
    line: int
    type: str
    name: str
    data: Dict[str, Any]

    def __init__(
        self,
        state: State,
        path: pathlib.Path,
        line: int,
        row: Dict[str, str],
    ):
        self.state = state
        self.path = path
        self.line = line
        self.data = {}
        self.read(row)

    def __str__(self):
        return f"<{type(self).__name__} name={self.name!r}>"

    def read(self, row: Dict[str, str]) -> None:
        raise NotImplementedError

    def update(self, row: Dict[str, str]) -> None:
        if any(row.values()):
            self.error("Updates are not supported in this context.")

    def release(self, reader: TabularReader = None) -> bool:
        raise NotImplementedError

    def enter(self) -> None:
        raise NotImplementedError

    def leave(self) -> None:
        raise NotImplementedError

    def error(self, message: str) -> None:
        raise TabularManifestError(f"{self.path}:{self.line}: {message}")


class ManifestReader(TabularReader):
    type: str = 'manifest'
    datasets: Set[str]

    def read(self, row: Dict[str, str]) -> None:
        self.name = str(self.path)
        self.data = {
            'type': 'manifest',
        }

    def release(self, reader: TabularReader = None) -> bool:
        return reader is None

    def enter(self) -> None:
        self.datasets = set()
        self.state.manifest = self

    def leave(self) -> None:
        self.state.manifest = None


class DatasetReader(TabularReader):
    type: str = 'dataset'

    def read(self, row: Dict[str, str]) -> None:
        self.name = row['dataset']

        if row['dataset'] in self.state.manifest.datasets:
            self.error("Dataset already defined.")

        self.data = {
            'type': 'dataset',
            'id': row['id'],
            'name': row['dataset'],
            'level': row['level'],
            'access': row['access'],
            'title': row['title'],
            'description': row['description'],
            'resources': {},
        }

    def release(self, reader: TabularReader = None) -> bool:
        return reader is None or isinstance(reader, (
            ManifestReader,
            DatasetReader,
        ))

    def enter(self) -> None:
        self.state.dataset = self

    def leave(self) -> None:
        self.state.dataset = None


class ResourceReader(TabularReader):
    type: str = 'resource'

    def read(self, row: Dict[str, str]) -> None:
        self.name = row['resource']

        if self.state.dataset is None:
            self.read_backend(row)
        else:
            self.read_resource(row)

    def read_backend(self, row: Dict[str, str]) -> None:
        # Backends will be loaded using
        # `spinta.manifests.helpers._load_manifest_backends`.

        if 'backends' not in self.state.manifest.data:
            self.state.manifest.data['backends'] = {}
        backends = self.state.manifest.data['backends']

        if self.name in backends:
            self.error(
                f"Backend {self.name!r} with the same name already defined."
            )

        self.data = {
            'type': row['type'],
            'name': self.name,
            'dsn': row['source'],
            'title': row['title'],
            'description': row['description'],
        }

        backends[self.name] = self.data

    def read_resource(self, row: Dict[str, str]) -> None:
        dataset = self.state.dataset.data

        if self.name in dataset['resources']:
            self.error("Resource with the same name already defined in ")

        self.data = {
            'type': row['type'],
            'backend': row['ref'],
            'external': row['source'],
            'prepare': (
                spyna.parse(row['prepare'])
                if row['prepare'] else None
            ),
            'level': row['level'],
            'access': row['access'],
            'title': row['title'],
            'description': row['description'],
        }

        dataset['resources'][self.name] = self.data

    def release(self, reader: TabularReader = None) -> bool:
        return reader is None or isinstance(reader, (
            ManifestReader,
            DatasetReader,
            ResourceReader,
        ))

    def enter(self) -> None:
        self.state.resource = self

    def leave(self) -> None:
        self.state.resource = None


class BaseReader(TabularReader):
    type: str = 'base'

    def read(self, row: Dict[str, str]) -> None:
        self.name = row['base']

        dataset = self.state.dataset.data if self.state.dataset else None
        self.data = {
            'model': get_relative_model_name(dataset, row['base']),
            'pk': row['ref'],
        }

    def release(self, reader: TabularReader = None) -> bool:
        return reader is None or isinstance(reader, (
            ManifestReader,
            DatasetReader,
            ResourceReader,
            BaseReader,
        ))

    def enter(self) -> None:
        self.state.base = self

    def leave(self) -> None:
        self.state.base = None


class ModelReader(TabularReader):
    type: str = 'model'

    def read(self, row: Dict[str, str]) -> None:
        dataset = self.state.dataset
        resource = self.state.resource
        base = self.state.base
        name = get_relative_model_name(
            dataset.data if dataset else None,
            row['model'],
        )

        self.name = name

        if name in self.state.models:
            self.error("Model with the same name is already defined.")

        self.data = {
            'type': 'model',
            'id': row['id'],
            'name': name,
            'base': base.name if base else None,
            'level': row['level'],
            'access': row['access'],
            'title': row['title'],
            'description': row['description'],
            'properties': {},
            'external': {
                'dataset': dataset.name if dataset else '',
                'resource': resource.name if dataset and resource else '',
                'pk': (
                    [x.strip() for x in row['ref'].split(',')]
                    if row['ref'] else []
                ),
                'name': row['source'],
                'prepare': (
                    spyna.parse(row['prepare'])
                    if row['prepare'] else None
                ),
            },
        }

        if resource and not dataset:
            self.data['backend'] = resource.name

    def release(self, reader: TabularReader = None) -> bool:
        return reader is None or isinstance(reader, (
            ManifestReader,
            DatasetReader,
            ResourceReader,
            BaseReader,
            ModelReader,
        ))

    def enter(self) -> None:
        self.state.model = self
        self.state.models.add(self.name)

    def leave(self) -> None:
        self.state.model = None


def _parse_property_ref(ref: str) -> Tuple[str, List[str]]:
    if '[' in ref:
        ref = ref.rstrip(']')
        ref_model, ref_props = ref.split('[', 1)
        ref_props = [p.strip() for p in ref_props.split(',')]
    else:
        ref_model = ref
        ref_props = []
    return ref_model, ref_props


class PropertyReader(TabularReader):
    type: str = 'property'

    def read(self, row: Dict[str, str]) -> None:
        self.name = row['property']

        if self.state.model is None:
            context = self.state.stack[-1]
            self.error(
                f"Property {self.name!r} must be defined in a model context. "
                f"Now it is defined in {context.name!r} {context.type} context."
            )

        if row['property'] in self.state.model.data['properties']:
            self.error(
                f"Property {self.name!r} with the same name is already "
                f"defined for this {self.state.model.name!r} model."
            )

        self.data = {
            'type': row['type'],
            'prepare': (
                spyna.parse(row['prepare'])
                if row['prepare'] else None
            ),
            'level': row['level'],
            'access': row['access'],
            'uri': row['uri'],
            'title': row['title'],
            'description': row['description'],
        }

        dataset = self.state.dataset.data if self.state.dataset else None

        if row['type'] == 'ref':
            ref_model, ref_props = _parse_property_ref(row['ref'])
            self.data['model'] = get_relative_model_name(dataset, ref_model)
            self.data['refprops'] = ref_props

        if dataset or row['source']:
            self.data['external'] = {
                'name': row['source'],
                'prepare': self.data.pop('prepare'),
            }

        self.state.model.data['properties'][row['property']] = self.data

    def release(self, reader: TabularReader = None) -> bool:
        return reader is None or isinstance(reader, (
            ManifestReader,
            DatasetReader,
            ResourceReader,
            BaseReader,
            ModelReader,
            PropertyReader,
        ))

    def enter(self) -> None:
        self.state.prop = self

    def leave(self) -> None:
        self.state.prop = None


class AppendReader(TabularReader):
    type: str = 'append'

    def read(self, row: Dict[str, str]) -> None:
        self.name = row['ref']
        self.data = {}
        self.state.stack[-1].update(row)

    def release(self, reader: TabularReader = None) -> bool:
        return True

    def enter(self) -> None:
        pass

    def leave(self) -> None:
        pass


class PrefixReader(TabularReader):
    type: str = 'prefix'

    def read(self, row: Dict[str, str]) -> None:
        self.name = row['ref']

        node = (
            self.state.prop or
            self.state.model or
            self.state.base or
            self.state.resource or
            self.state.dataset or
            self.state.manifest
        )

        if 'prefixes' not in node.data:
            node.data['prefixes'] = {}

        prefixes = node.data['prefixes']

        if self.name in prefixes:
            self.error(
                f"Prefix {self.name!r} with the same name is already "
                f"defined for this {node.name!r} {node.type}."
            )

        self.data = {
            'id': row['id'],
            'eid': f'{self.path}:{self.line}',
            'type': self.type,
            'name': self.name,
            'uri': row['uri'],
            'title': row['title'],
            'description': row['description'],
        }

        prefixes[self.name] = self.data

    def update(self, row: Dict[str, str]) -> None:
        self.read(row)

    def release(self, reader: TabularReader = None) -> bool:
        return True

    def enter(self) -> None:
        pass

    def leave(self) -> None:
        pass


READERS = {
    'dataset': DatasetReader,
    'resource': ResourceReader,
    'base': BaseReader,
    'model': ModelReader,
    'property': PropertyReader,
    '': AppendReader,
    'prefix': PrefixReader,
}


class State:
    stack: List[TabularReader]

    backends: Dict[str, Dict[str, str]] = None

    models: Set[str]

    manifest: ManifestReader = None
    dataset: DatasetReader = None
    resource: ResourceReader = None
    base: BaseReader = None
    model: ModelReader = None
    prop: PropertyReader = None

    def __init__(self):
        self.stack = []
        self.models = set()

    def release(self, reader: TabularReader = None) -> Iterator[ParsedRow]:
        for item in list(reversed(self.stack)):
            if item.release(reader):
                if isinstance(item, (
                    ManifestReader,
                    DatasetReader,
                    ModelReader,
                )):
                    yield item.line, item.data
                item.leave()
                self.stack.pop()
            else:
                break

        if reader:
            reader.enter()
            self.stack.append(reader)


def _read_tabular_manifest_rows(
    path: Optional[pathlib.Path],
    rows: Iterator[List[str]],
) -> Iterator[ParsedRow]:
    header = next(rows, None)
    if header is None:
        # Looks like an empty file.
        return
    header = _detect_header(path, 1, header)

    defaults = {k: '' for k in DATASET}

    state = State()
    reader = ManifestReader(state, path, 1, {})
    yield from state.release(reader)

    for i, row in enumerate(rows, 2):
        row = dict(zip(header, row))
        row = {**defaults, **row}
        dimension = _detect_dimension(path, i, row)
        Reader = READERS[dimension]
        reader = Reader(state, path, i, row)
        yield from state.release(reader)

    yield from state.release()


def read_tabular_manifest(path: pathlib.Path) -> Iterator[ParsedRow]:
    with path.open() as f:
        csv_reader = csv.reader(f)
        yield from _read_tabular_manifest_rows(path, csv_reader)


def striptable(table):
    return textwrap.dedent(table).strip()


def read_ascii_tabular_rows(
    manifest: str,
    *,
    strip: bool = False,
) -> Iterator[List[str]]:
    if strip:
        manifest = striptable(manifest)

    lines = (line.strip() for line in manifest.splitlines())
    lines = filter(None, lines)

    # Read header
    header = next(lines, None)
    if header is None:
        return
    header = header.split('|')
    header = [h.strip().lower() for h in header]
    header = [SHORT_NAMES.get(h, h) for h in header]

    # Find index where dimension columns end.
    dim = sum(1 for h in header if h in DATASET[:6])
    yield header
    for line in lines:
        row = line.split('|')
        row = [x.strip() for x in row]
        rem = len(header) - len(row)
        row = row[:dim - rem] + [''] * rem + row[dim - rem:]
        assert len(header) == len(row), line
        yield row


def read_ascii_tabular_manifest(
    manifest: str,
    *,
    strip: bool = False,
) -> Iterator[ParsedRow]:
    rows = read_ascii_tabular_rows(manifest, strip=strip)
    yield from _read_tabular_manifest_rows(None, rows)


def load_ascii_tabular_manifest(
    context: Context,
    manifest: Manifest,
    manifest_ascii_table: str,
    *,
    strip: bool = False,
):
    schemas = read_ascii_tabular_manifest(manifest_ascii_table)
    load_manifest_nodes(context, manifest, schemas)
    commands.link(context, manifest)


def get_relative_model_name(dataset: dict, name: str) -> str:
    if name.startswith('/'):
        return name[1:]
    elif dataset is None:
        return name
    else:
        return '/'.join([
            dataset['name'],
            name,
        ])


def to_relative_model_name(model: Model, dataset: Dataset = None) -> str:
    """Convert absolute model `name` to relative."""
    if dataset is None:
        return model.name
    if model.name.startswith(dataset.name):
        prefix = dataset.name
        return model.name[len(prefix) + 1:]
    else:
        return '/' + model.name


def tabular_eid(model: Model):
    if isinstance(model.eid, int):
        return model.eid
    else:
        return 0


def _prefixes_to_tabular(
    prefixes: Dict[str, UriPrefix],
) -> Iterator[ManifestRow]:
    first = True
    for name, prefix in prefixes.items():
        yield torow(DATASET, {
            'id': prefix.id,
            'type': prefix.type if first else '',
            'ref': name,
            'uri': prefix.uri,
            'title': prefix.title,
            'description': prefix.description,
        })
        first = False


def _backends_to_tabular(
    backends: Dict[str, Backend],
) -> Iterator[ManifestRow]:
    for name, backend in backends.items():
        yield torow(DATASET, {
            'type': backend.type,
            'resource': name,
            'source': backend.config.get('dsn'),
        })


def datasets_to_tabular(
    manifest: Manifest,
    *,
    external: bool = True,
    access: Access = Access.private,
    internal: bool = False,
):
    yield from _prefixes_to_tabular(manifest.prefixes)
    yield from _backends_to_tabular(manifest.backends)
    dataset = None
    resource = None
    models = manifest.models if internal else take(manifest.models)
    for model in sorted(models.values(), key=tabular_eid):
        if model.access < access:
            continue

        if model.external:
            if dataset is None or dataset.name != model.external.dataset.name:
                dataset = model.external.dataset
                if dataset:
                    yield torow(DATASET, {
                        'id': dataset.id,
                        'dataset': dataset.name,
                        'level': dataset.level,
                        'access': dataset.access.name,
                        'title': dataset.title,
                        'description': dataset.description,
                    })

            if model.external and model.external.resource and (
                resource is None or
                resource.name != model.external.resource.name
            ):
                resource = model.external.resource
                if resource:
                    yield torow(DATASET, {
                        'resource': resource.name,
                        'source': resource.external,
                        'prepare': unparse(resource.prepare or NA),
                        'type': resource.backend.type if resource.backend else '',
                        'ref': '' if resource.external else resource.backend.name,
                        'level': resource.level,
                        'access': resource.access.name,
                        'title': resource.title,
                        'description': resource.description,
                    })

        yield torow(DATASET, {})

        data = {
            'id': model.id,
            'model': model.name,
            'level': model.level,
            'access': model.access.name,
            'title': model.title,
            'description': model.description,
        }
        if model.external and model.external.dataset:
            data['model'] = to_relative_model_name(
                model,
                model.external.dataset,
            )
        if external and model.external:
            data.update({
                'source': model.external.name,
                'prepare': unparse(model.external.prepare or NA),
            })
            if all(p.access >= access for p in model.external.pkeys):
                # Add `ref` only if all properties are available in the
                # resulting manifest.
                data['ref'] = ', '.join([
                    p.name for p in model.external.pkeys
                ])
        yield torow(DATASET, data)

        for prop in model.properties.values():
            if prop.name.startswith('_'):
                continue

            if prop.access < access:
                continue

            data = {
                'property': prop.place,
                'type': prop.dtype.name,
                'level': prop.level,
                'access': prop.access.name,
                'uri': prop.uri,
                'title': prop.title,
                'description': prop.description,
            }

            if external and prop.external:
                if isinstance(prop.external, list):
                    # data['source'] = ', '.join(x.name for x in prop.external)
                    # data['prepare'] = ', '.join(
                    #     unparse(x.prepare or NA)
                    #     for x in prop.external if x.prepare
                    # )
                    raise DeprecationWarning(
                        "Source can't be a list, use prepare instead."
                    )
                elif prop.external:
                    data['source'] = prop.external.name
                    data['prepare'] = unparse(prop.external.prepare or NA)

            if isinstance(prop.dtype, Ref):
                if model.external and model.external.dataset:
                    data['ref'] = to_relative_model_name(
                        prop.dtype.model,
                        model.external.dataset,
                    )
                else:
                    data['ref'] = prop.dtype.model.name

            yield torow(DATASET, data)


class ManifestRow(TypedDict):
    id: str
    dataset: str
    resource: str
    base: str
    model: str
    property: str
    type: str
    ref: str
    source: str
    prepare: str
    level: str
    access: str
    uri: str
    title: str
    description: str


def torow(keys, values) -> ManifestRow:
    return {k: values.get(k) for k in keys}


def write_tabular_manifest(file: TextIO, rows: Iterable[dict]):
    writer = csv.DictWriter(file, fieldnames=DATASET)
    writer.writeheader()
    writer.writerows(rows)


SHORT_NAMES = {
    'd': 'dataset',
    'r': 'resource',
    'b': 'base',
    'm': 'model',
}
