from __future__ import annotations

import csv
import pathlib
import textwrap
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Set
from typing import TextIO
from typing import Tuple
from typing import TypeVar
from typing import Union
from typing import cast

from spinta import commands
from spinta import spyna
from spinta.backends import Backend
from spinta.backends.components import BackendOrigin
from spinta.components import Context
from spinta.dimensions.enum.components import EnumItem
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Property
from spinta.core.enums import Access
from spinta.core.ufuncs import unparse
from spinta.datasets.components import Dataset
from spinta.dimensions.enum.components import Enums
from spinta.dimensions.prefix.components import UriPrefix
from spinta.exceptions import PropertyNotFound
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.components import ACCESS
from spinta.manifests.tabular.components import BackendRow
from spinta.manifests.tabular.components import BaseRow
from spinta.manifests.tabular.components import DESCRIPTION
from spinta.manifests.tabular.components import DatasetRow
from spinta.manifests.tabular.components import ID
from spinta.manifests.tabular.components import MANIFEST_COLUMNS
from spinta.manifests.tabular.components import ManifestColumn
from spinta.manifests.tabular.components import ManifestRow
from spinta.manifests.tabular.components import ManifestTableRow
from spinta.manifests.tabular.components import ModelRow
from spinta.manifests.tabular.components import PREPARE
from spinta.manifests.tabular.components import PROPERTY
from spinta.manifests.tabular.components import PrefixRow
from spinta.manifests.tabular.components import PropertyRow
from spinta.manifests.tabular.components import REF
from spinta.manifests.tabular.components import ResourceRow
from spinta.manifests.tabular.components import SOURCE
from spinta.manifests.tabular.components import TITLE
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
    '',
    'prefix',
    'enum',
    'param',
    'comment',
    'ns',
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
    data: ManifestRow               # Used when `appendable` is False
    rows: List[Dict[str, Any]]      # Used when `appendable` is True
    appendable: bool = False        # Tells if reader is appendable.

    def __init__(
        self,
        state: State,
        path: pathlib.Path,
        line: int,
    ):
        self.state = state
        self.path = path
        self.line = line
        self.data = {}
        self.rows = []

    def __str__(self):
        return f"<{type(self).__name__} name={self.name!r}>"

    def read(self, row: Dict[str, str]) -> None:
        raise NotImplementedError

    def append(self, row: Dict[str, str]) -> None:
        if any(row.values()):
            self.error(
                f"Updates are not supported in context of {self.type!r}."
            )

    def release(self, reader: TabularReader = None) -> bool:
        raise NotImplementedError

    def items(self) -> Iterator[ParsedRow]:
        if self.appendable:
            for data in self.rows:
                yield self.line, data
        else:
            yield self.line, self.data

    def enter(self) -> None:
        raise NotImplementedError

    def leave(self) -> None:
        raise NotImplementedError

    def error(self, message: str) -> None:
        raise TabularManifestError(f"{self.path}:{self.line}: {message}")


class ManifestReader(TabularReader):
    type: str = 'manifest'
    datasets: Set[str]
    namespaces: Set[str]
    data: ManifestTableRow

    def read(self, row: ManifestRow) -> None:
        self.name = str(self.path)
        self.data = {
            'type': 'manifest',
        }

    def release(self, reader: TabularReader = None) -> bool:
        return reader is None

    def enter(self) -> None:
        self.datasets = set()
        self.namespaces = set()
        self.state.manifest = self

    def leave(self) -> None:
        self.state.manifest = None


class DatasetReader(TabularReader):
    type: str = 'dataset'
    data: DatasetRow

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
    data: Union[BackendRow, ResourceRow]

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
    data: BaseRow

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
    data: ModelRow

    def read(self, row: Dict[str, str]) -> None:
        dataset = self.state.dataset
        resource = self.state.resource
        base = self.state.base
        name = get_relative_model_name(
            dataset.data if dataset else None,
            row['model'],
        )

        if self.state.rename_duplicates:
            dup = 1
            _name = name
            while _name in self.state.models:
                _name = f'{name}_{dup}'
                dup += 1
            name = _name
        elif name in self.state.models:
            self.error(f"Model {name!r} with the same name is already defined.")

        self.name = name

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
    data: PropertyRow
    enums: Set[str]

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
    data: ManifestRow

    def read(self, row: ManifestRow) -> None:
        self.name = row[REF]
        self.data = row

    def release(self, reader: TabularReader = None) -> bool:
        return True

    def enter(self) -> None:
        pass

    def leave(self) -> None:
        self.state.stack[-1].append(self.data)


class PrefixReader(TabularReader):
    type: str = 'prefix'
    data: PrefixRow

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

    def append(self, row: Dict[str, str]) -> None:
        self.read(row)

    def release(self, reader: TabularReader = None) -> bool:
        return not isinstance(reader, AppendReader)

    def enter(self) -> None:
        pass

    def leave(self) -> None:
        pass


class NamespaceReader(TabularReader):
    type: str = 'ns'
    appendable: bool = True

    def read(self, row: Dict[str, str]) -> None:
        if not row['ref']:
            # `ref` is a required parameter.
            return

        self.name = row['ref']

        manifest = self.state.manifest

        if self.name in manifest.namespaces:
            self.error(
                f"Namespace {self.name!r} with the same name is already "
                f"defined."
            )

        manifest.namespaces.add(self.name)

        self.rows.append({
            'id': row['id'],
            'type': self.type,
            'name': self.name,
            'title': row['title'],
            'description': row['description'],
        })

    def append(self, row: Dict[str, str]) -> None:
        self.read(row)

    def release(self, reader: TabularReader = None) -> bool:
        return not isinstance(reader, AppendReader)

    def enter(self) -> None:
        pass

    def leave(self) -> None:
        pass


def _read_enum_row(name: str, row: ManifestRow) -> Dict[str, Any]:
    return {
        'name': name,
        'source': row[SOURCE],
        'prepare': (
            spyna.parse(row[PREPARE])
            if row[PREPARE] else NA
        ),
        'access': row[ACCESS],
        'title': row[TITLE],
        'description': row[DESCRIPTION],
    }


class EnumReader(TabularReader):
    type: str = 'enum'
    appendable: bool = True

    def read(self, row: ManifestRow) -> None:
        prop = self.state.prop

        if row[REF]:
            self.name = row[REF]
        else:
            self.name = ''

        if 'enums' not in prop.data:
            prop.data['enums'] = {}

        if self.name in prop.data['enums']:
            self.error(
                f"Enum {self.name!r} with the same name is already "
                f"defined."
            )

        source = row[SOURCE] or row[PREPARE]
        prop.data['enums'][self.name] = {
            source: _read_enum_row(self.name, row)
        }

    def append(self, row: ManifestRow) -> None:
        if not row[SOURCE] and not row[PREPARE]:
            # At least source or prepare must be defined.
            return

        enum = cast(EnumReader, self.state.stack[-1])
        prop = self.state.prop
        source = row[SOURCE] or row[PREPARE]

        if source in prop.data['enums'][enum.name]:
            self.error(
                f"Enum {self.name!r} item {source!r} with the same value is "
                f"already defined."
            )

        prop.data['enums'][enum.name][source] = _read_enum_row(enum.name, row)

    def release(self, reader: TabularReader = None) -> bool:
        return not isinstance(reader, AppendReader)

    def enter(self) -> None:
        pass

    def leave(self) -> None:
        pass


READERS = {
    # Main dimensions
    'dataset': DatasetReader,
    'resource': ResourceReader,
    'base': BaseReader,
    'model': ModelReader,
    'property': PropertyReader,

    # Extra dimensions
    '': AppendReader,
    'prefix': PrefixReader,
    'ns': NamespaceReader,
    'enum': EnumReader,
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

    rename_duplicates: bool = False

    def __init__(self):
        self.stack = []
        self.models = set()

    def release(self, reader: TabularReader = None) -> Iterator[ParsedRow]:
        for parent in list(reversed(self.stack)):
            if parent.release(reader):
                if isinstance(parent, (
                    ManifestReader,
                    NamespaceReader,
                    DatasetReader,
                    ModelReader,
                )):
                    yield from parent.items()
                self.stack.pop()
                parent.leave()
            else:
                break

        if reader:
            reader.enter()
            self.stack.append(reader)


def _read_tabular_manifest_rows(
    path: Optional[pathlib.Path],
    rows: Iterator[List[str]],
    *,
    rename_duplicates: bool = True,
) -> Iterator[ParsedRow]:
    header = next(rows, None)
    if header is None:
        # Looks like an empty file.
        return
    header = _detect_header(path, 1, header)

    defaults = {k: '' for k in MANIFEST_COLUMNS}

    state = State()
    state.rename_duplicates = rename_duplicates
    reader = ManifestReader(state, path, 1)
    reader.read({})
    yield from state.release(reader)

    for i, row in enumerate(rows, 2):
        row = dict(zip(header, row))
        row = {**defaults, **row}
        dimension = _detect_dimension(path, i, row)
        Reader = READERS[dimension]
        reader = Reader(state, path, i)
        reader.read(row)
        yield from state.release(reader)

    yield from state.release()


def read_tabular_manifest(
    path: pathlib.Path,
    *,
    rename_duplicates: bool = False,
) -> Iterator[ParsedRow]:
    with path.open() as f:
        csv_reader = csv.reader(f)
        yield from _read_tabular_manifest_rows(
            path,
            csv_reader,
            rename_duplicates=rename_duplicates,
        )


def striptable(table):
    return textwrap.dedent(table).strip()


def _join_escapes(row: List[str]) -> List[str]:
    res = []
    for v in row:
        if res and res[-1] and res[-1].endswith('\\'):
            res[-1] = res[-1][:-1] + '|' + v
        else:
            res.append(v)
    return res


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
    header = normalizes_columns(header.split('|'))

    # Find index where dimension columns end.
    dim = sum(1 for h in header if h in DATASET[:6])
    yield header
    for line in lines:
        row = _join_escapes(line.split('|'))
        row = [x.strip() for x in row]
        rem = len(header) - len(row)
        row = row[:dim - rem] + [''] * rem + row[dim - rem:]
        assert len(header) == len(row), line
        yield row


def read_ascii_tabular_manifest(
    manifest: str,
    *,
    strip: bool = False,
    rename_duplicates: bool = False,
) -> Iterator[ParsedRow]:
    rows = read_ascii_tabular_rows(manifest, strip=strip)
    yield from _read_tabular_manifest_rows(
        None,
        rows,
        rename_duplicates=rename_duplicates,
    )


def load_ascii_tabular_manifest(
    context: Context,
    manifest: Manifest,
    manifest_ascii_table: str,
    *,
    strip: bool = False,
) -> None:
    schemas = read_ascii_tabular_manifest(manifest_ascii_table, strip=strip)
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


def _order_models_by_access(model: Model):
    return model.access or Access.private


class OrderBy(NamedTuple):
    func: Callable[[Union[Model, Property, EnumItem]], Any]
    reverse: bool = False


MODELS_ORDER_BY = {
    'access': OrderBy(_order_models_by_access, reverse=True),
    'default': OrderBy(tabular_eid),
}


def _order_properties_by_access(prop: Property):
    return prop.access or Access.private


PROPERTIES_ORDER_BY = {
    'access': OrderBy(_order_properties_by_access, reverse=True),
}


T = TypeVar('T', Model, Property, EnumItem)


def sort(
    ordering: Dict[str, OrderBy],
    items: Iterable[T],
    order_by: Optional[str],
) -> Iterable[T]:
    order: Optional[OrderBy] = None

    if order_by:
        order = ordering[order_by]
    elif 'default' in ordering:
        order = ordering['default']

    if order:
        return sorted(items, key=order.func, reverse=order.reverse)
    else:
        return items


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


def _namespaces_to_tabular(
    namespaces: Dict[str, Namespace],
) -> Iterator[ManifestRow]:
    namespaces = {
        k: ns
        for k, ns in namespaces.items() if not ns.generated
    }
    first = True
    for name, ns in namespaces.items():
        yield torow(DATASET, {
            'type': ns.type if first else '',
            'ref': name,
            'title': ns.title,
            'description': ns.description,
        })
        first = False


def _order_enums_by_access(item: EnumItem):
    return item.access or Access.private


ENUMS_ORDER_BY = {
    'access': OrderBy(_order_enums_by_access, reverse=True),
}


def _enums_to_tabular(
    enums: Optional[Enums],
    *,
    external: bool = True,
    access: Access = Access.private,
    order_by: ManifestColumn = None,
) -> Iterator[ManifestRow]:
    if enums is None:
        return
    for name, enum in enums.items():
        first = True
        items = sort(ENUMS_ORDER_BY, enum.values(), order_by)
        for item in items:
            if item.access < access:
                continue
            yield torow(DATASET, {
                'type': 'enum' if first else '',
                'ref': name if first else '',
                'source': item.source if external else '',
                'prepare': spyna.unparse(item.prepare or NA),
                'access': item.given.access,
                'title': item.title,
                'description': item.description,
            })
            first = False


def datasets_to_tabular(
    manifest: Manifest,
    *,
    external: bool = True,
    access: Access = Access.private,
    internal: bool = False,
    order_by: ManifestColumn = None,
) -> Iterator[ManifestRow]:
    yield from _prefixes_to_tabular(manifest.prefixes)
    yield from _backends_to_tabular(manifest.backends)
    yield from _namespaces_to_tabular(manifest.namespaces)
    dataset = None
    resource = None

    models = manifest.models if internal else take(manifest.models)
    models = sort(MODELS_ORDER_BY, models.values(), order_by)

    for model in models:
        if model.access < access:
            continue

        if model.external:
            if dataset is None or dataset.name != model.external.dataset.name:
                dataset = model.external.dataset
                if dataset:
                    resource = None
                    yield torow(DATASET, {
                        'id': dataset.id,
                        'dataset': dataset.name,
                        'level': dataset.level,
                        'access': dataset.given.access,
                        'title': dataset.title,
                        'description': dataset.description,
                    })

            if model.external and model.external.resource and (
                resource is None or
                resource.name != model.external.resource.name
            ):
                resource = model.external.resource
                if resource:
                    backend = resource.backend
                    yield torow(DATASET, {
                        'resource': resource.name,
                        'source': resource.external,
                        'prepare': unparse(resource.prepare or NA),
                        'type': resource.type,
                        'ref': (
                            backend.name
                            if (
                                backend and
                                backend.origin != BackendOrigin.resource
                            )
                            else ''
                        ),
                        'level': resource.level,
                        'access': resource.given.access,
                        'title': resource.title,
                        'description': resource.description,
                    })

        yield torow(DATASET, {})

        data = {
            'id': model.id,
            'model': model.name,
            'level': model.level,
            'access': model.given.access,
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
            if (
                not model.external.unknown_primary_key and
                all(p.access >= access for p in model.external.pkeys)
            ):
                # Add `ref` only if all properties are available in the
                # resulting manifest.
                data['ref'] = ', '.join([
                    p.name for p in model.external.pkeys
                ])
        yield torow(DATASET, data)

        props = sort(PROPERTIES_ORDER_BY, model.properties.values(), order_by)
        for prop in props:
            if prop.name.startswith('_'):
                continue

            if prop.access < access:
                continue

            data = {
                'property': prop.place,
                'type': prop.dtype.name,
                'level': prop.level,
                'access': prop.given.access,
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
                    pkeys = prop.dtype.model.external.pkeys
                    rkeys = prop.dtype.refprops
                    if rkeys and pkeys != rkeys:
                        rkeys = ', '.join([p.place for p in rkeys])
                        data['ref'] += f'[{rkeys}]'
                else:
                    data['ref'] = prop.dtype.model.name

            yield torow(DATASET, data)
            yield from _enums_to_tabular(
                prop.enums,
                external=external,
                access=access,
                order_by=order_by,
            )


def torow(keys, values) -> ManifestRow:
    return {k: values.get(k) for k in keys}


def write_tabular_manifest(file: TextIO, rows: Iterable[ManifestRow]):
    writer = csv.DictWriter(file, fieldnames=DATASET)
    writer.writeheader()
    writer.writerows(rows)


def render_tabular_manifest(
    manifest: Manifest,
    cols: List[ManifestColumn] = None,
    *,
    sizes: Dict[ManifestColumn, int] = None,
) -> str:
    rows = datasets_to_tabular(manifest)
    return render_tabular_manifest_rows(rows, cols, sizes=sizes)


def render_tabular_manifest_rows(
    rows: Iterable[ManifestRow],
    cols: List[ManifestColumn] = None,
    *,
    sizes: Dict[ManifestColumn, int] = None,
) -> str:
    cols = cols or MANIFEST_COLUMNS
    hs = 1 if ID in cols else 0  # hierarchical cols start
    he = cols.index(PROPERTY)    # hierarchical cols end
    hsize = 1                      # hierarchical column size
    bsize = 3                      # border size
    if sizes is None:
        sizes = dict(
            [(c, len(c)) for c in cols[:hs]] +
            [(c, 1) for c in cols[hs:he]] +
            [(c, len(c)) for c in cols[he:]]
        )
        rows = list(rows)
        for row in rows:
            for i, col in enumerate(cols):
                val = '' if row[col] is None else str(row[col])
                if col == ID:
                    sizes[col] = 2
                elif i < he:
                    size = (hsize + bsize) * (he - hs - i) + sizes[PROPERTY]
                    if size < len(val):
                        sizes[PROPERTY] += len(val) - size
                elif sizes[col] < len(val):
                    sizes[col] = len(val)

    line = []
    for col in cols:
        size = sizes[col]
        line.append(col[:size].ljust(size))
    lines = [line]

    for row in rows:
        if ID in cols:
            line = [row[ID][:2] if row[ID] else '  ']
        else:
            line = []

        for i, col in enumerate(cols[hs:he + 1]):
            val = row[col] or ''
            if val:
                depth = i
                break
        else:
            val = ''
            depth = 0

        line += [' ' * hsize] * depth
        size = (hsize + bsize) * (he - hs - depth) + sizes[PROPERTY]
        line += [val.ljust(size)]

        for col in cols[he + 1:]:
            val = '' if row[col] is None else str(row[col])
            val = val.replace('|', '\\|')
            size = sizes[col]
            line.append(val.ljust(size))

        lines.append(line)

    lines = [' | '.join(line) for line in lines]
    lines = [l.rstrip() for l in lines]
    return '\n'.join(lines)


SHORT_NAMES = {
    'd': 'dataset',
    'r': 'resource',
    'b': 'base',
    'm': 'model',
    'p': 'property',
    't': 'type',
}


def normalizes_columns(cols: List[str]) -> List[ManifestColumn]:
    result: List[ManifestColumn] = []
    for col in cols:
        col = col.strip().lower()
        col = SHORT_NAMES.get(col, col)
        col = cast(ManifestColumn, col)
        if col in MANIFEST_COLUMNS:
            result.append(col)
        else:
            raise PropertyNotFound(property=col)
    return result
