from typing import Iterator, Optional, Tuple, Iterable

import csv
import pathlib

from spinta import spyna
from spinta.core.enums import Access
from spinta.components import Model
from spinta.types.datatype import Ref
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.constants import DATASET


def read_tabular_manifest(
    path: pathlib.Path
) -> Iterator[Tuple[int, Optional[dict]]]:
    with path.open() as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return

        header = [h.strip().lower() for h in header]

        unknown_columns = set(header[:len(DATASET)]) - set(DATASET)
        if unknown_columns:
            unknown_columns = ', '.join(sorted(unknown_columns, key=header.index))
            raise Exception(f"Unknown columns: {unknown_columns}.")

        dataset = None
        resource = None
        base = None
        model = {}
        models = {}
        datasets = {}
        category = ['dataset', 'resource', 'base', 'model']
        defaults = {k: '' for k in DATASET}
        for i, row in enumerate(reader, 1):
            row = dict(zip(header, row))
            row = {**defaults, **row}

            if all(row[k] == '' for k in category + ['property']):
                continue

            if row['dataset']:
                if model:
                    yield model['eid'], model['schema']
                    model = None
                if dataset:
                    yield dataset['eid'], dataset['schema']
                if row['dataset'] in datasets:
                    eid = datasets[row['dataset']]
                    raise Exception(
                        f"Row {i}: dataset {row['dataset']} is already "
                        f"defined in {eid}."
                    )
                datasets[row['dataset']] = i
                dataset = {
                    'eid': i,
                    'resources': {},
                    'schema': {
                        'type': 'dataset',
                        'name': row['dataset'],
                        'id': row['id'],
                        'level': row['level'],
                        'access': row['access'],
                        'title': row['title'],
                        'description': row['description'],
                        'resources': {}
                    },
                }
                resource = None
                base = None

            elif row['resource']:
                if model:
                    yield model['eid'], model['schema']
                    model = None
                if dataset is None:
                    raise Exception(
                        f"Row {i}: dataset must be defined before resource."
                    )
                if row['resource'] in dataset['resources']:
                    eid = dataset['resources'][row['resource']]
                    raise Exception(
                        f"Row {i}: resource {row['resource']} is already "
                        f"defined in {eid}."
                    )
                resource = {
                    'eid': i,
                    'name': row['resource'],
                    'schema': {
                        'type': row['type'],
                        'backend': row['ref'],
                        'external': row['source'],
                        'level': row['level'],
                        'access': row['access'],
                        'title': row['title'],
                        'description': row['description'],
                    }
                }
                dataset['resources'][row['resource']] = i
                dataset['schema']['resources'][row['resource']] = resource['schema']
                base = None

            elif row['base']:
                if model:
                    yield model['eid'], model['schema']
                    model = None
                if resource is None:
                    raise Exception(
                        f"Row {i}: resource must be defined before base."
                    )
                base = {
                    'model': get_relative_model_name(dataset, row['base']),
                    'pk': row['ref'],
                }

            elif row['model']:
                if model:
                    yield model['eid'], model['schema']
                if dataset is not None and resource is None:
                    raise Exception(
                        f"Row {i}: resource must be defined before model."
                    )
                if row['model'] in models:
                    eid = models[row['model']]
                    raise Exception(
                        f"Row {i}: model {row['model']} is already "
                        f"defined in {eid}."
                    )
                models[row['model']] = i
                model = {
                    'eid': i,
                    'properties': {},
                    'schema': {
                        'type': 'model',
                        'name': get_relative_model_name(dataset, row['model']),
                        'id': row['id'],
                        'level': row['level'],
                        'access': row['access'],
                        'title': row['title'],
                        'description': row['description'],
                        'properties': {},
                    },
                }
                if resource is not None:
                    model['schema'].update({
                        'external': {
                            'dataset': dataset['schema']['name'],
                            'resource': resource['name'],
                            'name': row['source'],
                        },
                    })
                    if row['prepare']:
                        model['schema']['external']['prepare'] = spyna.parse(row['prepare'])
                    if row['ref']:
                        model['schema']['external']['pk'] = [
                            x.strip() for x in row['ref'].split(',')
                        ]
                else:
                    if row['prepare']:
                        model['schema']['prepare'] = spyna.parse(row['prepare'])
                if base:
                    model['base'] = base
                    base = None

            elif row['property']:
                if model is None:
                    raise Exception(
                        f"Row {i}: model must be defined before property."
                    )
                if row['property'] in model['properties']:
                    eid = model['properties'][row['properties']]
                    raise Exception(
                        f"Row {i}: property {row['property']} is already "
                        f"defined in {eid}."
                    )
                prop = {
                    'eid': i,
                    'schema': {
                        'type': row['type'],
                        'level': row['level'],
                        'access': row['access'],
                        'title': row['title'],
                        'description': row['description'],
                        'external': row['source'],
                    },
                }
                if resource is not None:
                    if row['prepare']:
                        prop['schema']['external'] = {
                            'name': row['source'],
                            'prepare': spyna.parse(row['prepare']),
                        }
                    else:
                        prop['schema']['external'] = row['source']
                    if row['ref']:
                        ref = spyna.parse(row['ref'])
                        if ref['name'] == 'filter':
                            fmodel, group = ref['args']
                        else:
                            fmodel = ref
                            group = []
                        assert fmodel['name'] == 'bind', ref
                        assert len(fmodel['args']) == 1, ref
                        fmodel = fmodel['args'][0]
                        prop['schema']['model'] = get_relative_model_name(
                            dataset,
                            fmodel,
                        )
                        if group:
                            prop['schema']['refprops'] = []
                            for p in group:
                                assert p['name'] == 'bind', ref
                                assert len(p['args']) == 1, ref
                                prop['schema']['refprops'].append(p['args'][0])
                else:
                    if row['prepare']:
                        prop['schema']['prepare'] = spyna.parse(row['prepare'])
                    if row['ref']:
                        prop['schema']['model'] = row['ref']
                model['properties'][row['property']] = i
                model['schema']['properties'][row['property']] = prop['schema']

        if dataset:
            yield dataset['eid'], dataset['schema']
        if model:
            yield model['eid'], model['schema']


def get_relative_model_name(dataset: dict, name: str) -> str:
    if name.startswith('/'):
        return name[1:]
    elif dataset is None:
        return name
    else:
        return '/'.join([
            dataset['schema']['name'],
            name,
        ])


def to_relative_model_name(base: Model, model: Model) -> str:
    """Convert absolute model `name` to relative."""
    if model.external is None:
        return model.name
    if base.external.dataset.name == model.external.dataset.name:
        prefix = base.external.dataset.name
        return model.name[len(prefix) + 1:]
    else:
        return '/' + model.name


def tabular_eid(model: Model):
    if isinstance(model.eid, int):
        return model.eid
    else:
        return 0


def datasets_to_tabular(
    manifest: Manifest,
    *,
    external: bool = True,
    access: Access = Access.private,
):
    dataset = None
    resource = None
    for model in sorted(manifest.models.values(), key=tabular_eid):
        if model.access < access:
            continue

        if external and model.external:
            if dataset is None or dataset.name != model.external.dataset.name:
                dataset = model.external.dataset
                yield torow(DATASET, {
                    'id': dataset.id,
                    'dataset': dataset.name,
                    'level': dataset.level,
                    'access': dataset.access.name,
                    'title': dataset.title,
                    'description': dataset.description,
                })

            if resource is None or resource.name != model.external.resource.name:
                resource = model.external.resource
                yield torow(DATASET, {
                    'resource': resource.name,
                    'source': resource.external,
                    'ref': resource.backend.name if resource.backend else '',
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
        if external and model.external:
            data.update({
                'model': to_relative_model_name(model, model),
                'source': model.external.name,
                'prepare': spyna.unparse(model.external.prepare) if model.external.prepare else None,
                'ref': ','.join([p.name for p in model.external.pkeys]),
            })
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
                'title': prop.title,
                'description': prop.description,
            }
            if external and prop.external:
                if isinstance(prop.external, list):
                    data['source'] = ', '.join(x.name for x in prop.external)
                    data['prepare'] = ', '.join(
                        spyna.unparse(x.prepare)
                        for x in prop.external if x.prepare
                    )
                elif prop.external:
                    data['source'] = prop.external.name
                    if prop.external.prepare:
                        data['prepare'] = spyna.unparse(prop.external.prepare)

                if isinstance(prop.dtype, Ref):
                    data['ref'] = to_relative_model_name(model, prop.dtype.model)
            else:
                if isinstance(prop.dtype, Ref):
                    data['ref'] = prop.dtype.model.name
            yield torow(DATASET, data)


def torow(keys, values):
    return {k: values.get(k) for k in keys}


def write_tabular_manifest(file: pathlib.Path, rows: Iterable[dict]):
    with file.open('w') as f:
        writer = csv.DictWriter(f, fieldnames=DATASET)
        writer.writeheader()
        writer.writerows(rows)
