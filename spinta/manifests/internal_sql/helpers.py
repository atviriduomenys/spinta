import uuid
from operator import itemgetter
from typing import Optional, List, Iterator, Dict, Any

import sqlalchemy as sa
from sqlalchemy.sql.elements import Null

from spinta.backends import Backend
from spinta.backends.components import BackendOrigin
from spinta.components import Namespace, Base, Model, Property
from spinta.core.enums import Access
from spinta.datasets.components import Dataset, Resource
from spinta.dimensions.comments.components import Comment
from spinta.dimensions.enum.components import Enums
from spinta.dimensions.lang.components import LangData
from spinta.dimensions.prefix.components import UriPrefix
from spinta.manifests.components import Manifest
from spinta.manifests.internal_sql.components import ManifestRow, MANIFEST_COLUMNS, ManifestColumn
from spinta.manifests.tabular.helpers import State, ManifestReader, READERS, ENUMS_ORDER_BY, \
    sort, MODELS_ORDER_BY, DATASETS_ORDER_BY, to_relative_model_name, PROPERTIES_ORDER_BY, _get_type_repr
from sqlalchemy_utils import UUIDType

from spinta.spyna import parse
from spinta.types.datatype import Ref
from spinta.utils.data import take
from spinta.utils.schema import NotAvailable, NA
from spinta.utils.types import is_str_uuid


def read_schema(path: str):
    engine = sa.create_engine(path)
    with engine.connect() as conn:
        yield from _read_all_sql_manifest_rows(path, conn)


def _read_all_sql_manifest_rows(
    path: Optional[str],
    conn: sa.engine.Connection,
    *,
    rename_duplicates: bool = True
):
    rows = conn.engine('SELECT * FROM _manifest')
    state = State()
    state.rename_duplicates = rename_duplicates
    reader = ManifestReader(state, path, '1')
    reader.read({})
    yield from state.release(reader)

    for row in rows:
        row[row["dim"]] = row["name"]
        dimension = row["dim"]
        Reader = READERS[dimension]
        reader = Reader(state, path, row["id"])
        reader.read(row)
        yield from state.release(reader)

    yield from state.release()


def write_internal_sql_manifest(dsn: str, manifest: Manifest):
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        meta.reflect()
        create_table = True
        if "_manifest" in meta.tables.keys():
            table = meta.tables["_manifest"]
            table.drop()
        if create_table:
            meta.clear()
            meta.reflect()
            table = sa.Table(
                '_manifest',
                meta,
                sa.Column("id", UUIDType, primary_key=True),
                sa.Column("parent", UUIDType),
                sa.Column("depth", sa.Integer),
                sa.Column("path", sa.String),
                sa.Column("mpath", sa.String),
                sa.Column("dim", sa.String),
                sa.Column("name", sa.String),
                sa.Column("type", sa.String),
                sa.Column("ref", sa.String),
                sa.Column("source", sa.String),
                sa.Column("prepare", sa.JSON),
                sa.Column("level", sa.Integer),
                sa.Column("access", sa.String),
                sa.Column("uri", sa.String),
                sa.Column("title", sa.String),
                sa.Column("description", sa.String)
            )
            table.create()

        rows = datasets_to_sql(manifest)
        for row in rows:
            conn.execute(table.insert().values(row))


def _handle_id(item_id: str):
    if item_id:
        if is_str_uuid(item_id):
            return uuid.UUID(item_id, version=4)
        else:
            raise Exception
    return uuid.uuid4()


def datasets_to_sql(
    manifest: Manifest,
    *,
    external: bool = True,  # clean content of source and prepare
    access: Access = Access.private,
    internal: bool = False,  # internal models with _ prefix like _txn
    order_by: ManifestColumn = None,
) -> Iterator[ManifestRow]:
    yield from _prefixes_to_sql(manifest.prefixes)
    yield from _backends_to_sql(manifest.backends)
    yield from _namespaces_to_sql(manifest.namespaces)

    yield from _enums_to_sql(
        manifest.enums,
        external=external,
        access=access,
        order_by=order_by)

    seen_datasets = set()
    dataset = {
        "id": None,
        "path": None,
        "mpath": None,
        "item": None,
        "depth": 0
    }
    resource = {
        "id": None,
        "path": None,
        "mpath": None,
        "item": None,
        "depth": 0
    }
    base = {
        "id": None,
        "path": None,
        "mpath": None,
        "item": None,
        "depth": 0
    }
    models = manifest.models if internal else take(manifest.models)
    models = sort(MODELS_ORDER_BY, models.values(), order_by)

    for model in models:
        if model.access < access:
            continue

        if model.external:
            if dataset["item"] is None or (model.external.dataset and dataset["item"].name != model.external.dataset.name):
                dataset["item"] = model.external.dataset
                if dataset["item"]:
                    seen_datasets.add(dataset["item"].name)
                    resource["item"] = None
                    for item in _dataset_to_sql(
                        dataset["item"],
                        external=external,
                        access=access,
                        order_by=order_by,
                    ):
                        yield item
                        if item["dim"] == "dataset":
                            dataset["id"] = item["id"]
                            dataset["path"] = item["path"]
                            dataset["mpath"] = item["mpath"]
                            dataset["depth"] = item["depth"]
            elif dataset["item"] is not None and \
                model.external.dataset is None:
                dataset["item"] = None
                resource["item"] = None
                base["item"] = None

            if external and model.external and model.external.resource and (
                resource["item"] is None or
                resource["item"].name != model.external.resource.name
            ):
                resource["item"] = model.external.resource
                if resource["item"]:
                    parent_id = None
                    depth = 0
                    path = ''
                    mpath = ''
                    if dataset["item"]:
                        parent_id = dataset["id"]
                        depth = dataset["depth"] + 1
                        path = dataset["path"]
                        mpath = dataset["mpath"]
                    for item in _resource_to_sql(
                        resource["item"],
                        external=external,
                        access=access,
                        parent_id=parent_id,
                        path=path,
                        mpath=mpath,
                        depth=depth
                    ):
                        yield item
                        if item["dim"] == "resource":
                            resource["id"] = item["id"]
                            resource["path"] = item["path"]
                            resource["mpath"] = item["mpath"]
                            resource["depth"] = item["depth"]

            elif external and \
                model.external and \
                model.external.resource is None and \
                dataset["item"] is not None and \
                resource["item"] is not None:
                base["item"] = None

        if model.base and (not base["item"] or model.base.name != base["item"].name):
            base["item"] = model.base
            parent_id = None
            depth = 0
            path = ''
            mpath = ''
            if resource["item"]:
                parent_id = resource["id"]
                depth = resource["depth"] + 1
                path = resource["path"]
                mpath = resource["mpath"]
            elif dataset["item"]:
                parent_id = dataset["id"]
                depth = dataset["depth"] + 1
                path = dataset["path"]
                mpath = dataset["mpath"]
            for item in _base_to_sql(
                base=base["item"],
                parent_id=parent_id,
                depth=depth,
                path=path,
                mpath=mpath
            ):
                yield item
                if item["dim"] == "base":
                    base["id"] = item["id"]
                    base["path"] = item["path"]
                    base["mpath"] = item["mpath"]
                    base["depth"] = item["depth"]
        elif base["item"] and not model.base:
            base["item"] = None
        parent_id = None
        depth = 0
        path = ''
        mpath = ''
        if base["item"]:
            parent_id = base["id"]
            depth = base["depth"] + 1
            path = base["path"]
            mpath = base["mpath"]
        elif resource["item"]:
            parent_id = resource["id"]
            depth = resource["depth"] + 1
            path = resource["path"]
            mpath = resource["mpath"]
        elif dataset["item"]:
            parent_id = dataset["id"]
            depth = dataset["depth"] + 1
            path = dataset["path"]
            mpath = dataset["mpath"]
        yield from _model_to_sql(
            model,
            external=external,
            access=access,
            order_by=order_by,
            parent_id=parent_id,
            depth=depth,
            path=path,
            mpath=mpath
        )

    datasets = sort(DATASETS_ORDER_BY, manifest.datasets.values(), order_by)
    for dataset in datasets:
        if dataset.name in seen_datasets:
            continue
        parent_id = None
        depth = 0
        path = ''
        mpath = ''
        for item in _dataset_to_sql(
            dataset,
            external=external,
            access=access,
            order_by=order_by,
        ):
            yield item
            if item["dim"] == "dataset":
                parent_id = item["id"]
                depth = int(item["depth"]) + 1
                path = item["path"]
                mpath = item["mpath"]

        for resource in dataset.resources.values():
            yield from _resource_to_sql(
                resource,
                parent_id=parent_id,
                depth=depth,
                path=path,
                mpath=mpath
            )


def _prefixes_to_sql(
    prefixes: Dict[str, UriPrefix],
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    for name, prefix in prefixes.items():
        item_id = _handle_id(prefix.id)
        yield torow(MANIFEST_COLUMNS, {
            'id': item_id,
            'parent': parent_id,
            'depth': depth,
            'path': path,
            'mpath': '/'.join([mpath, name] if mpath else [name]),
            'dim': 'prefix',
            'name': name,
            'type': prefix.type,
            'ref': name,
            'uri': prefix.uri,
            'title': prefix.title,
            'description': prefix.description,
            'prepare': _handle_prepare(NA)
        })


def _namespaces_to_sql(
    namespaces: Dict[str, Namespace],
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    namespaces = {
        k: ns
        for k, ns in namespaces.items() if not ns.generated
    }
    for name, ns in namespaces.items():
        item_id = _handle_id(ns.id)
        yield torow(MANIFEST_COLUMNS, {
            'id': item_id,
            'parent': parent_id,
            'depth': depth,
            'path': path,
            'mpath': '/'.join([mpath, name] if mpath else [name]),
            'dim': 'ns',
            'name': name,
            'type': ns.type,
            'ref': name,
            'title': ns.title,
            'description': ns.description,
            'prepare': _handle_prepare(NA)
        })


def _enums_to_sql(
    enums: Optional[Enums],
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None,
    external: bool = True,
    access: Access = Access.private,
    order_by: ManifestColumn = None,
) -> Iterator[ManifestRow]:
    if enums is None:
        return
    for name, enum in enums.items():
        items = sort(ENUMS_ORDER_BY, enum.values(), order_by)
        new_parent_id = _handle_id("")
        mpath_name = name if name else str(new_parent_id)
        new_mpath = '/'.join([mpath, mpath_name] if mpath else [mpath_name])
        yield torow(MANIFEST_COLUMNS, {
            'id': new_parent_id,
            'parent': parent_id,
            'depth': depth,
            'path': path,
            'mpath': new_mpath,
            'dim': 'enum',
            'name': name,
            'type': 'enum',
            'ref': name,
            'prepare': _handle_prepare(NA)
        })
        for item in items:
            if item.access is not None and item.access < access:
                continue
            new_item_id = _handle_id("")
            new_item_mpath = '/'.join([new_mpath, str(new_item_id)] if new_mpath else [str(new_item_id)])
            yield torow(MANIFEST_COLUMNS, {
                'id': new_item_id,
                'parent': new_parent_id,
                'depth': depth + 1,
                'path': path,
                'mpath': new_item_mpath,
                'dim': 'enum.item',
                'source': item.source if external else None,
                'prepare': _handle_prepare(item.prepare),
                'access': item.given.access,
                'title': item.title,
                'description': item.description,
            })
            yield from _lang_to_sql(item.lang, path=path, mpath=new_mpath, depth=depth + 2, parent_id=new_item_id)


def _lang_to_sql(
    lang: Optional[LangData],
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None,
) -> Iterator[ManifestRow]:
    if lang is None:
        return
    for name, data in sorted(lang.items(), key=itemgetter(0)):
        item_id = _handle_id("")
        yield torow(MANIFEST_COLUMNS, {
            'id': item_id,
            'parent': parent_id,
            'depth': depth + 1,
            'path': path,
            'mpath': '/'.join([mpath, name] if mpath else [name]),
            'dim': 'lang',
            'name': name,
            'type': 'lang',
            'ref': name,
            'title': data['title'],
            'description': data['description'],
            'prepare': _handle_prepare(NA)
        })


def _comments_to_sql(
    comments: Optional[List[Comment]],
    access: Access = Access.private,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    if comments is None:
        return
    for comment in comments:
        if comment.access < access:
            return
        new_id = _handle_id(comment.id)
        yield torow(MANIFEST_COLUMNS, {
            'id': new_id,
            'parent': parent_id,
            'depth': depth,
            'path': path,
            'mpath': '/'.join([mpath, str(new_id)] if mpath else [str(new_id)]),
            'dim': 'comment',
            'type': 'comment',
            'ref': comment.parent,
            'source': comment.author,
            'access': comment.given.access,
            'title': comment.created,
            'description': comment.comment,
            'prepare': _handle_prepare(NA)
        })


def _backends_to_sql(
    backends: Dict[str, Backend],
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    for name, backend in backends.items():
        new_id = _handle_id("")
        yield torow(MANIFEST_COLUMNS, {
            'id': new_id,
            'parent': parent_id,
            'depth': depth,
            'path': path,
            'mpath': '/'.join([mpath, name] if mpath else [name]),
            'dim': 'resource',
            'name': name,
            'type': backend.type,
            'source': backend.config.get('dsn'),
            'prepare': _handle_prepare(NA)
        })


def _dataset_to_sql(
    dataset: Dataset,
    parent_id: uuid.UUID = None,
    path: str = None,
    mpath: str = None,
    depth: int = 0,
    external: bool = True,
    access: Access = Access.private,
    order_by: ManifestColumn = None,
) -> Iterator[ManifestRow]:
    dataset_id = _handle_id(dataset.id)
    new_path = '/'.join([path, dataset.name] if path else [dataset.name])
    new_mpath = '/'.join([mpath, dataset.name] if mpath else [dataset.name])
    yield torow(MANIFEST_COLUMNS, {
        'id': dataset_id,
        'parent': parent_id,
        'depth': depth,
        'path': new_path,
        'mpath': new_mpath,
        'dim': 'dataset',
        'name': dataset.name,
        'dataset': dataset.name,
        'level': dataset.level,
        'access': dataset.given.access,
        'title': dataset.title,
        'description': dataset.description,
        'prepare': _handle_prepare(NA)
    })
    yield from _lang_to_sql(dataset.lang, parent_id=dataset_id, depth=depth + 1, path=new_path, mpath=new_mpath)
    yield from _prefixes_to_sql(dataset.prefixes, parent_id=dataset_id, depth=depth + 1, path=new_path, mpath=new_mpath)
    yield from _enums_to_sql(
        dataset.ns.enums,
        external=external,
        access=access,
        order_by=order_by,
        parent_id=dataset_id,
        depth=depth + 1,
        path=new_path,
        mpath=new_mpath
    )


def _params_to_sql(
    params_data: dict,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    if not params_data:
        return
    for param, values in params_data.items():
        param_base_id = _handle_id("")
        new_mpath = '/'.join([mpath, param] if mpath else [param])
        for i in range(len(values["source"])):
            new_id = _handle_id("")
            prepare = _handle_prepare(values["prepare"][i])
            if not (isinstance(values["prepare"][i], NotAvailable) and values['source'][i] is None):
                if i == 0:
                    yield torow(MANIFEST_COLUMNS, {
                        'id': param_base_id,
                        'parent': parent_id,
                        'depth': depth,
                        'path': path,
                        'mpath': new_mpath,
                        'dim': 'param',
                        'name': param,
                        'type': 'param',
                        'ref': param,
                        'source': values["source"][i],
                        'prepare': prepare,
                        'title': values["title"],
                        'description': values["description"]
                    })
                yield torow(MANIFEST_COLUMNS, {
                    'id': new_id,
                    'parent': param_base_id,
                    'depth': depth + 1,
                    'path': path,
                    'mpath': '/'.join([new_mpath, new_id] if new_mpath else [new_id]),
                    'dim': 'param.item',
                    'source': values["source"][i],
                    'prepare': prepare
                })


def _resource_to_sql(
    resource: Resource,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None,
    external: bool = True,
    access: Access = Access.private,
) -> Iterator[ManifestRow]:
    backend = resource.backend
    new_mpath = '/'.join([mpath, resource.name] if mpath else [resource.name])
    item_id = _handle_id("")
    yield torow(MANIFEST_COLUMNS, {
        'id': item_id,
        'parent': parent_id,
        'depth': depth,
        'path': path,
        'mpath': new_mpath,
        'dim': 'resource',
        'name': resource.name,
        'source': resource.external if external else None,
        'prepare': _handle_prepare(resource.prepare),
        'type': resource.type,
        'ref': (
            backend.name
            if (
                external and
                backend and
                backend.origin != BackendOrigin.resource
            )
            else None
        ),
        'level': resource.level,
        'access': resource.given.access,
        'title': resource.title,
        'description': resource.description,
    })
    yield from _params_to_sql(resource.params, parent_id=item_id, depth=depth + 1, path=path, mpath=new_mpath)
    yield from _comments_to_sql(resource.comments, access=access, parent_id=item_id, depth=depth + 1, path=path,
                                mpath=new_mpath)
    yield from _lang_to_sql(resource.lang, parent_id=item_id, depth=depth + 1, path=path, mpath=new_mpath)


def _base_to_sql(
    base: Base,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    item_id = _handle_id("")
    new_mpath = '/'.join([mpath, base.name] if mpath else [base.name])
    data = {
        'id': item_id,
        'parent': parent_id,
        'depth': depth,
        'path': path,
        'mpath': new_mpath,
        'dim': 'base',
        'name': base.name,
        'prepare': _handle_prepare(NA)
    }
    if base.pk:
        data['ref'] = ', '.join([pk.place for pk in base.pk])
    yield torow(MANIFEST_COLUMNS, data)
    yield from _lang_to_sql(base.lang, parent_id=item_id, depth=depth + 1, path=path, mpath=new_mpath)


def _model_to_sql(
    model: Model,
    external: bool = True,
    access: Access = Access.private,
    order_by: ManifestColumn = None,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    item_id = _handle_id(model.id)
    name = model.name
    if model.external and model.external.dataset:
        name = to_relative_model_name(
            model,
            model.external.dataset,
        )
    new_mpath = '/'.join([mpath, name] if mpath else [name])
    new_path = '/'.join([path, name] if path else [name])
    data = {
        'id': item_id,
        'parent': parent_id,
        'depth': depth,
        'path': new_path,
        'mpath': new_mpath,
        'dim': 'model',
        'name': name,
        'level': model.level.value if model.level else None,
        'access': model.given.access,
        'title': model.title,
        'description': model.description,
        'uri': model.uri if model.uri else None,
    }

    if external and model.external:
        data.update({
            'source': model.external.name,
            'prepare': _handle_prepare(model.external.prepare),
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

    hide_list = []
    if model.external:
        if not model.external.unknown_primary_key:
            hide_list = [model.external.pkeys]
    yield torow(MANIFEST_COLUMNS, data)
    yield from _params_to_sql(model.params, parent_id=item_id, depth=depth + 1, path=new_path, mpath=new_mpath)
    yield from _comments_to_sql(model.comments, access=access, parent_id=item_id, depth=depth + 1, path=new_path,
                                mpath=new_mpath)
    yield from _lang_to_sql(model.lang, parent_id=item_id, depth=depth + 1, path=new_path, mpath=new_mpath)
    yield from _unique_to_sql(model.unique, hide_list, parent_id=item_id, depth=depth + 1, path=new_path,
                              mpath=new_mpath)

    props = sort(PROPERTIES_ORDER_BY, model.properties.values(), order_by)
    for prop in props:
        yield from _property_to_sql(
            prop,
            external=external,
            access=access,
            order_by=order_by,
            parent_id=item_id,
            depth=depth + 1,
            path=new_path,
            mpath=new_mpath
        )


def _unique_to_sql(
    model_unique_data,
    hide_list: List,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    if not model_unique_data:
        return
    for row in model_unique_data:
        if row not in hide_list:
            item_id = _handle_id("")
            yield torow(MANIFEST_COLUMNS, {
                'id': item_id,
                'parent': parent_id,
                'depth': depth,
                'path': path,
                'mpath': '/'.join([mpath, item_id] if mpath else [item_id]),
                'dim': 'unique',
                'type': 'unique',
                'ref': ', '.join([r.name for r in row]),
                'prepare': _handle_prepare(NA)
            })


def _property_to_sql(
    prop: Property,
    external: bool = True,
    access: Access = Access.private,
    order_by: ManifestColumn = None,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[ManifestRow]:
    if prop.name.startswith('_'):
        return

    if prop.access < access:
        return

    item_id = _handle_id("")
    new_path = '/'.join([path, prop.place] if path else [prop.place])
    new_mpath = '/'.join([mpath, prop.place] if mpath else [prop.place])
    data = {
        'id': item_id,
        'parent': parent_id,
        'depth': depth,
        'path': new_path,
        'mpath': new_mpath,
        'dim': 'property',
        'name': prop.place,
        'type': _get_type_repr(prop.dtype),
        'level': prop.level.value if prop.level else None,
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
            data['prepare'] = _handle_prepare(prop.external.prepare)
    if isinstance(prop.dtype, Ref):
        model = prop.model
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
    elif prop.enum is not None:
        data['ref'] = prop.given.enum
    elif prop.unit is not None:
        data['ref'] = prop.given.unit

    yield torow(MANIFEST_COLUMNS, data)
    yield from _comments_to_sql(prop.comments, access=access, parent_id=item_id, depth=depth + 1, path=new_path,
                                mpath=new_mpath)
    yield from _lang_to_sql(prop.lang, parent_id=item_id, depth=depth + 1, path=new_path, mpath=new_mpath)
    yield from _enums_to_sql(
        prop.enums,
        external=external,
        access=access,
        order_by=order_by,
        parent_id=item_id,
        depth=depth + 1,
        path=new_path,
        mpath=new_mpath
    )


def _value_or_null(value: Any):
    if isinstance(value, Null) or value or value is False or value == 0:
        return value
    return None


def torow(keys, values) -> ManifestRow:
    return {k: _value_or_null(values.get(k)) for k in keys}


def _handle_prepare(prepare: Any):
    if isinstance(prepare, NotAvailable):
        prepare = sa.null()
    else:
        prepare = parse(prepare)
    return prepare
