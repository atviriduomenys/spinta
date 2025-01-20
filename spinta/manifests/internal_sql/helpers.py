import uuid
from operator import itemgetter
from typing import Optional, List, Iterator, Dict, Any, Tuple, Iterable

import sqlalchemy as sa
from sqlalchemy.sql.elements import Null

from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import BackendOrigin
from spinta.components import Namespace, Base, Model, Property, Context, Config, EntryId, MetaData, Action
from spinta.core.enums import Access
from spinta.core.ufuncs import Expr
from spinta.datasets.components import Dataset, Resource, Param
from spinta.dimensions.comments.components import Comment
from spinta.dimensions.enum.components import Enums
from spinta.dimensions.lang.components import LangData
from spinta.dimensions.prefix.components import UriPrefix
from spinta.exceptions import InvalidIdType
from spinta.manifests.components import Manifest, ManifestSchema
from spinta.manifests.helpers import _load_manifest
from spinta.manifests.internal_sql.commands.auth import internal_authorized, get_namespace_highest_access
from spinta.manifests.internal_sql.components import InternalManifestRow, INTERNAL_MANIFEST_COLUMNS, \
    InternalManifestColumn, InternalSQLManifest
from spinta.manifests.tabular.components import ManifestRow, MANIFEST_COLUMNS
from spinta.manifests.tabular.helpers import ENUMS_ORDER_BY, sort, MODELS_ORDER_BY, DATASETS_ORDER_BY, \
    to_relative_model_name, PROPERTIES_ORDER_BY, _get_type_repr, _read_tabular_manifest_rows
from sqlalchemy_utils import UUIDType

from spinta.nodes import get_node
from spinta.spyna import unparse
from spinta.types.datatype import ArrayBackRef, Ref, Array, BackRef, Object
from spinta.types.text.components import Text
from spinta.utils.data import take
from spinta.utils.schema import NotAvailable, NA
from spinta.utils.types import is_str_uuid


def get_manifest(context: Context, manifest: InternalSQLManifest):
    if context.has('request.manifest'):
        return context.get('request.manifest')
    return manifest


def get_transaction_connection(context: Context):
    if context.has('transaction.manifest'):
        return context.get('transaction.manifest').connection
    return None


def can_return_namespace_data(context: Context, manifest: InternalSQLManifest, full_name: str, item, parents: list, action: Action):
    if full_name.startswith('_'):
        return False
    if not internal_authorized(
        context,
        full_name,
        get_namespace_highest_access(
            context,
            manifest,
            full_name
        ),
        action,
        parents
    ):
        return False

    return True


def select_full_table(table, extra_cols=None):
    if extra_cols is None:
        extra_cols = []

    return sa.select([
        table,
        sa.literal_column("prepare IS NULL").label("prepare_is_null")],
        *extra_cols
    )


def read_initial_schema(context: Context, manifest: InternalSQLManifest):
    conn = context.get('transaction.manifest').connection
    table = manifest.table
    stmt = select_full_table(table).where(table.c.path == None).order_by(table.c.index)
    rows = conn.execute(stmt)
    yield from internal_to_schema(manifest, rows)


def internal_to_schema(manifest: InternalSQLManifest, rows):
    converted = convert_sql_to_tabular_rows(list(rows))
    yield from _read_tabular_manifest_rows(path=manifest.path, rows=converted, allow_updates=True)


def read_schema(path: str):
    engine = sa.create_engine(path)
    with engine.connect() as conn:
        yield from _read_all_sql_manifest_rows(path, conn)


def get_object_from_id(context: Context, manifest: InternalSQLManifest, uid):
    conn = context.get('transaction.manifest').connection
    table = manifest.table
    results = conn.execute(select_full_table(table).where(
        table.c.id == uid
    ).limit(1))
    for item in results:
        return item
    return None


def update_schema_with_external(schema, external: dict):
    for id_, item in schema:
        if item['type'] == 'model':
            if external['dataset']:
                if '/' not in item['name']:
                    item['name'] = f'{external["dataset"]}/{item["name"]}'
                item['external']['dataset'] = external['dataset']
            if external['resource']:
                item['external']['resource'] = external['resource']
            if item['properties']:
                for prop in item['properties'].values():
                    if 'model' in prop and prop['model']:
                        if '/' not in prop['model'] and external['dataset']:
                            prop['model'] = f'{external["dataset"]}/{prop["model"]}'
            if item['base']:
                base = item['base']
                if base['parent']:
                    if '/' not in base['parent'] and external['dataset']:
                        base['parent'] = f'{external["dataset"]}/{base["parent"]}'
        yield id_, item


def _traverse_properties(prop: dict, ds: str):
    if prop['type'] not in ['backref', 'array_backref']:
        if 'model' in prop:
            model_name = prop['model']
            if '/' in model_name or not ds:
                yield model_name
            else:
                yield f'{ds}/{model_name}'
        elif 'items' in prop:
            yield from _traverse_properties(prop['items'], ds)
        elif 'properties' in prop and isinstance(prop['properties'], dict):
            for prop_ in prop['properties'].values():
                yield from _traverse_properties(prop_, ds)


def load_required_models(context: Context, manifest: InternalSQLManifest, schema, model_list: list):
    for id_, item in schema:
        if item['type'] == 'model':
            ds = item['external']['dataset'] if item['external'] and item['external']['dataset'] else None
            if item['properties']:
                for prop in item['properties'].values():
                    for model_prop in _traverse_properties(prop, ds):
                        if model_prop not in model_list:
                            model_list.append(model_prop)
                            commands.get_model(context, manifest, model_prop)
            if item['base']:
                if item['base']['parent'] not in model_list:
                    model_list.append(item['base']['parent'])
                    commands.get_model(context, manifest, item['base']['parent'])
        yield id_, item


def get_namespace_partial_data(
    context: Context,
    manifest: InternalSQLManifest,
    namespace: str,
    parents: list,
    action: Action,
    recursive: bool = False,
):
    conn = context.get('transaction.manifest').connection
    table = manifest.table
    parents = parents.copy()
    parents.append(namespace)

    results = conn.execute(select_full_table(table).where(
        sa.and_(
            table.c.mpath.startswith(namespace),
            table.c.mpath != namespace,
            sa.or_(
                table.c.dim == 'ns',
                table.c.dim == 'dataset',
                table.c.dim == 'model'
            )
        )
    ).order_by(table.c.mpath))
    result = []
    recursive_list = []
    for item in results:
        item = item._asdict()
        if item['path'] == namespace or item['mpath'] == namespace:
            continue

        type_ = 'ns'
        if item['dim'] == 'ns' or item['dim'] == 'dataset':
            name = item['name']
        else:
            type_ = 'model'
            name = item['path']

        name = name[len(namespace):]
        if name[0] == '/':
            name = name[1:]
        split = name.split('/')
        full_name = f'{namespace}/{split[0]}' if namespace else split[0]
        if len(split) == 1:
            result.append(split[0])
            if can_return_namespace_data(context, manifest, full_name, item, parents, action=action):
                if recursive and type_ == 'ns' and full_name not in recursive_list:
                    recursive_list.append(full_name)
                yield {
                    '_type': type_,
                    'name': f'{full_name}/:ns' if type_ == 'ns' else full_name,
                    'title': item['title'] or '',
                    'description': item['description'] or ''
                }
        elif split[0] not in result:
            result.append(split[0])
            if can_return_namespace_data(context, manifest, full_name, item, parents, action=action):
                if recursive and full_name not in recursive_list:
                    recursive_list.append(full_name)
                yield {
                    '_type': 'ns',
                    'name': f'{full_name}/:ns',
                    'title': '',
                    'description': ''
                }

    if recursive and recursive_list:
        for item in recursive_list:
            yield from get_namespace_partial_data(
                context,
                manifest,
                item,
                recursive=recursive,
                parents=parents,
                action=action
            )


def load_internal_manifest_nodes(
    context: Context,
    manifest: InternalSQLManifest,
    schemas: Iterable[ManifestSchema],
    *,
    link: bool = False,
    ignore_types: list = []
) -> None:
    to_link = []
    config = context.get('config')
    for eid, schema in schemas:
        if schema.get('type') == 'manifest':
            _load_manifest(context, manifest, schema, eid, reset=False)
        else:
            node = _load_internal_manifest_node(context, config, manifest, None, eid, schema)
            if node.type not in ignore_types:
                commands.set_node(context, manifest, node.type, node.name, node)
                if link:
                    to_link.append(node)

    if to_link:
        for node in to_link:
            commands.link(context, node)


def _load_internal_manifest_node(
    context: Context,
    config: Config,
    manifest: Manifest,
    source: Optional[Manifest],
    eid: EntryId,
    data: dict,
) -> MetaData:
    node = get_node(context, config, manifest, eid, data, check=False)
    node.eid = eid
    node.type = data['type']
    node.parent = manifest
    node.manifest = manifest
    commands.load(context, node, data, manifest, source=source)
    return node


def load_internal_namespace_from_name(
    context: Context,
    manifest: InternalSQLManifest,
    path: str,
    *,
    # Drop last element from path which is usually a model name.
    drop: bool = True,
) -> Namespace:
    ns: Optional[Namespace] = None
    parent: Optional[Namespace] = None
    parts: List[str] = []
    parts_ = [p for p in path.split('/') if p]
    if drop:
        parts_ = parts_[:-1]
    objects = manifest.get_objects()
    for part in parts_:
        parts.append(part)
        name = '/'.join(parts)

        ns = objects['ns'].get(name, None)
        if ns is None:
            ns = Namespace()
            data = {
                'type': 'ns',
                'name': name,
                'title': '',
                'description': '',
            }
            commands.load(context, ns, data, manifest)
            ns.generated = True

        if parent is not None:
            if ns.name == parent.name:
                raise RuntimeError(f"Self reference in {path!r}.")

            ns.parent = parent or manifest

            if part and part not in parent.names:
                parent.names[part] = ns
        else:
            ns.parent = manifest

        parent = ns
    return ns


def get_table_structure(meta: sa.MetaData):
    table = sa.Table(
        '_manifest',
        meta,
        sa.Column("index", sa.BIGINT),
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
        sa.Column("description", sa.String),
    )
    return table


def _read_all_sql_manifest_rows(
    path: Optional[str],
    conn: sa.engine.Connection,
    *,
    rename_duplicates: bool = True
):
    meta = sa.MetaData(conn)
    inspector = sa.inspect(conn.engine)
    table_names = inspector.get_table_names()
    if '_manifest' in table_names:
        table = get_table_structure(meta)
        full_table = select_full_table(table)
        stmt = full_table.order_by(
            table.c.index
        )
        rows = conn.execute(stmt)
        converted = convert_sql_to_tabular_rows(list(rows))
        yield from _read_tabular_manifest_rows(path=path, rows=converted, rename_duplicates=rename_duplicates)


def write_internal_sql_manifest(context: Context, dsn: str, manifest: Manifest):
    engine = sa.create_engine(dsn)
    inspect = sa.inspect(engine)
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            meta = sa.MetaData(conn)
            table = get_table_structure(meta)
            if inspect.has_table('_manifest'):
                conn.execute(table.delete())
            else:
                table.create()
            rows = datasets_to_sql(context, manifest)
            index = 0
            for row in rows:
                row['index'] = index
                index += 1
                conn.execute(table.insert().values(row))

            trans.commit()
        except Exception as e:
            trans.rollback()
            raise e


def _handle_id(item_id: Any):
    if item_id:
        if is_str_uuid(item_id):
            return uuid.UUID(item_id, version=4)
        elif isinstance(item_id, uuid.UUID):
            return item_id
        else:
            raise InvalidIdType(id=item_id, id_type=type(item_id))
    return uuid.uuid4()


def _compare_bases(base_0, base_1):
    if isinstance(base_0, Base) and isinstance(base_1, Base):
        converted_0 = list(_base_to_sql(base_0))[0]
        converted_0["id"] = base_0.id
        converted_1 = list(_base_to_sql(base_1))[0]
        converted_1["id"] = base_1.id
        return converted_0 == converted_1
    return base_0 == base_1


def datasets_to_sql(
    context: Context,
    manifest: Manifest,
    *,
    external: bool = True,  # clean content of source and prepare
    access: Access = Access.private,
    internal: bool = False,  # internal models with _ prefix like _txn
    order_by: InternalManifestColumn = None,
) -> Iterator[InternalManifestRow]:
    yield from _prefixes_to_sql(manifest.prefixes)
    yield from _backends_to_sql(manifest.backends)
    yield from _namespaces_to_sql(commands.get_namespaces(context, manifest))
    yield from _enums_to_sql(
        manifest.enums,
        external=external,
        access=access,
        order_by=order_by,
    )

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
    models = commands.get_models(context, manifest)
    models = models if internal else take(models)
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
            if not model.external.dataset:
                dataset["item"] = None
                resource["item"] = None
                base["item"] = None

            if external and model.external.resource and (
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
                        depth=depth,
                    ):
                        yield item
                        if item["dim"] == "resource":
                            resource["id"] = item["id"]
                            resource["path"] = item["path"]
                            resource["mpath"] = item["mpath"]
                            resource["depth"] = item["depth"]
            elif external:
                if resource["item"] is not None and not model.external.resource:
                    resource["item"] = None
                    base["item"] = None

        if model.base and (not base["item"] or not _compare_bases(model.base, base["item"])):
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
                mpath=mpath,
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
        path = dataset["path"] if dataset["item"] else ""
        mpath = ''
        if base["item"]:
            parent_id = base["id"]
            depth = base["depth"] + 1
            mpath = base["mpath"]
        elif resource["item"]:
            parent_id = resource["id"]
            depth = resource["depth"] + 1
            mpath = resource["mpath"]
        elif dataset["item"]:
            parent_id = dataset["id"]
            depth = dataset["depth"] + 1
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

    datasets = sort(DATASETS_ORDER_BY, commands.get_datasets(context, manifest).values(), order_by)
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
) -> Iterator[InternalManifestRow]:
    for name, prefix in prefixes.items():
        item_id = _handle_id(prefix.id)
        yield to_row(INTERNAL_MANIFEST_COLUMNS, {
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
) -> Iterator[InternalManifestRow]:
    namespaces = {
        k: ns
        for k, ns in namespaces.items() if not ns.generated
    }

    for name, ns in namespaces.items():
        item_id = _handle_id(ns.id)
        yield to_row(INTERNAL_MANIFEST_COLUMNS, {
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
    order_by: InternalManifestColumn = None,
) -> Iterator[InternalManifestRow]:
    if enums is None:
        return
    for name, enum in enums.items():
        items = sort(ENUMS_ORDER_BY, enum.values(), order_by)
        new_parent_id = _handle_id("")
        mpath_name = name if name else str(new_parent_id)
        new_mpath = '/'.join([mpath, mpath_name] if mpath else [mpath_name])
        yield to_row(INTERNAL_MANIFEST_COLUMNS, {
            'id': new_parent_id,
            'parent': parent_id,
            'depth': depth,
            'path': path,
            'mpath': new_mpath,
            'dim': 'enum',
            'name': name,
            'type': 'enum',
            'ref': name,
            'prepare': _handle_prepare(NA),
        })
        for item in items:
            if item.access is not None and item.access < access:
                continue
            new_item_id = _handle_id(item.id)
            new_item_mpath = '/'.join([new_mpath, str(new_item_id)] if new_mpath else [str(new_item_id)])
            yield to_row(INTERNAL_MANIFEST_COLUMNS, {
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
                'level': item.level,
            })
            yield from _lang_to_sql(item.lang, path=path, mpath=new_item_mpath, depth=depth + 2, parent_id=new_item_id)


def _lang_to_sql(
    lang: Optional[LangData],
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None,
) -> Iterator[InternalManifestRow]:
    if lang is None:
        return
    for name, data in sorted(lang.items(), key=itemgetter(0)):
        item_id = _handle_id(data.get("id"))
        yield to_row(INTERNAL_MANIFEST_COLUMNS, {
            'id': item_id,
            'parent': parent_id,
            'depth': depth,
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
) -> Iterator[InternalManifestRow]:
    if comments is None:
        return
    for comment in comments:
        if comment.access < access:
            return
        new_id = _handle_id(comment.id)
        yield to_row(INTERNAL_MANIFEST_COLUMNS, {
            'id': new_id,
            'parent': parent_id,
            'depth': depth,
            'path': path,
            'mpath': '/'.join([mpath, str(new_id)] if mpath else [str(new_id)]),
            'name': comment.parent if comment.parent else None,
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
) -> Iterator[InternalManifestRow]:
    for name, backend in backends.items():
        new_id = _handle_id(backend.config.get("id"))
        yield to_row(INTERNAL_MANIFEST_COLUMNS, {
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
    order_by: InternalManifestColumn = None,
) -> Iterator[InternalManifestRow]:
    dataset_id = _handle_id(dataset.id)
    new_path = '/'.join([path, dataset.name] if path else [dataset.name])
    new_mpath = '/'.join([mpath, dataset.name] if mpath else [dataset.name])
    yield to_row(INTERNAL_MANIFEST_COLUMNS, {
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
    params: List[Param],
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[InternalManifestRow]:
    if not params:
        return
    for param in params:
        param_base_id = _handle_id("")
        new_mpath = '/'.join([mpath, param.name] if mpath else [param.name])
        for i, (source, prepare) in enumerate(zip(param.source, param.prepare)):
            new_id = _handle_id('')
            if param.id is not None:
                new_id = _handle_id(param.id)
            prepare = _handle_prepare(prepare)
            if i == 0:
                yield to_row(INTERNAL_MANIFEST_COLUMNS, {
                    'id': param_base_id,
                    'parent': parent_id,
                    'depth': depth,
                    'path': path,
                    'mpath': new_mpath,
                    'dim': 'param',
                    'name': param.name,
                    'type': 'param',
                    'ref': param.name,
                    'source': source,
                    'title': param.title,
                    'description': param.description
                })
            yield to_row(INTERNAL_MANIFEST_COLUMNS, {
                'id': new_id,
                'parent': param_base_id,
                'depth': depth + 1,
                'path': path,
                'mpath': '/'.join([new_mpath, str(new_id)] if new_mpath else [str(new_id)]),
                'dim': 'param.item',
                'source': source,
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
) -> Iterator[InternalManifestRow]:
    backend = resource.backend
    new_mpath = '/'.join([mpath, resource.name] if mpath else [resource.name])
    item_id = _handle_id(resource.id)
    yield to_row(INTERNAL_MANIFEST_COLUMNS, {
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
) -> Iterator[InternalManifestRow]:
    item_id = _handle_id(base.id)
    new_path = '/'.join([path, base.name] if path else [base.name])
    new_mpath = '/'.join([mpath, base.name] if mpath else [base.name])
    data = {
        'id': item_id,
        'parent': parent_id,
        'depth': depth,
        'path': new_path,
        'mpath': new_mpath,
        'dim': 'base',
        'name': base.name,
        'level': base.level.value if base.level else None,
        'prepare': _handle_prepare(NA)
    }
    if base.pk:
        data['ref'] = ', '.join([pk.place for pk in base.pk])
    yield to_row(INTERNAL_MANIFEST_COLUMNS, data)
    yield from _lang_to_sql(base.lang, parent_id=item_id, depth=depth + 1, path=new_path, mpath=new_mpath)


def _model_to_sql(
    model: Model,
    external: bool = True,
    access: Access = Access.private,
    order_by: InternalManifestColumn = None,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[InternalManifestRow]:
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
    yield to_row(INTERNAL_MANIFEST_COLUMNS, data)
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
) -> Iterator[InternalManifestRow]:
    if not model_unique_data:
        return
    for row in model_unique_data:
        if row not in hide_list:
            item_id = _handle_id("")
            yield to_row(INTERNAL_MANIFEST_COLUMNS, {
                'id': item_id,
                'parent': parent_id,
                'depth': depth,
                'path': path,
                'mpath': '/'.join([mpath, str(item_id)] if mpath else [item_id]),
                'dim': 'unique',
                'type': 'unique',
                'ref': ', '.join([r.name for r in row]),
                'prepare': _handle_prepare(NA)
            })


def _property_to_sql(
    prop: Property,
    external: bool = True,
    access: Access = Access.private,
    order_by: InternalManifestColumn = None,
    parent_id: uuid.UUID = None,
    depth: int = 0,
    path: str = None,
    mpath: str = None
) -> Iterator[InternalManifestRow]:
    if prop.name.startswith('_'):
        return

    if prop.access and prop.access < access:
        return

    item_id = _handle_id(prop.id)
    new_path = '/'.join([path, prop.name] if path else [prop.name])
    new_mpath = '/'.join([mpath, prop.name] if mpath else [prop.name])
    type_ = _get_type_repr(prop.dtype)
    data = {
        'id': item_id,
        'parent': parent_id,
        'depth': depth,
        'path': new_path,
        'mpath': new_mpath,
        'dim': 'property',
        'name': prop.given.name,
        'type': type_,
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

    yield_rows = []

    if isinstance(prop.dtype, (Array, ArrayBackRef)):
        yield_array_row = prop.dtype.items
        yield_rows.append(yield_array_row)

    elif isinstance(prop.dtype, (Ref, BackRef)):
        model = prop.model
        data['ref'] = prop.dtype.model.name

        if model.external and model.external.dataset:
            if isinstance(prop.dtype, Ref):
                pkeys = prop.dtype.model.external.pkeys
                rkeys = prop.dtype.refprops

                if rkeys and pkeys != rkeys:
                    rkeys = ', '.join([p.place for p in rkeys])
                    data['ref'] += f'[{rkeys}]'
            else:
                rkey = prop.dtype.refprop.place
                if prop.dtype.explicit:
                    data['ref'] += f'[{rkey}]'

        if prop.dtype.properties:
            for obj_prop in prop.dtype.properties.values():
                yield_rows.append(obj_prop)

    elif isinstance(prop.dtype, Object):
        for obj_prop in prop.dtype.properties.values():
            yield_rows.append(obj_prop)

    elif isinstance(prop.dtype, Text):
        for lang_prop in prop.dtype.langs.values():
            yield_rows.append(lang_prop)

    elif prop.enum is not None:
        data['ref'] = prop.given.enum
    elif prop.unit is not None:
        data['ref'] = prop.given.unit

    if data['name']:
        yield to_row(INTERNAL_MANIFEST_COLUMNS, data)
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
    if yield_rows:
        for yield_row in yield_rows:
            if not yield_row:
                continue

            yield from _property_to_sql(yield_row, external=external, access=access, order_by=order_by, parent_id=item_id, depth=depth + 1, path=new_path, mpath=new_mpath)


def _value_or_null(value: Any):
    if isinstance(value, Null) or value or value is False or value == 0:
        return value
    return None


def _value_or_empty(value: Any):
    if isinstance(value, Null) or value or value is False or value == 0:
        return value
    return ''


def to_row(keys, values) -> InternalManifestRow:
    return {k: _value_or_null(values.get(k)) for k in keys}


def to_row_tabular(keys, values) -> ManifestRow:
    result = {}
    for key in keys:
        if key == "prepare" and values.get("prepare_is_null") is not None and not values.get("prepare_is_null"):
            result[key] = unparse(values.get("prepare"))
        else:
            result[key] = _value_or_empty(values.get(key))
    return result


def _handle_prepare(prepare: Any):
    if isinstance(prepare, NotAvailable):
        prepare = sa.null()
    elif isinstance(prepare, Expr):
        prepare = prepare.todict()
    return prepare


def convert_sql_to_tabular_rows(rows: list) -> Iterator[Tuple[str, List[str]]]:
    previous_row = {}
    data_row = {}
    enum_data = {}
    param_data = {}
    meta_dimensions = {
        "current": None,
        "previous": None,
        'dataset': None,
        'resource': None,
        'base': None
    }
    yield 1, MANIFEST_COLUMNS
    line = 2
    for i, row in enumerate(rows):
        row = dict(row)
        _update_meta_dimensions(meta_dimensions, row)
        is_first = row["dim"] != previous_row.get("dim")
        base, resource, dataset = _requires_end_marker(row, meta_dimensions)
        if dataset:
            yield line, list(to_row_tabular(MANIFEST_COLUMNS, {'dataset': '/'}).values())
            line += 1
        elif resource:
            yield line, list(to_row_tabular(MANIFEST_COLUMNS, {'resource': '/'}).values())
            line += 1
        elif base:
            yield line, list(to_row_tabular(MANIFEST_COLUMNS, {'base': '/'}).values())
            line += 1
        if _requires_seperator(row, previous_row, meta_dimensions):
            yield line, list(to_row_tabular(MANIFEST_COLUMNS, {}).values())
            line += 1
        dimension = row["dim"]
        if dimension == "prefix":
            data_row = _convert_prefixes(row, is_first)
        elif dimension == "ns":
            data_row = _convert_namespaces(row, is_first)
        elif dimension == "lang":
            data_row = _convert_lang(row, is_first)
        elif dimension == "comment":
            data_row = _convert_comment(row, is_first)
        elif dimension == "resource":
            data_row = _convert_resource(row)
        elif dimension == "enum":
            enum_data = _convert_enum(row)
        elif dimension == "enum.item":
            data_row = _convert_enum_item(row, enum_data, is_first)
        elif dimension == "unique":
            data_row = _convert_unique(row)
        elif dimension == "param":
            param_data = row
        elif dimension == "param.item":
            data_row = _convert_param(row, param_data, is_first)
        elif dimension == "dataset":
            data_row = _convert_dataset(row)
        elif dimension == "base":
            data_row = _convert_base(row)
        elif dimension == "property":
            data_row = _convert_property(row)
        elif dimension == "model":
            data_row = _convert_model(row)
        previous_row = row
        if dimension != "enum" and dimension != "param":
            yield line, list(data_row.values())
            line += 1


def _update_meta_dimensions(meta_dimensions: dict, row: InternalManifestRow):
    if row["dim"] == "dataset":
        meta_dimensions["dataset"] = row["depth"]
        meta_dimensions["resource"] = None
        meta_dimensions["base"] = None
    elif row["dim"] == "resource":
        meta_dimensions["resource"] = row["depth"]
        meta_dimensions["base"] = None
    elif row["dim"] == "base":
        meta_dimensions["base"] = row["depth"]
    if meta_dimensions["current"] != row["dim"] and row["dim"] in [
        "dataset", "resource", "base", "model", "property"
    ]:
        meta_dimensions["previous"] = meta_dimensions["current"]
        meta_dimensions["current"] = row["dim"]


def _requires_end_marker(row: InternalManifestRow, meta_dimensions: dict):
    base_end_marker = False
    resource_end_marker = False
    dataset_end_marker = False
    if row["dim"] not in ["dataset", "resource", "base", "property", "enum.item", "param.item"]:
        depth = row["depth"]
        if meta_dimensions["base"] is not None:
            if depth <= meta_dimensions["base"]:
                meta_dimensions["base"] = None
                base_end_marker = True
        if meta_dimensions["resource"]:
            if depth <= meta_dimensions["resource"] is not None:
                meta_dimensions["resource"] = None
                resource_end_marker = True
        if meta_dimensions["dataset"] is not None:
            if depth <= meta_dimensions["dataset"]:
                meta_dimensions["dataset"] = None
                dataset_end_marker = True
    return base_end_marker, resource_end_marker, dataset_end_marker


def _requires_seperator(row: InternalManifestRow, previous_row: InternalManifestRow, meta_dimensions: dict):
    primary_list = (None, "dataset", "resource", "base", "model", "property")

    def is_primary_dimension(dimension: str):
        if dimension in primary_list:
            return primary_list.index(dimension)
        return -1

    if row["dim"] == previous_row.get("dim"):
        return False

    previous_dim = is_primary_dimension(previous_row.get("dim"))
    current_dim = is_primary_dimension(row["dim"])
    if previous_row.get("dim") == "base":
        return False
    elif row["dim"] == "resource":
        return False
    elif current_dim > previous_dim != -1:
        return False
    elif current_dim == -1 and previous_dim != -1:
        return False
    elif current_dim == -1 and previous_dim == -1:
        if previous_row.get("dim") == "lang" or row.get("dim") == "lang":
            return False
        if (previous_row.get("dim") == "enum" and row["dim"] == "enum.item") or (previous_row.get("dim") == "param" and row["dim"] == "param.item"):
            return False
    elif current_dim != -1 and primary_list[current_dim] == "property" and previous_dim == -1:
        return False
    elif previous_dim == -1 and meta_dimensions["previous"] == meta_dimensions["current"]:
        return False
    return True


def _convert_model(row: InternalManifestRow):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    new["model"] = row["name"]
    return new


def _convert_property(row: InternalManifestRow):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    new["property"] = row["name"]
    return new


def _convert_base(row: InternalManifestRow):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    new["base"] = row["name"]
    return new


def _convert_dataset(row: InternalManifestRow):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    new["dataset"] = row["name"]
    return new


def _convert_param(row: InternalManifestRow, param_data: InternalManifestRow, first: bool = False):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    if not first:
        new["type"] = ''
        new["ref"] = ''
        new["title"] = ''
        new["description"] = ''
    else:
        new["type"] = 'param'
        new["ref"] = _value_or_empty(param_data["ref"])
        new["title"] = _value_or_empty(param_data["title"])
        new["description"] = _value_or_empty(param_data["description"])
    return new


def _convert_unique(row: InternalManifestRow):
    return to_row_tabular(MANIFEST_COLUMNS, row)


def _convert_enum_item(row: InternalManifestRow, enum_data: InternalManifestRow, first: bool = False):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    if not first:
        new["type"] = ''
        new["ref"] = ''
    else:
        new["type"] = "enum"
        new["ref"] = enum_data["ref"]
    return new


def _convert_resource(row: InternalManifestRow):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    new["resource"] = row["name"]
    return new


def _convert_comment(row: InternalManifestRow, first: bool = False):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    if not first:
        new["type"] = ''
    return new


def _convert_lang(row: InternalManifestRow, first: bool = False):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    if not first:
        new["type"] = ''
        new["ref"] = ''
    return new


def _convert_namespaces(row: InternalManifestRow, first: bool = False):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    if not first:
        new["type"] = ''
    return new


def _convert_prefixes(row: InternalManifestRow, first: bool = False):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    if not first:
        new["type"] = ''
    return new


def _convert_enum(row: InternalManifestRow):
    new = to_row_tabular(MANIFEST_COLUMNS, row)
    new["ref"] = _value_or_empty(new["ref"])
    return new
