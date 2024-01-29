from abc import ABC, abstractmethod
from typing import Dict, List
import sqlalchemy as sa
from spinta import commands
from spinta.components import Model, Namespace, Context
from spinta.datasets.components import Dataset
from spinta.datasets.inspect.helpers import zipitems
from spinta.exceptions import ModelNotFound, NamespaceNotFound, DatasetNotFound
from spinta.manifests.components import Manifest
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.manifests.internal_sql.helpers import internal_to_schema, load_internal_manifest_nodes, get_object_from_id, \
    select_full_table, update_schema_with_external, load_required_models, get_manifest, get_transaction_connection, \
    datasets_to_sql
from spinta.types.namespace import load_namespace_from_name
from spinta.utils.schema import NA


def get_model_name_list(context: Context, manifest: InternalSQLManifest, loaded: bool, namespace: str = None, recursive: bool = False):
    manifest = get_manifest(context, manifest)
    table = manifest.table
    conn = get_transaction_connection(context)
    objs = manifest.get_objects()
    if conn is None or loaded:
        if 'model' and objs and objs['model']:
            if namespace:
                for model_name, model in objs['model'].items():
                    if model.ns.name == namespace:
                        yield model_name
            else:
                yield from objs['model'].keys()
    else:
        if namespace == '':
            for model in objs['model'].values():
                if model.name.startswith('_'):
                    yield model.name

        if namespace:
            stmt = sa.select(table.c.path).where(
                sa.and_(
                    table.c.path.startswith(namespace),
                    table.c.dim == 'model'
                )
            )
        else:
            stmt = sa.select(table.c.path).where(
                table.c.dim == 'model'
            )
        rows = conn.execute(stmt)
        for row in rows:
            if not recursive and namespace:
                # Check if path is actually right after ns,
                # ex: namespace = 'dataset/test'
                # models: 'dataset/test/gov/Model', 'dataset/test/Model'
                # This will filter out first model, since it belongs to gov namespace
                fixed_path = row['path'].replace(namespace, '')
                if fixed_path.startswith('/'):
                    fixed_path = fixed_path[1:]
                if len(fixed_path.split('/')) == 1:
                    yield row['path']
            else:
                yield row['path']


def get_namespace_name_list(context: Context, manifest: InternalSQLManifest, loaded: bool, namespace: str = None):
    manifest = get_manifest(context, manifest)
    table = manifest.table
    conn = get_transaction_connection(context)
    if conn is None or loaded:
        objs = manifest.get_objects()
        if 'ns' and objs and objs['ns']:
            if namespace is not None:
                for ns_name, ns in objs['ns'].items():
                    if ns.parent and isinstance(ns.parent, Namespace) and ns.parent.name == namespace:
                        yield ns_name
            else:
                yield from objs['ns'].keys()
    else:
        if not namespace:
            stmt = sa.select(table.c.mpath).where(
                sa.or_(
                    table.c.dim == 'namespace',
                    table.c.dim == 'dataset'
                )
            ).order_by(table.c.mpath)
        else:
            stmt = sa.select(table.c.mpath).where(
                sa.and_(
                    table.c.mpath.startswith(namespace),
                    table.c.mpath != namespace,
                    sa.or_(
                        table.c.dim == 'namespace',
                        table.c.dim == 'dataset'
                    )
                )

            ).order_by(table.c.index)
        rows = conn.execute(stmt)
        yielded = []
        for row in rows:
            if namespace:
                # Fix namespace path, ex given namespace is 'dataset/test'
                # Fetched namespaces are: 'dataset/test/gov', 'dataset/test/other/gov'
                # it will return 'dataset/test/gov' and 'dataset/test/other'
                fixed_path = row['mpath'].replace(namespace, '')
                if fixed_path.startswith('/'):
                    fixed_path = fixed_path[1:]
                fixed_path = f'{namespace}/{fixed_path.split("/")[0]}'
                if fixed_path not in yielded:
                    yielded.append(fixed_path)
                    yield fixed_path
            else:
                yield row['mpath']


def _get_dataset_name_list(context: Context, manifest: InternalSQLManifest, loaded: bool):
    manifest = get_manifest(context, manifest)
    table = manifest.table
    conn = get_transaction_connection(context)
    if conn is None or loaded:
        objs = manifest.get_objects()
        if 'dataset' and objs and objs['dataset']:
            yield from objs['dataset'].keys()
    else:
        stmt = sa.select(table.c.path).where(
            table.c.dim == 'dataset'
        ).order_by(table.c.path)
        rows = conn.execute(stmt)
        for row in rows:
            yield row['path']


@commands.has_model.register(Context, InternalSQLManifest, str)
def has_model(context: Context, manifest: InternalSQLManifest, model: str, loaded: bool = False, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    if model in manifest.get_objects()['model']:
        return True
    elif not loaded and conn is not None:
        table = manifest.table
        ns = conn.execute(
            sa.select(table).where(
                sa.and_(
                    table.c.path == model,
                    table.c.dim == 'model'
                )
            )
        )
        if any(ns):
            return True
    return False


@commands.get_model.register(Context, InternalSQLManifest, str)
def get_model(context: Context, manifest: InternalSQLManifest, model: str, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    objects = manifest.get_objects()
    if has_model(context, manifest, model):
        if model in objects['model']:
            m = objects['model'][model]
            return m
        elif conn is not None:
            schemas = []
            table = manifest.table
            m = conn.execute(
                select_full_table(table).where(
                    sa.and_(
                        table.c.path == model,
                        table.c.dim == 'model'
                    )
                ).limit(1)
            )
            model_obj = None
            props = []
            for item in m:
                model_obj = item
                props = conn.execute(
                    select_full_table(table).where(
                        sa.and_(
                            table.c.path.startswith(model),
                            table.c.dim != 'model'
                        )
                    ).order_by(table.c.index)
                )

            parent_id = model_obj['parent']
            parent_dataset = None
            parent_resource = None
            parent_schemas = []
            while parent_id is not None:
                parent_obj = get_object_from_id(context, manifest, parent_id)
                if parent_obj is None:
                    break

                parent_schemas.append(parent_obj)
                parent_id = parent_obj['parent']

                if parent_obj['dim'] == 'dataset':
                    parent_dataset = parent_obj['name']
                    dataset = commands.get_dataset(context, manifest, parent_dataset)
                    if parent_resource:
                        get_dataset_resource(context, manifest, dataset, parent_resource)
                    break
                elif parent_obj['dim'] == 'resource' and not parent_resource:
                    parent_resource = parent_obj['name']

            schemas.extend(reversed(parent_schemas))
            schemas.append(model_obj)
            schemas.extend(props)
            required_models = [model]

            schemas = internal_to_schema(manifest, schemas)
            schemas = update_schema_with_external(schemas, {
                'dataset': parent_dataset,
                'resource': parent_resource
            })
            schemas = load_required_models(context, manifest, schemas, required_models)
            load_internal_manifest_nodes(context, manifest, schemas, link=True, ignore_types=['dataset', 'resource'])
            if model in objects['model']:
                return objects['model'][model]
    raise ModelNotFound(model=model)


@commands.get_models.register(Context, InternalSQLManifest)
def get_models(context: Context, manifest: InternalSQLManifest, loaded: bool = False, **kwargs):
    model_names = get_model_name_list(context, manifest, loaded)
    objs = manifest.get_objects()
    for name in model_names:
        # get_model loads the model if it has not been loaded
        if name not in objs['model']:
            commands.get_model(context, manifest, name)
    return objs['model']


@commands.set_model.register(Context, InternalSQLManifest, str, Model)
def set_model(context: Context, manifest: InternalSQLManifest, model_name: str, model: Model, **kwargs):
    manifest.get_objects()['model'][model_name] = model


@commands.set_models.register(Context, InternalSQLManifest, dict)
def set_models(context: Context, manifest: InternalSQLManifest, models: Dict[str, Model], **kwargs):
    manifest.get_objects()['model'] = models


@commands.has_namespace.register(Context, InternalSQLManifest, str)
def has_namespace(context: Context, manifest: InternalSQLManifest, namespace: str, loaded: bool = False, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    if namespace in manifest.get_objects()['ns']:
        return True
    elif conn is not None and not loaded:
        table = manifest.table
        ns = conn.execute(
            sa.select(table).where(table.c.mpath.startswith(namespace)).limit(1)
        )
        if any(ns):
            return True
    return False


@commands.get_namespace.register(Context, InternalSQLManifest, str)
def get_namespace(context: Context, manifest: InternalSQLManifest, namespace: str, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    objects = manifest.get_objects()

    if has_namespace(context, manifest, namespace):
        if namespace in objects['ns']:
            ns = objects['ns'][namespace]
            return ns
        elif conn is not None:
            table = manifest.table
            ns = conn.execute(
                select_full_table(table).where(
                    sa.and_(
                        table.c.name == namespace,
                        table.c.dim == 'ns'
                    )
                )
            )
            schemas = internal_to_schema(manifest, ns)
            load_internal_manifest_nodes(context, manifest, schemas, link=True)
            if namespace in objects['ns']:
                return objects['ns'][namespace]

            ns = load_namespace_from_name(context, manifest, namespace, drop=False)
            return ns

    raise NamespaceNotFound(namespace=namespace)


@commands.get_namespaces.register(Context, InternalSQLManifest)
def get_namespaces(context: Context, manifest: InternalSQLManifest, loaded: bool = False, **kwargs):
    ns_names = get_namespace_name_list(context, manifest, loaded)
    objs = manifest.get_objects()
    for name in ns_names:
        # get_namespace loads the namespace if it has not been loaded
        if name not in objs['ns']:
            commands.get_namespace(context, manifest, name)
    return objs['ns']


@commands.set_namespace.register(Context, InternalSQLManifest, str, Namespace)
def set_namespace(context: Context, manifest: InternalSQLManifest, namespace: str, ns: Namespace, **kwargs):
    manifest = get_manifest(context, manifest)
    manifest.get_objects()['ns'][namespace] = ns


@commands.has_dataset.register(Context, InternalSQLManifest, str)
def has_dataset(context: Context, manifest: InternalSQLManifest, dataset: str, loaded: bool = False, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    if dataset in manifest.get_objects()['dataset']:
        return True
    elif conn is not None and not loaded:
        table = manifest.table
        ds = conn.execute(
            sa.select(table).where(
                sa.and_(
                    table.c.mpath == dataset,
                    table.c.dim == 'dataset'
                )

            ).limit(1)
        )
        if any(ds):
            return True
    return False


def has_dataset_resource(context: Context, manifest: InternalSQLManifest, dataset: Dataset, resource: str, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    if resource in dataset.resources:
        return True
    elif conn is not None:
        table = manifest.table
        ds = conn.execute(
            sa.select(table).where(
                sa.and_(
                    table.c.path == dataset.name,
                    table.c.dim == 'resource',
                    table.c.name == resource
                )

            ).limit(1)
        )
        if any(ds):
            return True
    return False


def get_dataset_resource(context: Context, manifest: InternalSQLManifest, dataset: Dataset, resource: str, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    if has_dataset_resource(context, manifest, dataset, resource, **kwargs):
        if resource in dataset.resources:
            return dataset.resources[resource]
        elif conn is not None:
            table = manifest.table
            resources = conn.execute(
                sa.select(table).where(
                    sa.and_(
                        table.c.path == dataset.name,
                        sa.or_(
                            sa.and_(
                                table.c.name == resource,
                                table.c.dim == 'resource'
                            ),
                            table.c.dim == 'dataset'
                        )
                    )
                )
            )
            schemas = internal_to_schema(manifest, resources)
            load_internal_manifest_nodes(context, manifest, schemas, link=True)
            if resource in dataset.resources:
                return dataset.resources[resource]


@commands.get_dataset.register(Context, InternalSQLManifest, str)
def get_dataset(context: Context, manifest: InternalSQLManifest, dataset: str, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)
    objects = manifest.get_objects()

    if has_dataset(context, manifest, dataset):
        if dataset in objects['dataset']:
            return objects['dataset'][dataset]
        elif conn is not None:
            table = manifest.table
            ds = conn.execute(
                select_full_table(table).where(
                    sa.and_(
                        table.c.path == dataset,
                        table.c.dim != 'base',
                        table.c.dim != 'resource',
                    )
                )
            )
            schemas = internal_to_schema(manifest, ds)
            load_internal_manifest_nodes(context, manifest, schemas, link=True)
            if dataset in objects['dataset']:
                return objects['dataset'][dataset]

    raise DatasetNotFound(dataset=dataset)


@commands.get_datasets.register(Context, InternalSQLManifest)
def get_datasets(context: Context, manifest: InternalSQLManifest, loaded: bool = False, **kwargs):
    dataset_names = _get_dataset_name_list(context, manifest, loaded)
    objs = manifest.get_objects()
    for name in dataset_names:
        # get_dataset loads the dataset if it has not been loaded
        if name not in objs['dataset']:
            commands.get_dataset(context, manifest, name)
    return objs['dataset']


@commands.set_dataset.register(Context, InternalSQLManifest, str, Dataset)
def set_dataset(context: Context, manifest: InternalSQLManifest, dataset_name: str, dataset: Dataset, **kwargs):
    manifest.get_objects()['dataset'][dataset_name] = dataset


@commands.get_dataset_models.register(Context, InternalSQLManifest, str)
def get_dataset_models(context: Context, manifest: InternalSQLManifest, dataset_name: str, **kwargs):
    model_names = get_model_name_list(context, manifest, False, dataset_name)
    if commands.has_dataset(context, manifest, dataset_name):
        dataset = commands.get_dataset(context, manifest, dataset_name)
        filtered_list = {}
        for name in model_names:
            model = commands.get_model(context, manifest, name)
            if model.external and model.external.dataset == dataset:
                filtered_list[name] = model
        return filtered_list
    return {}


class RowMergeAction(ABC):
    row: dict

    def __init__(self, row):
        self.row = row

    @abstractmethod
    def query(self, table):
        pass

    def update_index(self, index):
        self.row['index'] = index
        return index + 1


class RowMergeInsertAction(RowMergeAction):
    def query(self, table):
        return sa.insert(table).values(self.row)


class RowMergeUpdateAction(RowMergeAction):
    def query(self, table):
        return sa.update(table).where(table.c.id == self.row['id']).values(self.row)


class RowMergeDeleteAction(RowMergeAction):
    def query(self, table):
        return sa.delete(table).where(table.c.id == self.row['id'])

    def update_index(self, index):
        return index


def _adjust_indexes(table, conn, from_index, starting_index):
    if from_index is not None:
        rows = conn.execute(
            select_full_table(table).where(
                table.c.index > from_index
            )
        )
        index = starting_index + 1
        for row in rows:
            conn.execute(
                sa.update(table).where(table.c.id == row['id']).values(
                    index=index
                )
            )
            index += 1


class RowMergeBuilder:
    actions: List[RowMergeAction]

    def __init__(self):
        self.actions = []

    def add_action(self, action: RowMergeAction):
        self.actions.append(action)

    def execute_all(self, table, conn, starting_index=0, last_index=None):
        index = starting_index

        new_last_index = None
        for action in self.actions:
            index = action.update_index(index)
            new_last_index = index

        if last_index is not None and new_last_index is not None:
            _adjust_indexes(table, conn, last_index, new_last_index)

        for action in self.actions:
            conn.execute(
                action.query(table)
            )


@commands.update_manifest_dataset_schema.register(Context, InternalSQLManifest, Manifest)
def update_manifest_dataset_schema(context: Context, manifest: InternalSQLManifest, target_manifest: Manifest, **kwargs):
    manifest = get_manifest(context, manifest)
    conn = get_transaction_connection(context)

    datasets = commands.get_datasets(context, target_manifest)
    if conn is None:
        raise Exception("NO CONNECTION")

    table = manifest.table
    target_rows = datasets_to_sql(context, target_manifest)

    for dataset in datasets.values():
        in_memory_existing_rows = []
        initial_index = None
        last_index = None
        if has_dataset(context, manifest, dataset.name):
            existing_data = conn.execute(
                select_full_table(table).where(
                    table.c.path.startswith(dataset.name),
                ).order_by(table.c.index)
            )

            for row in existing_data:
                in_memory_existing_rows.append(row)
                if initial_index is None:
                    initial_index = row['index']
                last_index = row['index']
        else:
            last_index_row = conn.execute(select_full_table(table).order_by(sa.desc(table.c.index)).limit(1))
            initial_index = 0
            for row in last_index_row:
                initial_index = row['index']

        zipped_rows = zipitems(
            target_rows,
            in_memory_existing_rows,
            _id_key
        )
        merge_builder = RowMergeBuilder()
        for rows in zipped_rows:
            for new, old in rows:
                if old is NA and new is not NA:
                    # Insert action
                    merge_builder.add_action(
                        RowMergeInsertAction(new)
                    )
                elif old is not NA and new is not NA:
                    # Update action
                    merge_builder.add_action(
                        RowMergeUpdateAction(new)
                    )
                else:
                    # Delete action
                    merge_builder.add_action(
                        RowMergeDeleteAction(old)
                    )

        with manifest.engine.begin() as merge_connection:
            merge_builder.execute_all(
                table,
                merge_connection,
                initial_index,
                last_index
            )


def _id_key(row: dict) -> str:
    return row["id"]
