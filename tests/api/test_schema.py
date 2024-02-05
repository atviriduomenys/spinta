import pathlib
import uuid
from io import BytesIO
from pathlib import Path
from typing import Union

from pytest import FixtureRequest
import pytest

from spinta.api.schema import create_migrate_rename_mapping
from spinta.backends import Backend
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.core.config import RawConfig
from spinta.formats.html.components import Cell
from spinta.manifests.internal_sql.helpers import get_table_structure
from spinta.testing.client import create_test_client
from spinta.testing.data import listdata
from spinta.testing.manifest import bootstrap_manifest
from spinta.testing.manifest import load_manifest_and_context
import sqlalchemy as sa

from spinta.testing.tabular import convert_ascii_manifest_to_csv
from spinta.testing.utils import error
from spinta.utils.schema import NA
from tests.manifests.internal_sql.test_internal import compare_sql_to_required


def boostrap_manifest_with_drop(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
    *,
    request: FixtureRequest = None,
    load_internal: bool = True,
    full_load: bool = True,
    drop_list: list = [],
    **kwargs,
):
    wipe_data = True if not drop_list else False
    context = bootstrap_manifest(rc, manifest, request=request, load_internal=load_internal, full_load=full_load, wipe_data=wipe_data, **kwargs)
    store = context.get('store')
    manifest = store.manifest
    if request:
        request.addfinalizer(lambda: clean_up_after_schema_changes(manifest.backend, drop_list))
    return context


def clean_up_after_schema_changes(
    backend: PostgreSQL, tables: list
):
    meta = backend.schema
    meta.reflect()
    drop_list = []
    for table in tables:
        if table in meta.tables:
            drop_list.append(meta.tables[table])
        changelog = get_pg_name(f'{table}{TableType.CHANGELOG.value}')
        if changelog in meta.tables:
            drop_list.append(meta.tables[changelog])
    meta.drop_all(tables=drop_list)


def test_schema_invalid_auth_scope(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/scope        |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='csv',
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    resp = app.post('/api/schema/error/scope/Country/:schema')
    assert error(resp, status=403) == "InsufficientScopeError"


def test_schema_invalid_manifest_type(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/type         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='csv',
        request=request,
        full_load=True
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])
    resp = app.post('/api/schema/error/type/Country/:schema')
    assert error(resp, status=400) == "NotSupportedManifestType"


def test_schema_invalid_path_model_and_ns(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/path         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])
    resp = app.post('/api/schema/error/path/Country/:schema')
    assert error(resp, status=400) == "InvalidSchemaUrlPath"

    resp = app.post('/api/schema/error/:schema')
    assert error(resp, status=400) == "InvalidSchemaUrlPath"


def test_schema_invalid_dataset_name(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/name         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])

    resp = app.post('/api/schema/error/n/:schema')
    assert error(resp, status=400) == "InvalidName"


def test_schema_invalid_content_type(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/content      |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])

    resp = app.post('/api/schema/error/content/:schema')
    assert error(resp, status=400) == "ModifySchemaRequiresFile"

    resp = app.post('/api/schema/error/content/:schema', headers={'content-type': 'text/plain'})
    assert error(resp, status=415) == "UnknownContentType"


def test_schema_invalid_file_size(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/size         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])

    # push 150MB file
    resp = app.post('/api/schema/error/size/:schema', content=bytes(1000000 * 150), headers={'content-type': 'text/csv'})
    assert error(resp, status=400) == "FileSizeTooLarge"


def test_schema_empty_file(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/size         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])

    csv_manifest = convert_ascii_manifest_to_csv('''
    id | d | r | b | m | property | type
    ''')

    resp = app.post('/api/schema/error/size/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
    assert error(resp, status=400) == "ModifyOneDatasetSchema"


def test_schema_dataset_missmatch(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/missmatch    |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])

    csv_manifest = convert_ascii_manifest_to_csv('''
    d | r | b | m | property      | type    | ref
    api/schema/error/match        |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''')

    resp = app.post('/api/schema/error/missmatch/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
    assert error(resp, status=400) == "DatasetNameMissmatch"


def test_schema_more_than_one_dataset(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/more         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])

    csv_manifest = convert_ascii_manifest_to_csv('''
    d | r | b | m | property      | type    | ref
    api/schema/error/more         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    api/schema/error/less         |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''')

    resp = app.post('/api/schema/error/more/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
    assert error(resp, status=400) == "ModifyOneDatasetSchema"


def test_schema_requires_ids(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = bootstrap_manifest(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/error/ids          |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
    )
    app = create_test_client(context)
    app.authorize(["spinta_schema_write"])

    csv_manifest = convert_ascii_manifest_to_csv(f'''
    id | d | r | b | m | property      | type    | ref
    {uuid.uuid4()} | api/schema/error/ids          |         |
    {uuid.uuid4()} |   |   |   | Country           |         | id
    {uuid.uuid4()} |   |   |   |   | id            | integer |
       |   |   |   |   | name          | string  |
    ''')

    resp = app.post('/api/schema/error/ids/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
    assert error(resp, status=400) == "DatasetSchemaRequiresIds"


def test_schema_create_new_dataset(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = boostrap_manifest_with_drop(
        rc, '''
    d | r | b | m | property      | type    | ref
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
        full_load=False,
        drop_list=[
            'api/schema/insert/Country'
        ]
    )
    store = context.get('store')
    manifest = store.manifest
    dsn = manifest.path
    engine = sa.create_engine(dsn)

    app = create_test_client(context)
    app.authorize(["spinta_schema_write", "spinta_getall"])

    dataset = uuid.uuid4()
    country_model = uuid.uuid4()
    country_prop_id = uuid.uuid4()
    country_prop_name = uuid.uuid4()
    csv_manifest = convert_ascii_manifest_to_csv(f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/insert             |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    ''')

    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, [])

        data = app.get('/')
        assert data.json()['_data'] == []

        app.post('api/schema/insert/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/insert', 'api/schema/insert', 'dataset', 'api/schema/insert', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/insert/Country', 'api/schema/insert/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/insert/Country/id', 'api/schema/insert/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/insert/Country/name', 'api/schema/insert/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        data = app.get('api/schema/insert')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/insert/Country',
                'title': ''
            }
        ]


def test_schema_create_new_dataset_when_not_empty(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    context = boostrap_manifest_with_drop(
        rc, '''
    d | r | b | m | property      | type    | ref
    api/schema/old                |         |
      |   |   | Country           |         | id
      |   |   |   | id            | integer |
      |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
        full_load=False,
        drop_list=[
            'api/schema/insert/Country',
            'api/schema/old/Country'
        ]
    )
    store = context.get('store')
    manifest = store.manifest
    dsn = manifest.path
    engine = sa.create_engine(dsn)

    app = create_test_client(context)
    app.authorize(["spinta_schema_write", "spinta_getall"])

    dataset = uuid.uuid4()
    country_model = uuid.uuid4()
    country_prop_id = uuid.uuid4()
    country_prop_name = uuid.uuid4()
    csv_manifest = convert_ascii_manifest_to_csv(f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/insert             |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    ''')

    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/old', 'api/schema/old', 'dataset', 'api/schema/old', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/old/Country', 'api/schema/old/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/old/Country/id', 'api/schema/old/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/old/Country/name', 'api/schema/old/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
        ]
        compare_sql_to_required(result_rows, compare_rows)

        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/old/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/old/Country',
                'title': '',
            },
        ]

        app.post('api/schema/insert/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/old', 'api/schema/old', 'dataset', 'api/schema/old', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/old/Country', 'api/schema/old/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/old/Country/id', 'api/schema/old/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/old/Country/name', 'api/schema/old/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
            [4, 4, None, 0, 'api/schema/insert', 'api/schema/insert', 'dataset', 'api/schema/insert', None, None, None, None, None, None, None, None, None],
            [5, 5, 4, 1, 'api/schema/insert/Country', 'api/schema/insert/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [6, 6, 5, 2, 'api/schema/insert/Country/id', 'api/schema/insert/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [7, 7, 5, 2, 'api/schema/insert/Country/name', 'api/schema/insert/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/insert/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/old/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/insert/Country',
                'title': '',
            },
            {
                'description': '',
                'name': 'api/schema/old/Country',
                'title': '',
            },
        ]


def test_schema_add_new_model(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = uuid.uuid4()
    country_model = uuid.uuid4()
    country_prop_id = uuid.uuid4()
    country_prop_name = uuid.uuid4()
    city_model = uuid.uuid4()
    city_prop_id = uuid.uuid4()
    city_prop_name = uuid.uuid4()
    city_prop_country = uuid.uuid4()

    context = boostrap_manifest_with_drop(
        rc, f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/add          |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
        full_load=False,
        drop_list=[
            'api/schema/add/City',
            'api/schema/add/Country'
        ]
    )
    store = context.get('store')
    manifest = store.manifest
    dsn = manifest.path
    engine = sa.create_engine(dsn)

    app = create_test_client(context)
    app.authorize(["spinta_schema_write", "spinta_getall"])

    csv_manifest = convert_ascii_manifest_to_csv(f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/add                |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    {city_model}        |   |   |   | City              |         | id
    {city_prop_id}      |   |   |   |   | id            | integer |
    {city_prop_name}    |   |   |   |   | name          | string  |
    {city_prop_country} |   |   |   |   | country       | ref     | Country
    ''')

    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/add', 'api/schema/add', 'dataset', 'api/schema/add', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/add/Country', 'api/schema/add/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/add/Country/id', 'api/schema/add/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/add/Country/name', 'api/schema/add/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
        ]
        compare_sql_to_required(result_rows, compare_rows)

        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/add/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/add/Country',
                'title': '',
            },
        ]

        app.post('api/schema/add/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/add', 'api/schema/add', 'dataset', 'api/schema/add', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/add/Country', 'api/schema/add/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/add/Country/id', 'api/schema/add/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/add/Country/name', 'api/schema/add/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
            [4, 4, 0, 1, 'api/schema/add/City', 'api/schema/add/City', 'model', 'City', None, 'id', None, None, None, None, None, None, None],
            [5, 5, 4, 2, 'api/schema/add/City/id', 'api/schema/add/City/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [6, 6, 4, 2, 'api/schema/add/City/name', 'api/schema/add/City/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
            [7, 7, 4, 2, 'api/schema/add/City/country', 'api/schema/add/City/country', 'property', 'country', 'ref', "api/schema/add/Country", None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/add/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/add/City',
                'title': '',
            },
            {
                'description': '',
                'name': 'api/schema/add/Country',
                'title': '',
            },
        ]


def test_schema_remove_model(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = uuid.uuid4()
    country_model = uuid.uuid4()
    country_prop_id = uuid.uuid4()
    country_prop_name = uuid.uuid4()
    city_model = uuid.uuid4()
    city_prop_id = uuid.uuid4()
    city_prop_name = uuid.uuid4()
    city_prop_country = uuid.uuid4()

    context = boostrap_manifest_with_drop(
        rc, f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/remove             |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    {city_model}        |   |   |   | City              |         | id
    {city_prop_id}      |   |   |   |   | id            | integer |
    {city_prop_name}    |   |   |   |   | name          | string  |
    {city_prop_country} |   |   |   |   | country       | ref     | Country
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
        full_load=False,
        drop_list=[
            'api/schema/remove/__City',
            'api/schema/remove/City',
            'api/schema/remove/Country',
        ]
    )
    store = context.get('store')
    manifest = store.manifest
    dsn = manifest.path
    engine = sa.create_engine(dsn)

    app = create_test_client(context)
    app.authorize(["spinta_schema_write", "spinta_getall"])

    csv_manifest = convert_ascii_manifest_to_csv(f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/remove             |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    ''')

    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/remove', 'api/schema/remove', 'dataset', 'api/schema/remove', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/remove/Country', 'api/schema/remove/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/remove/Country/id', 'api/schema/remove/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/remove/Country/name', 'api/schema/remove/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
            [4, 4, 0, 1, 'api/schema/remove/City', 'api/schema/remove/City', 'model', 'City', None, 'id', None, None, None, None, None, None, None],
            [5, 5, 4, 2, 'api/schema/remove/City/id', 'api/schema/remove/City/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [6, 6, 4, 2, 'api/schema/remove/City/name', 'api/schema/remove/City/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
            [7, 7, 4, 2, 'api/schema/remove/City/country', 'api/schema/remove/City/country', 'property', 'country', 'ref', "api/schema/remove/Country", None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/remove/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/remove/City',
                'title': '',
            },
            {
                'description': '',
                'name': 'api/schema/remove/Country',
                'title': '',
            },
        ]

        app.post('api/schema/remove/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/remove', 'api/schema/remove', 'dataset', 'api/schema/remove', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/remove/Country', 'api/schema/remove/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/remove/Country/id', 'api/schema/remove/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/remove/Country/name', 'api/schema/remove/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/remove/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/remove/Country',
                'title': '',
            },
        ]


def test_schema_update_model(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = uuid.uuid4()
    country_model = uuid.uuid4()
    country_prop_id = uuid.uuid4()
    country_prop_name = uuid.uuid4()
    country_prop_code = uuid.uuid4()

    context = boostrap_manifest_with_drop(
        rc, f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/update             |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    ''',
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
        full_load=True,
        drop_list=[
            'api/schema/update/Salis',
            'api/schema/update/Country'
        ]
    )
    store = context.get('store')
    manifest = store.manifest
    dsn = manifest.path
    engine = sa.create_engine(dsn)

    app = create_test_client(context)
    app.authorize(["spinta_schema_write", "spinta_getall", "spinta_insert"])

    csv_manifest = convert_ascii_manifest_to_csv(f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/update             |         |
    {country_model}     |   |   |   | Salis             |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | vardas        | string  |
    {country_prop_code} |   |   |   |   | code          | string  |
    ''')

    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/update', 'api/schema/update', 'dataset', 'api/schema/update', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/update/Country', 'api/schema/update/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/update/Country/id', 'api/schema/update/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/update/Country/name', 'api/schema/update/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
        ]
        compare_sql_to_required(result_rows, compare_rows)

        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/update/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/update/Country',
                'title': '',
            },
        ]
        app.post('api/schema/update/Country', json={
            "id": 0,
            "name": "Lietuva"
        })
        app.post('api/schema/update/Country', json={
            "id": 1,
            "name": "Latvia"
        })
        result = app.get('api/schema/update/Country')
        assert listdata(result, "id", "name") == [
            (0, "Lietuva"),
            (1, "Latvia")
        ]
        app.post('api/schema/update/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/update', 'api/schema/update', 'dataset', 'api/schema/update', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/update/Salis', 'api/schema/update/Salis', 'model', 'Salis', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/update/Salis/id', 'api/schema/update/Salis/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/update/Salis/vardas', 'api/schema/update/Salis/vardas', 'property', 'vardas', 'string', None, None, None, None, None, None, None, None],
            [4, 4, 1, 2, 'api/schema/update/Salis/code', 'api/schema/update/Salis/code', 'property', 'code', 'string', None, None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        result = app.get('api/schema/update/Salis')
        assert listdata(result, "id", "vardas", "code") == [
            (0, "Lietuva", None),
            (1, "Latvia", None)
        ]


def test_schema_update_model_multiple_times(
    tmp_path: Path,
    rc: RawConfig,
    postgresql: str,
    request: FixtureRequest,
):
    dataset = uuid.uuid4()
    country_model = uuid.uuid4()
    country_prop_id = uuid.uuid4()
    country_prop_name = uuid.uuid4()
    country_prop_code = uuid.uuid4()

    original_manifest = f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/update             |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    '''

    original_manifest_v2 = convert_ascii_manifest_to_csv(f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/update             |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | code          | string  |
    ''')

    context = boostrap_manifest_with_drop(
        rc,
        original_manifest,
        backend=postgresql,
        tmp_path=tmp_path,
        manifest_type='internal_sql',
        request=request,
        full_load=True,
        drop_list=[
            'api/schema/update/Salis',
            'api/schema/update/Country'
        ]
    )
    store = context.get('store')
    manifest = store.manifest
    dsn = manifest.path
    engine = sa.create_engine(dsn)

    app = create_test_client(context)
    app.authorize(["spinta_schema_write", "spinta_getall", "spinta_insert"])

    csv_manifest = convert_ascii_manifest_to_csv(f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/update             |         |
    {country_model}     |   |   |   | Salis             |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | vardas        | string  |
    {country_prop_code} |   |   |   |   | code          | string  |
    ''')

    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/update', 'api/schema/update', 'dataset', 'api/schema/update', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/update/Country', 'api/schema/update/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/update/Country/id', 'api/schema/update/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/update/Country/name', 'api/schema/update/Country/name', 'property', 'name', 'string', None, None, None, None, None, None, None, None],
        ]
        compare_sql_to_required(result_rows, compare_rows)

        data = app.get('/api/schema/:ns/:all')
        assert data.json()['_data'] == [
            {
                'description': '',
                'name': 'api/schema/update/:ns',
                'title': ''
            },
            {
                'description': '',
                'name': 'api/schema/update/Country',
                'title': '',
            },
        ]
        app.post('api/schema/update/Country', json={
            "id": 0,
            "name": "Lietuva"
        })
        app.post('api/schema/update/Country', json={
            "id": 1,
            "name": "Latvia"
        })
        result = app.get('api/schema/update/Country')
        assert listdata(result, "id", "name") == [
            (0, "Lietuva"),
            (1, "Latvia")
        ]
        app.post('api/schema/update/:schema', content=csv_manifest, headers={'content-type': 'text/csv'})
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/update', 'api/schema/update', 'dataset', 'api/schema/update', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/update/Salis', 'api/schema/update/Salis', 'model', 'Salis', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/update/Salis/id', 'api/schema/update/Salis/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/update/Salis/vardas', 'api/schema/update/Salis/vardas', 'property', 'vardas', 'string', None, None, None, None, None, None, None, None],
            [4, 4, 1, 2, 'api/schema/update/Salis/code', 'api/schema/update/Salis/code', 'property', 'code', 'string', None, None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        result = app.get('api/schema/update/Salis')
        assert listdata(result, "id", "vardas", "code") == [
            (0, "Lietuva", None),
            (1, "Latvia", None)
        ]

        app.post('api/schema/update/:schema', content=original_manifest_v2, headers={'content-type': 'text/csv'})
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))

        compare_rows = [
            [0, 0, None, 0, 'api/schema/update', 'api/schema/update', 'dataset', 'api/schema/update', None, None, None, None, None, None, None, None, None],
            [1, 1, 0, 1, 'api/schema/update/Country', 'api/schema/update/Country', 'model', 'Country', None, 'id', None, None, None, None, None, None, None],
            [2, 2, 1, 2, 'api/schema/update/Country/id', 'api/schema/update/Country/id', 'property', 'id', 'integer', None, None, None, None, None, None, None, None],
            [3, 3, 1, 2, 'api/schema/update/Country/code', 'api/schema/update/Country/code', 'property', 'code', 'string', None, None, None, None, None, None, None, None],
        ]

        compare_sql_to_required(result_rows, compare_rows)
        result = app.get('api/schema/update/Country')
        assert listdata(result, "id", "code", "name") == [
            (0, "Lietuva", NA),
            (1, "Latvia", NA)
        ]


def test_schema_create_migrate_rename_mapping(
    rc: RawConfig,
):
    dataset = uuid.uuid4()
    country_model = uuid.uuid4()
    country_prop_id = uuid.uuid4()
    country_prop_name = uuid.uuid4()
    city_model = uuid.uuid4()
    city_prop_id = uuid.uuid4()
    city_prop_name = uuid.uuid4()
    city_prop_country = uuid.uuid4()

    old_context, old_manifest = load_manifest_and_context(
        rc, f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/error/ids          |         |
    {country_model}     |   |   |   | Country           |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | name          | string  |
    {city_model}        |   |   |   | City              |         | id
    {city_prop_id}      |   |   |   |   | id            | integer |
    {city_prop_name}    |   |   |   |   | name          | string  |
    {city_prop_country} |   |   |   |   | country       | ref     | Country
    '''
    )

    new_context, new_manifest = load_manifest_and_context(
        rc, f'''
    id                  | d | r | b | m | property      | type    | ref
    {dataset}           | api/schema/error/ids          |         |
    {country_model}     |   |   |   | Salis             |         | id
    {country_prop_id}   |   |   |   |   | id            | integer |
    {country_prop_name} |   |   |   |   | pavadinimas   | string  |
    {city_model}        |   |   |   | Miestas           |         | id
    {city_prop_id}      |   |   |   |   | id            | integer |
    {city_prop_name}    |   |   |   |   | pavadinimas   | string  |
    {city_prop_country} |   |   |   |   | salis         | ref     | Salis
    '''
    )

    rename_data = create_migrate_rename_mapping(old_context, new_context, old_manifest, new_manifest, dataset_name="api/schema/error/ids")
    assert rename_data == {
        'api/schema/error/ids/Country': {
            '': 'api/schema/error/ids/Salis',
            'name': 'pavadinimas'
        },
        'api/schema/error/ids/City': {
            '': 'api/schema/error/ids/Miestas',
            'name': 'pavadinimas',
            'country': 'salis'
        }
    }
