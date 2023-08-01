import pathlib
import re
import uuid

import pytest

from spinta.core.config import RawConfig
from spinta.manifests.internal_sql.helpers import write_internal_sql_manifest, get_table_structure
from spinta.testing.datasets import Sqlite
from tests.manifests.test_manifest import setup_tabular_manifest

import sqlalchemy as sa

db_type = {
    "sqlite": "sqlite",
    "postgresql": "postgresql"
}

pattern = re.compile(r'\{(\d+)\}')


def extract_integers_in_brackets(input_string):
    integers_list = re.findall(pattern, input_string)
    return [int(i) for i in integers_list]


def compare_sql_to_required(sql_rows: list, required_rows: list):
    for i, row in enumerate(sql_rows):
        converted_row = required_rows[i]
        if isinstance(converted_row[0], int):
            converted_row[0] = sql_rows[converted_row[0]][0]
        if isinstance(converted_row[1], int):
            converted_row[1] = sql_rows[converted_row[1]][0]

        if "{" in converted_row[4]:
            new_mpath = converted_row[4]
            values = extract_integers_in_brackets(converted_row[4])
            for value in values:
                new_mpath = new_mpath.replace("{" + str(value) + "}", str(sql_rows[value][0]))
            converted_row[4] = new_mpath

        assert row == converted_row


@pytest.mark.parametrize("db_type", db_type.values(), ids=db_type.keys())
def test_internal_store_meta_rows(
    db_type: str,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    postgresql: str
):
    table = f'''
    d | resource | b | m | property | type   | ref                  | source                  | prepare | access | uri                         | title               | description
      |          |   |   |          | ns     | datasets             |                         |         |        |                             | All datasets        | All external datasets.
      |          |   |   |          |        | datasets/gov         |                         |         |        |                             | Government datasets | All government datasets.
      |          |   |   |          |        | datasets/gov/example |                         |         |        |                             | Example             |
      |          |   |   |          |        |                      |                         |         |        |                             |                     |
      |          |   |   |          | enum   | side                 | l                       | 'left'  | open   |                             | Left                | Left side.
      |          |   |   |          |        |                      | r                       | 'right' | open   |                             | Right               | Right side.
      |          |   |   |          |        |                      |                         |         |        |                             |                     |
      | default  |   |   |          | sql    |                      | sqlite:///{tmp_path}/db |         |        |                             |                     |
      |          |   |   |          |        |                      |                         |         |        |                             |                     | 
      |          |   |   |          | prefix | locn                 |                         |         |        | http://www.w3.org/ns/locn#  |                     |
      |          |   |   |          |        | ogc                  |                         |         |        | http://www.opengis.net/rdf# |                     |

    '''
    tabular_manifest = setup_tabular_manifest(rc, tmp_path, table)
    if db_type == "sqlite":
        dsn = 'sqlite:///' + str(tmp_path / 'db.sqlite')
        db = Sqlite(dsn)
        with db.engine.connect():
            write_internal_sql_manifest(db.dsn, tabular_manifest)
    else:
        dsn = postgresql
        write_internal_sql_manifest(dsn, tabular_manifest)

    compare_rows = [
        [0, None, 0, None, 'locn', 'prefix', 'locn', 'prefix', 'locn', None, None, None, None, 'http://www.w3.org/ns/locn#', None, None],
        [1, None, 0, None, 'ogc', 'prefix', 'ogc', 'prefix', 'ogc', None, None, None, None, 'http://www.opengis.net/rdf#', None, None],
        [2, None, 0, None, 'default', 'resource', 'default', 'sql', None, f'sqlite:///{tmp_path}/db', None, None, None, None, None, None],
        [3, None, 0, None, 'datasets', 'ns', 'datasets', 'ns', 'datasets', None, None, None, None, None, 'All datasets', 'All external datasets.'],
        [4, None, 0, None, 'datasets/gov', 'ns', 'datasets/gov', 'ns', 'datasets/gov', None, None, None, None, None, 'Government datasets', 'All government datasets.'],
        [5, None, 0, None, 'datasets/gov/example', 'ns', 'datasets/gov/example', 'ns', 'datasets/gov/example', None, None, None, None, None, 'Example', None],
        [6, None, 0, None, 'side', 'enum', 'side', 'enum', 'side', None, None, None, None, None, None, None],
        [7, 6, 1, None, 'side/{7}', 'enum.item', None, None, None, 'l', 'left', None, 'open', None, 'Left', 'Left side.'],
        [8, 6, 1, None, 'side/{8}', 'enum.item', None, None, None, 'r', 'right', None, 'open', None, 'Right', 'Right side.']
    ]

    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, compare_rows)


@pytest.mark.parametrize("db_type", db_type.values(), ids=db_type.keys())
def test_internal_store_dataset_rows(
    db_type: str,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    postgresql: str
):
    table = f'''
    dataset              | r | b | m    | property | type    | ref  | uri                         | title    | description
    datasets/gov/example |   |   |      |          |         |      |                             |          |
                         |   |   |      |          | lang    | lt   |                             | Pavyzdys | Pavyzdinis duomenų rinkinys.
                         |   |   |      |          |         |      |                             |          |  
                         |   |   |      |          | prefix  | locn | http://www.w3.org/ns/locn#  |          |
                         |   |   |      |          |         | ogc  | http://www.opengis.net/rdf# |          |
                         |   |   |      |          |         |      |                             |          |
                         |   |   | Test |          |         |      |                             |          |
                         |   |   |      | integer  | integer |      |                             |          |
                         |   |   |      |          |         |      |                             |          |
    datasets/gov/new     |   |   |      |          |         |      |                             |          |
                         |   |   |      |          |         |      |                             |          |
                         |   |   | New  |          |         |      |                             |          |
                         |   |   |      | new_str  | string  |      |                             |          |
                         |   |   |      |          |         |      |                             |          |
    /                    |   |   |      |          |         |      |                             |          |
                         |   |   |      |          |         |      |                             |          |
                         |   |   | One  |          |         |      |                             |          |
                         |   |   |      | one_str  | string  |      |                             |          |

    '''
    tabular_manifest = setup_tabular_manifest(rc, tmp_path, table)
    if db_type == "sqlite":
        dsn = 'sqlite:///' + str(tmp_path / 'db.sqlite')
        db = Sqlite(dsn)
        with db.engine.connect():
            write_internal_sql_manifest(db.dsn, tabular_manifest)
    else:
        dsn = postgresql
        write_internal_sql_manifest(dsn, tabular_manifest)

    compare_rows = [
        [0, None, 0, 'datasets/gov/example', 'datasets/gov/example', 'dataset', 'datasets/gov/example', None, None, None, None, None, None, None, None, None],
        [1, 0, 1, 'datasets/gov/example', 'datasets/gov/example/lt', 'lang', 'lt', 'lang', 'lt', None, None, None, None, None, 'Pavyzdys', 'Pavyzdinis duomenų rinkinys.'],
        [2, 0, 1, 'datasets/gov/example', 'datasets/gov/example/locn', 'prefix', 'locn', 'prefix', 'locn', None, None, None, None, 'http://www.w3.org/ns/locn#', None, None],
        [3, 0, 1, 'datasets/gov/example', 'datasets/gov/example/ogc', 'prefix', 'ogc', 'prefix', 'ogc', None, None, None, None, 'http://www.opengis.net/rdf#', None, None],
        [4, 0, 1, 'datasets/gov/example/Test', 'datasets/gov/example/Test', 'model', 'Test', None, None, None, None, None, None, None, None, None],
        [5, 4, 2, 'datasets/gov/example/Test/integer', 'datasets/gov/example/Test/integer', 'property', 'integer', 'integer', None, None, None, None, None, None, None, None],
        [6, None, 0, 'datasets/gov/new', 'datasets/gov/new', 'dataset', 'datasets/gov/new', None, None, None, None, None, None, None, None, None],
        [7, 6, 1, 'datasets/gov/new/New', 'datasets/gov/new/New', 'model', 'New', None, None, None, None, None, None, None, None, None],
        [8, 7, 2, 'datasets/gov/new/New/new_str', 'datasets/gov/new/New/new_str', 'property', 'new_str', 'string', None, None, None, None, None, None, None, None],
        [9, None, 0, 'One', 'One', 'model', 'One', None, None, None, None, None, None, None, None, None],
        [10, 9, 1, 'One/one_str', 'One/one_str', 'property', 'one_str', 'string', None, None, None, None, None, None, None, None]
    ]

    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, compare_rows)


@pytest.mark.parametrize("db_type", db_type.values(), ids=db_type.keys())
def test_internal_store_resource_rows(
    db_type: str,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    postgresql: str
):
    table = f'''
    dataset              | r       | b | m    | property | type    | ref | source                   | title |description
    datasets/gov/example |         |   |      |          |         |     |                          |       |
                         | default |   |      |          | sql     |     | sqlite:///{tmp_path}/db  |       |
                         |         |   |      |          | lang    | lt  |                          |       |
                         |         |   |      |          |         |     |                          |       |
                         |         |   | Test |          |         |     |                          |       |
                         |         |   |      | integer  | integer |     |                          |       |
                         |         |   |      |          |         |     |                          |       |
                         | /       |   |      |          |         |     |                          |       |
                         |         |   |      |          |         |     |                          |       |
                         |         |   | New  |          |         |     |                          |       |
                         |         |   |      | new_str  | string  |     |                          |       |
                         |         |   |      |          |         |     |                          |       |
                         | res     |   |      |          | sql     |     | sqlite:///{tmp_path}/res |       |
                         |         |   |      |          | comment | NEW |                          | NEW   | TEST
                         |         |   |      |          |         |     |                          |       |
                         |         |   | One  |          |         |     |                          |       |
                         |         |   |      | one_str  | string  |     |                          |       |

    '''
    tabular_manifest = setup_tabular_manifest(rc, tmp_path, table)
    if db_type == "sqlite":
        dsn = 'sqlite:///' + str(tmp_path / 'db.sqlite')
        db = Sqlite(dsn)
        with db.engine.connect():
            write_internal_sql_manifest(db.dsn, tabular_manifest)
    else:
        dsn = postgresql
        write_internal_sql_manifest(dsn, tabular_manifest)

    compare_rows = [
        [0, None, 0, 'datasets/gov/example', 'datasets/gov/example', 'dataset', 'datasets/gov/example', None, None, None, None, None, None, None, None, None],
        [1, 0, 1, 'datasets/gov/example', 'datasets/gov/example/default', 'resource', 'default', 'sql', None, f'sqlite:///{tmp_path}/db', None, None, None, None, None, None],
        [2, 1, 2, 'datasets/gov/example', 'datasets/gov/example/default/lt', 'lang', 'lt', 'lang', 'lt', None, None, None, None, None, None, None],
        [3, 1, 2, 'datasets/gov/example/Test', 'datasets/gov/example/default/Test', 'model', 'Test', None, None, None, None, None, None, None, None, None],
        [4, 3, 3, 'datasets/gov/example/Test/integer', 'datasets/gov/example/default/Test/integer', 'property', 'integer', 'integer', None, None, None, None, None, None, None, None],
        [5, 0, 1, 'datasets/gov/example/New', 'datasets/gov/example/New', 'model', 'New', None, None, None, None, None, None, None, None, None],
        [6, 5, 2, 'datasets/gov/example/New/new_str', 'datasets/gov/example/New/new_str', 'property', 'new_str', 'string', None, None, None, None, None, None, None, None],
        [7, 0, 1, 'datasets/gov/example', 'datasets/gov/example/res', 'resource', 'res', 'sql', None, f'sqlite:///{tmp_path}/res', None, None, None, None, None, None],
        [8, 7, 2, 'datasets/gov/example', 'datasets/gov/example/res/{8}', 'comment', 'NEW', 'comment', 'NEW', None, None, None, None, None, 'NEW', 'TEST'],
        [9, 7, 2, 'datasets/gov/example/One', 'datasets/gov/example/res/One', 'model', 'One', None, None, None, None, None, None, None, None, None],
        [10, 9, 3, 'datasets/gov/example/One/one_str', 'datasets/gov/example/res/One/one_str', 'property', 'one_str', 'string', None, None, None, None, None, None, None, None]
    ]

    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, compare_rows)


@pytest.mark.parametrize("db_type", db_type.values(), ids=db_type.keys())
def test_internal_store_base_rows(
    db_type: str,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    postgresql: str
):
    table = f'''
    dataset              | r | base | m    | property | type    | ref | source                   | title |description
    datasets/gov/example |   |      |      |          |         |     |                          |       |
                         |   |      |      |          |         |     |                          |       |
                         |   |      | Test |          |         |     |                          |       |
                         |   |      |      | integer  | integer |     |                          |       |
                         |   |      |      |          |         |     |                          |       |
                         |   | Test |      |          |         |     |                          |       |
                         |   |      | New  |          |         |     |                          |       |
                         |   |      |      | new_str  | string  |     |                          |       |
                         |   |      |      | integer  |         |     |                          |       |
                         |   |      |      |          |         |     |                          |       |
                         |   | New  |      |          |         |     |                          |       |
                         |   |      | One  |          |         |     |                          |       |
                         |   |      |      | one_str  | string  |     |                          |       |
                         |   | /    |      |          |         |     |                          |       |
                         |   |      | Two  |          |         |     |                          |       |
                         |   |      |      | one_str  | string  |     |                          |       |

    '''
    tabular_manifest = setup_tabular_manifest(rc, tmp_path, table)
    if db_type == "sqlite":
        dsn = 'sqlite:///' + str(tmp_path / 'db.sqlite')
        db = Sqlite(dsn)
        with db.engine.connect():
            write_internal_sql_manifest(db.dsn, tabular_manifest)
    else:
        dsn = postgresql
        write_internal_sql_manifest(dsn, tabular_manifest)

    compare_rows = [
        [0, None, 0, 'datasets/gov/example', 'datasets/gov/example', 'dataset', 'datasets/gov/example', None, None, None, None, None, None, None, None, None],
        [1, 0, 1, 'datasets/gov/example/Test', 'datasets/gov/example/Test', 'model', 'Test', None, None, None, None, None, None, None, None, None],
        [2, 1, 2, 'datasets/gov/example/Test/integer', 'datasets/gov/example/Test/integer', 'property', 'integer', 'integer', None, None, None, None, None, None, None, None],
        [3, 0, 1, 'datasets/gov/example/Test', 'datasets/gov/example/Test', 'base', 'Test', None, None, None, None, None, None, None, None, None],
        [4, 3, 2, 'datasets/gov/example/New', 'datasets/gov/example/Test/New', 'model', 'New', None, None, None, None, None, None, None, None, None],
        [5, 4, 3, 'datasets/gov/example/New/new_str', 'datasets/gov/example/Test/New/new_str', 'property', 'new_str', 'string', None, None, None, None, None, None, None, None],
        [6, 4, 3, 'datasets/gov/example/New/integer', 'datasets/gov/example/Test/New/integer', 'property', 'integer', None, None, None, None, None, None, None, None, None],
        [7, 0, 1, 'datasets/gov/example/New', 'datasets/gov/example/New', 'base', 'New', None, None, None, None, None, None, None, None, None],
        [8, 7, 2, 'datasets/gov/example/One', 'datasets/gov/example/New/One', 'model', 'One', None, None, None, None, None, None, None, None, None],
        [9, 8, 3, 'datasets/gov/example/One/one_str', 'datasets/gov/example/New/One/one_str', 'property', 'one_str', 'string', None, None, None, None, None, None, None, None],
        [10, 0, 1, 'datasets/gov/example/Two', 'datasets/gov/example/Two', 'model', 'Two', None, None, None, None, None, None, None, None, None],
        [11, 10, 2, 'datasets/gov/example/Two/one_str', 'datasets/gov/example/Two/one_str', 'property', 'one_str', 'string', None, None, None, None, None, None, None, None]
    ]

    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, compare_rows)


@pytest.mark.parametrize("db_type", db_type.values(), ids=db_type.keys())
def test_internal_store_properties_rows(
    db_type: str,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    postgresql: str
):
    table = f'''
    dataset              | r | b | m    | property     | type     | ref  | prepare
    datasets/gov/example |   |   |      |              |          |      |
                         |   |   |      |              |          |      |
                         |   |   | Test |              |          |      |
                         |   |   |      | integer      | integer  |      |
                         |   |   |      |              |          |      |
                         |   |   | New  |              |          |      |
                         |   |   |      | new_str      | string   |      |
                         |   |   |      | new_int      | integer  |      |
                         |   |   |      | new_float    | number   |      |
                         |   |   |      | new_time     | time     |      |
                         |   |   |      | new_date     | date     |      |
                         |   |   |      | new_datetime | datetime |      |
                         |   |   |      | new_bool     | boolean  |      |
                         |   |   |      | new_bin      | binary   |      |
                         |   |   |      | new_geo      | geometry |      |
                         |   |   |      | new_file     | file     |      | file()
                         |   |   |      | new_ref      | ref      | Test |
                         |   |   |      | new_url      | url      |      |
                         |   |   |      | new_uri      | uri      |      |
    '''
    tabular_manifest = setup_tabular_manifest(rc, tmp_path, table)
    if db_type == "sqlite":
        dsn = 'sqlite:///' + str(tmp_path / 'db.sqlite')
        db = Sqlite(dsn)
        with db.engine.connect():
            write_internal_sql_manifest(db.dsn, tabular_manifest)
    else:
        dsn = postgresql
        write_internal_sql_manifest(dsn, tabular_manifest)

    compare_rows = [
        [0, None, 0, 'datasets/gov/example', 'datasets/gov/example', 'dataset', 'datasets/gov/example', None, None, None, None, None, None, None, None, None],
        [1, 0, 1, 'datasets/gov/example/Test', 'datasets/gov/example/Test', 'model', 'Test', None, None, None, None, None, None, None, None, None],
        [2, 1, 2, 'datasets/gov/example/Test/integer', 'datasets/gov/example/Test/integer', 'property', 'integer', 'integer', None, None, None, None, None, None, None, None],
        [3, 0, 1, 'datasets/gov/example/New', 'datasets/gov/example/New', 'model', 'New', None, None, None, None, None, None, None, None, None],
        [4, 3, 2, 'datasets/gov/example/New/new_str', 'datasets/gov/example/New/new_str', 'property', 'new_str', 'string', None, None, None, None, None, None, None, None],
        [5, 3, 2, 'datasets/gov/example/New/new_int', 'datasets/gov/example/New/new_int', 'property', 'new_int', 'integer', None, None, None, None, None, None, None, None],
        [6, 3, 2, 'datasets/gov/example/New/new_float', 'datasets/gov/example/New/new_float', 'property', 'new_float', 'number', None, None, None, None, None, None, None, None],
        [7, 3, 2, 'datasets/gov/example/New/new_time', 'datasets/gov/example/New/new_time', 'property', 'new_time', 'time', None, None, None, None, None, None, None, None],
        [8, 3, 2, 'datasets/gov/example/New/new_date', 'datasets/gov/example/New/new_date', 'property', 'new_date', 'date', None, None, None, None, None, None, None, None],
        [9, 3, 2, 'datasets/gov/example/New/new_datetime', 'datasets/gov/example/New/new_datetime', 'property', 'new_datetime', 'datetime', None, None, None, None, None, None, None, None],
        [10, 3, 2, 'datasets/gov/example/New/new_bool', 'datasets/gov/example/New/new_bool', 'property', 'new_bool', 'boolean', None, None, None, None, None, None, None, None],
        [11, 3, 2, 'datasets/gov/example/New/new_bin', 'datasets/gov/example/New/new_bin', 'property', 'new_bin', 'binary', None, None, None, None, None, None, None, None],
        [12, 3, 2, 'datasets/gov/example/New/new_geo', 'datasets/gov/example/New/new_geo', 'property', 'new_geo', 'geometry', None, None, None, None, None, None, None, None],
        [13, 3, 2, 'datasets/gov/example/New/new_file', 'datasets/gov/example/New/new_file', 'property', 'new_file', 'file', None, None, {"name": "file", "args": []}, None, None, None, None, None],
        [14, 3, 2, 'datasets/gov/example/New/new_ref', 'datasets/gov/example/New/new_ref', 'property', 'new_ref', 'ref', 'Test', None, None, None, None, None, None, None],
        [15, 3, 2, 'datasets/gov/example/New/new_url', 'datasets/gov/example/New/new_url', 'property', 'new_url', 'url', None, None, None, None, None, None, None, None],
        [16, 3, 2, 'datasets/gov/example/New/new_uri', 'datasets/gov/example/New/new_uri', 'property', 'new_uri', 'uri', None, None, None, None, None, None, None, None]
    ]

    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, compare_rows)


@pytest.mark.parametrize("db_type", db_type.values(), ids=db_type.keys())
def test_internal_store_json_null_rows(
    db_type: str,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    postgresql: str
):
    table = f'''
    d | r | b | m | property | type   | ref  | source | prepare
      |   |   |   |          | enum   | side |        | null
      |   |   |   |          |        |      | l      | 'left'
      |   |   |   |          |        |      | r      | 'right'
    '''
    tabular_manifest = setup_tabular_manifest(rc, tmp_path, table)
    if db_type == "sqlite":
        dsn = 'sqlite:///' + str(tmp_path / 'db.sqlite')
        db = Sqlite(dsn)
        with db.engine.connect():
            write_internal_sql_manifest(db.dsn, tabular_manifest)
    else:
        dsn = postgresql
        write_internal_sql_manifest(dsn, tabular_manifest)

    compare_rows = [
        [0, None, 0, None, 'side', 'enum', 'side', 'enum', 'side', None, None, None, None, None, None, None, 1],
        [1, 0, 1, None, 'side/{1}', 'enum.item', None, None, None, None, None, None, None, None, None, None, 0],
        [2, 0, 1, None, 'side/{2}', 'enum.item', None, None, None, 'l', 'left', None, None, None, None, None, 0],
        [3, 0, 1, None, 'side/{3}', 'enum.item', None, None, None, 'r', 'right', None, None, None, None, None, 0],

    ]

    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta),
            sa.literal_column("prepare IS NULL").label("prepare_is_null")
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, compare_rows)


@pytest.mark.parametrize("db_type", db_type.values(), ids=db_type.keys())
def test_internal_store_old_ids(
    db_type: str,
    rc: RawConfig,
    tmp_path: pathlib.Path,
    postgresql: str
):
    # Currently unique, param does not store ids

    dataset_id = uuid.UUID('3de1cff9-0580-48ae-8fbc-78e557523b88')
    resource_id = uuid.UUID('5d6a9217-0ff9-4dcf-b625-19867e25d5c0')
    base_id = uuid.UUID('3f060134-2e86-407a-9405-65b45288a3f9')
    model_0_id = uuid.UUID('feddc481-012e-4695-9f03-7704904a8ee4')
    model_1_id = uuid.UUID('99d4bd10-afcf-478f-826e-575a77877ce3')
    property_0_id = uuid.UUID('7c17d66f-708c-4d59-8d0d-8e1cbf90ff4b')
    property_1_id = uuid.UUID('9c297b7b-4130-4646-b2d3-821814003615')
    enum_item_0_id = uuid.UUID('8fcd2d38-d99c-4ec6-8a51-1a733e131bd3')
    enum_item_1_id = uuid.UUID('088e0849-ac84-47bd-996e-b89866fbaa4e')
    lang_id = uuid.UUID('f26edb00-b809-4cc0-9059-b440eda69326')
    comment_id = uuid.UUID('9070450a-fcb6-463b-aec2-e5161014ed0d')
    namespace_item_0_id = uuid.UUID('e1e87932-2303-49f8-b511-e7fe1b098463')
    namespace_item_1_id = uuid.UUID('a5a698cf-48e8-4e5f-b161-8b247debec78')
    prefix_item_0_id = uuid.UUID('4d37348f-b0e0-4b0d-96c1-f9b095632ec5')
    prefix_item_1_id = uuid.UUID('d9ddac7d-3bcc-4cb2-a319-5c275fc169e1')

    table = f'''
    id                    | dataset | resource | base | model | property | type    | ref          | source                  | prepare | uri                         | title               | description
    {namespace_item_0_id} |         |          |      |       |          | ns      | datasets     |                         |         |                             | All datasets        | All external datasets.
    {namespace_item_1_id} |         |          |      |       |          |         | datasets/gov |                         |         |                             | Government datasets | All government datasets.
                          |         |          |      |       |          |         |              |                         |         |                             |                     |
    {enum_item_0_id}      |         |          |      |       |          | enum    | side         | l                       | 'left'  |                             | Left                | Left side.
    {enum_item_1_id}      |         |          |      |       |          |         |              | r                       | 'right' |                             | Right               | Right side.  
    {dataset_id}          | data    |          |      |       |          |         |              |                         |         |                             |                     |
    {lang_id}             |         |          |      |       |          | lang    | lt           |                         |         |                             | Pavyzdys            | Pavyzdinis duomenų rinkinys.
                          |         |          |      |       |          |         |              |                         |         |                             |                     |
    {prefix_item_0_id}    |         |          |      |       |          | prefix  | locn         |                         |         | http://www.w3.org/ns/locn#  |                     |
    {prefix_item_1_id}    |         |          |      |       |          |         | ogc          |                         |         | http://www.opengis.net/rdf# |                     |
    {resource_id}         |         | res      |      |       |          | sql     |              | sqlite:///{tmp_path}/db |         |                             |                     |
    {model_0_id}          |         |          |      | Test  |          |         |              |                         |         |                             |                     |
    {property_0_id}       |         |          |      |       | num      | number  |              |                         |         |                             |                     |
                          |         |          |      |       |          |         |              |                         |         |                             |                     |
    {base_id}             |         |          | Test |       |          |         |              |                         |         |                             |                     |
    {model_1_id}          |         |          |      | New   |          |         |              |                         |         |                             |                     |
    {comment_id}          |         |          |      |       |          | comment | TEXT         |                         |         |                             | Example             | Comment
    {property_1_id}       |         |          |      |       | text     | string  |              |                         |         |                             |                     |
    '''
    tabular_manifest = setup_tabular_manifest(rc, tmp_path, table)
    if db_type == "sqlite":
        dsn = 'sqlite:///' + str(tmp_path / 'db.sqlite')
        db = Sqlite(dsn)
        with db.engine.connect():
            write_internal_sql_manifest(db.dsn, tabular_manifest)
    else:
        dsn = postgresql
        write_internal_sql_manifest(dsn, tabular_manifest)

    compare_rows = [
        [namespace_item_0_id, None, 0, None, 'datasets', 'ns', 'datasets', 'ns', 'datasets', None, None, None, None, None, 'All datasets', 'All external datasets.'],
        [namespace_item_1_id, None, 0, None, 'datasets/gov', 'ns', 'datasets/gov', 'ns', 'datasets/gov', None, None, None, None, None, 'Government datasets', 'All government datasets.'],
        [2, None, 0, None, 'side', 'enum', 'side', 'enum', 'side', None, None, None, None, None, None, None],
        [enum_item_0_id, 2, 1, None, f'side/{enum_item_0_id}', 'enum.item', None, None, None, 'l', 'left', None, None, None, 'Left', 'Left side.'],
        [enum_item_1_id, 2, 1, None, f'side/{enum_item_1_id}', 'enum.item', None, None, None, 'r', 'right', None, None, None, 'Right', 'Right side.'],
        [dataset_id, None, 0, 'data', 'data', 'dataset', 'data', None, None, None, None, None, None, None, None, None],
        [lang_id, dataset_id, 1, 'data', 'data/lt', 'lang', 'lt', 'lang', 'lt', None, None, None, None, None, 'Pavyzdys', 'Pavyzdinis duomenų rinkinys.'],
        [prefix_item_0_id, dataset_id, 1, 'data', 'data/locn', 'prefix', 'locn', 'prefix', 'locn', None, None, None, None, 'http://www.w3.org/ns/locn#', None, None],
        [prefix_item_1_id, dataset_id, 1, 'data', 'data/ogc', 'prefix', 'ogc', 'prefix', 'ogc', None, None, None, None, 'http://www.opengis.net/rdf#', None, None],
        [resource_id, dataset_id, 1, 'data', 'data/res', 'resource', 'res', 'sql', None, f'sqlite:///{tmp_path}/db', None, None, None, None, None, None],
        [model_0_id, resource_id, 2, 'data/Test', 'data/res/Test', 'model', 'Test', None, None, None, None, None, None, None, None, None],
        [property_0_id, model_0_id, 3, 'data/Test/num', 'data/res/Test/num', 'property', 'num', 'number', None, None, None, None, None, None, None, None],
        [base_id, resource_id, 2, 'data/Test', 'data/res/Test', 'base', 'Test', None, None, None, None, None, None, None, None, None],
        [model_1_id, base_id, 3, 'data/New', 'data/res/Test/New', 'model', 'New', None, None, None, None, None, None, None, None, None],
        [comment_id, model_1_id, 4, 'data/New', f'data/res/Test/New/{comment_id}', 'comment', 'TEXT', 'comment', 'TEXT', None, None, None, None, None, 'Example', 'Comment'],
        [property_1_id, model_1_id, 4, 'data/New/text', 'data/res/Test/New/text', 'property', 'text', 'string', None, None, None, None, None, None, None, None],
    ]

    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        meta = sa.MetaData(conn)
        stmt = sa.select([
            get_table_structure(meta)
        ])
        rows = conn.execute(stmt)
        result_rows = []
        for item in rows:
            result_rows.append(list(item))
        compare_sql_to_required(result_rows, compare_rows)
