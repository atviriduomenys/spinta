import datetime
import json
import hashlib
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import pytest

from spinta.components import Model
from spinta.components import Property
from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest_and_context
from spinta.types.datatype import DataType
from spinta.types.datatype import Ref
from spinta.types.datatype import Object
from spinta.types.datatype import Array


@pytest.mark.skip('datasets')
@pytest.mark.models(
    'backends/postgres/report/:dataset/test',
)
def test_push_same_model(model, app):
    app.authmodel(model, ['insert'])
    data = [
        {'_op': 'insert', '_type': model, 'status': 'ok'},
        {'_op': 'insert', '_type': model, 'status': 'warning'},
        {'_op': 'insert', '_type': model, 'status': 'critical'},
        {'_op': 'insert', '_type': model, 'status': 'blocker'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post('/', headers=headers, data=payload)
    resp = resp.json()
    data = resp.pop('_data')
    assert resp == {
        '_transaction': resp['_transaction'],
        '_status': 'ok',
    }
    assert len(data) == 4
    assert data[0] == {
        '_id': data[0]['_id'],
        '_revision': data[0]['_revision'],
        '_type': 'backends/postgres/report/:dataset/test',
        'count': None,
        'notes': [],
        'operating_licenses': [],
        'report_type': None,
        'revision': None,
        'status': 'ok',
        'update_time': None,
        'valid_from_date': None,
    }


def sha1(s):
    return hashlib.sha1(s.encode()).hexdigest()


@pytest.mark.skip('datasets')
def test_push_different_models(app):
    app.authorize(['spinta_set_meta_fields'])
    app.authmodel('country/:dataset/csv/:resource/countries', ['insert'])
    app.authmodel('backends/postgres/report/:dataset/test', ['insert'])
    data = [
        {'_op': 'insert', '_type': 'country/:dataset/csv', '_id': sha1('lt'), 'code': 'lt'},
        {'_op': 'insert', '_type': 'backends/postgres/report/:dataset/test', 'status': 'ok'},
    ]
    headers = {'content-type': 'application/x-ndjson'}
    payload = (json.dumps(x) + '\n' for x in data)
    resp = app.post(f'/', headers=headers, data=payload)
    resp = resp.json()
    assert '_data' in resp, resp
    data = resp.pop('_data')
    assert resp == {
        '_transaction': resp.get('_transaction'),
        '_status': 'ok',
    }
    assert len(data) == 2

    d = data[0]
    assert d == {
        '_id': d['_id'],
        '_revision': d['_revision'],
        '_type': 'country/:dataset/csv/:resource/countries',
        'code': 'lt',
        'title': None,
    }

    d = data[1]
    assert d == {
        '_id': d['_id'],
        '_revision': d['_revision'],
        '_type': 'backends/postgres/report/:dataset/test',
        'count': None,
        'notes': [],
        'operating_licenses': [],
        'report_type': None,
        'revision': None,
        'status': 'ok',
        'update_time': None,
        'valid_from_date': None,
    }


class PrimaryKey:
    model: str       # Model absolute name
    keys: List[str]  # List of property names uniquely identifying an object
    last: Dict[str, Any]  # Last pushed object
    time: datetime.datetime  # Time of last push


class PrimaryId:
    key: PrimaryKey
    timestamp: int   # Last pushed time, unix timestamp (0 if never pushed)
    external: str    # UUID of an external global object
    internal: str    # SHA1 of and internal unique object identifier
    checksum: str    # SHA1 checksum of whole object.


class SecondaryKey:
    model: str       # Model absolute name
    keys: List[str]  # List of property names uniquely identifying an object


class SecondaryId:
    key: SecondaryKey
    external: str    # UUID of an external global object
    internal: str    # SHA1 of an internal unique object identifier


def get_object_id(model: Model, data: Dict[str, Any]) -> Optional[PrimaryId]:
    """Find external object id by given model and object data

    Returns None if external id is not found.
    """
    return ''


def get_object_ref_id(dtype: Ref, data: Dict[str, Any]) -> Optional[SecondaryId]:
    """Find external object id by given ref and object data

    If ref uses same keys as primarky key, then get_object_id will be used,
    otherwise, secondary key will be used.

    Returns None if external id is not found.
    """
    return ''


def create_object_id(model: Model, data: Dict[str, Any]) -> PrimaryId:
    """Create and external object id for given object data

    `_id` must be present in `data`, it will be used as external id.

    This function will also creates all secondary keys refered to this model,
    if refered keys do not match primary key.
    """
    return PrimaryId()


def update_object_id(model: Model, data: Dict[str, Any]) -> PrimaryId:
    """Update external id of an object by given data

    `_id` must be present in `data`, it will be used as external id.

    This will also updates all secondary keys refered to this model, if refered
    keys do not match primary key.
    """
    oid = get_object_id(model, data)
    if oid is None:
        create_object_id()
    return PrimaryId()


def ref_objects_pushed(node: Union[Model, DataType], data: Any) -> bool:
    if isinstance(node, Model):
        for prop in node.properties.values():
            if not ref_objects_pushed():
                return False
    elif isinstance(node, Object):
        for prop in node.properties.values():
            if not ref_objects_pushed(prop.dtype):
                return False
    elif isinstance(node, Array):
        if not ref_objects_pushed(node.items.dtype):
            return False
    elif isinstance(node, Ref):
        if get_object_ref_id() is None:
            return False
    return True


def push_single_object(model: Model, data: Dict[str, Any]) -> bool:
    # Check if referenced objects already pushed.
    if not ref_objects_pushed(model, data):
        return False

    oid = get_object_id(model, data)

    return True



def test_push_v2(rc: RawConfig):
    context, manifest = load_manifest_and_context(rc, '''
    d | r | b | m | property   | type    | ref     | source | access
    example                    |         |         |        |
      |   |   | City           |         | id      | CITY   |
      |   |   |   | id         | integer |         | ID     | open
      |   |   |   | name       | string  |         | NAME   | open
    ''')
    model = manifest.models['example/City']
    data = {
        '_type': 'example/City',
        'name': 'Vilnius',
    }
    assert push_single_object(model, data)
