import pytest

from spinta.utils.schema import NA
from spinta.testing.dtypes import post, upsert, put, patch, delete, get, search


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_insert(model, app):
    app.authmodel(model, ['insert', 'getone', 'search'])
    pk, rev, val = post(app, model, 'new')
    assert val == 'new'
    assert get(app, model, pk, rev) == val
    assert search(app, model, pk, rev, val) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_upsert_insert(model, app):
    app.authmodel(model, ['upsert', 'getone', 'search'])
    pk, rev, val = upsert(app, model, 'old', 'new', status=201)
    assert val == 'new'
    assert get(app, model, pk, rev) == val
    assert search(app, model, pk, rev, 'old') == []
    assert search(app, model, pk, rev, val) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_upsert_patch(model, app):
    app.authmodel(model, ['insert', 'upsert', 'getone', 'search'])
    pk0, rev0, val = post(app, model, 'old')
    pk1, rev1, val = upsert(app, model, 'old', 'new', status=200)
    assert val == 'new'
    assert pk0 == pk1
    assert rev0 != rev1
    assert get(app, model, pk0, rev1) == val
    assert search(app, model, pk0, rev1, 'old') == []
    assert search(app, model, pk0, rev1, val) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_update(model, app):
    app.authmodel(model, ['insert', 'update', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    rev1, val = put(app, model, pk, rev0, 'new')
    assert val == 'new'
    assert rev0 != rev1
    assert get(app, model, pk, rev1) == val
    assert search(app, model, pk, rev1, 'old') == []
    assert search(app, model, pk, rev1, val) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
)
def test_update_missing(model, app):
    app.authmodel(model, ['insert', 'update', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    rev1, val = put(app, model, pk, rev0)
    assert val is None
    assert rev0 != rev1
    assert get(app, model, pk, rev1) == val
    assert search(app, model, pk, rev1, 'old') == []
    assert search(app, model, pk, rev1, val) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_update_missing_in_array(model, app):
    app.authmodel(model, ['insert', 'update', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    rev1, val = put(app, model, pk, rev0)
    assert val is NA
    assert rev0 != rev1
    assert get(app, model, pk, rev1) == NA
    assert search(app, model, pk, rev1, 'old') == []
    assert search(app, model, pk, rev1, by='_id') == [NA]


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_update_same(model, app):
    app.authmodel(model, ['insert', 'update', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    rev1, val = put(app, model, pk, rev0, 'old')
    assert val == 'old'
    assert rev0 == rev1
    assert get(app, model, pk, rev0) == val
    assert search(app, model, pk, rev0, val) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_patch(model, app):
    app.authmodel(model, ['insert', 'patch', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    rev1, val = patch(app, model, pk, rev0, 'new')
    assert val == 'new'
    assert rev0 != rev1
    assert get(app, model, pk, rev1) == val
    assert search(app, model, pk, rev0, 'old') == []
    assert search(app, model, pk, rev1, val) == [val]


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_patch_missing(model, app):
    app.authmodel(model, ['insert', 'patch', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    rev1, val = patch(app, model, pk, rev0)
    assert val is NA
    assert rev0 == rev1
    assert get(app, model, pk, rev0) == 'old'
    assert search(app, model, pk, rev0, 'old') == ['old']


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
    'backends/mongo/dtypes/object/string',
    'backends/postgres/dtypes/object/string',
    'backends/mongo/dtypes/array/string',
    'backends/postgres/dtypes/array/string',
)
def test_patch_same(model, app):
    app.authmodel(model, ['insert', 'patch', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    rev1, val = patch(app, model, pk, rev0, 'old')
    assert val is NA
    assert rev0 == rev1
    assert get(app, model, pk, rev0) == 'old'
    assert search(app, model, pk, rev0, 'old') == ['old']


@pytest.mark.models(
    'backends/mongo/dtypes/string',
    'backends/postgres/dtypes/string',
)
def test_delete(model, app):
    app.authmodel(model, ['insert', 'delete', 'getone', 'search'])
    pk, rev0, val = post(app, model, 'old')
    delete(app, model, pk, rev0)
    assert get(app, model, pk, rev0, status=404) == ['ItemDoesNotExist']
    assert search(app, model, pk, rev0, 'old') == []
