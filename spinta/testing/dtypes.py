from spinta.utils.schema import NA
from spinta.utils.data import take
from spinta.testing.utils import get_error_codes


def path(model: str):
    parts = model.split('/')
    if len(parts) > 3 and (parts[0], parts[2]) == ('backends', 'dtypes'):
        parts = parts[3:]
        parts = [
            a if a == 'array' else b
            for a, b in zip([None] + parts, parts)
            if b != 'array'
        ]
        return '.'.join(parts)


def nest(model: str, data: dict):
    parts = model.split('/')
    if len(parts) > 3 and (parts[0], parts[2]) == ('backends', 'dtypes'):
        parts = parts[3:]
        if parts[-1] in data:
            d = data = data.copy()
            value = d.pop(parts[-1])
            for k in parts[:-1]:
                if k == 'array':
                    v = []
                else:
                    v = {}
                if isinstance(d, list):
                    d.append(v)
                else:
                    d[k] = v
                d = v
            if isinstance(d, list):
                d.append(value)
            else:
                d[parts[-1]] = value
    return data


def flat(model, data):
    parts = model.split('/')
    if len(parts) > 3 and (parts[0], parts[2]) == ('backends', 'dtypes'):
        parts = parts[3:]
        if parts[0] in data:
            v = data = data.copy()
            for k in parts:
                if isinstance(v, list):
                    if len(v) > 0:
                        v = v[0]
                    else:
                        v = NA
                        break
                else:
                    if k in v:
                        v = v[k]
                    else:
                        v = NA
                        break
            data.pop(parts[0])
            if v is not NA:
                data[parts[-1]] = v
    return data


def post(app, model: str, value: str, *, status: int = 201):
    name = model.split('/')[-1]
    data = nest(model, {name: value})
    resp = app.post(f'/{model}', json=data)
    assert resp.status_code == status, resp.json()
    data = resp.json()
    data = flat(model, data)
    pk = data['_id']
    rev = data['_revision']
    val = data[name]
    assert data == {
        '_type': model,
        '_id': pk,
        '_revision': rev,
        name: val,
    }
    return pk, rev, val


def upsert(app, model: str, where: str, value: str, *, status: int):
    name = model.split('/')[-1]
    data = nest(model, {
        '_op': 'upsert',
        '_where': f'%s={where!r}' % path(model),
        name: value,
    })
    resp = app.post(f'/{model}', json=data)
    assert resp.status_code == status, resp.json()
    data = resp.json()
    data = flat(model, data)
    pk = data['_id']
    rev = data['_revision']
    val = data[name]
    assert data == {
        '_type': model,
        '_id': pk,
        '_revision': rev,
        name: val,
    }
    return pk, rev, val


def put(app, model: str, pk: str, rev: str, value: str = NA):
    name = model.split('/')[-1]
    data = nest(model, take(all, {
        '_revision': rev,
        name: value,
    }))
    resp = app.put(f'/{model}/{pk}', json=data)
    data = resp.json()
    assert resp.status_code == 200, data
    data = flat(model, data)
    pk = data['_id']
    rev = data['_revision']
    val = take(name, data)
    assert data == take(all, {
        '_type': model,
        '_id': pk,
        '_revision': rev,
        name: val,
    })
    return rev, val


def patch(app, model: str, pk: str, rev: str, value: str = NA):
    name = model.split('/')[-1]
    data = nest(model, take(all, {
        '_revision': rev,
        name: value,
    }))
    resp = app.patch(f'/{model}/{pk}', json=data)
    data = resp.json()
    assert resp.status_code == 200, data
    data = flat(model, data)
    pk = data['_id']
    rev = data['_revision']
    val = take(name, data)
    assert data == take(all, {
        '_type': model,
        '_id': pk,
        '_revision': rev,
        name: val,
    })
    return rev, val


def delete(app, model: str, pk: str, rev: str):
    data = nest(model, take(all, {
        '_revision': rev,
    }))
    # XXX: DELETE method should not include a request body.
    resp = app.request('DELETE', f'/{model}/{pk}', json=data)
    assert resp.content == b''
    assert resp.status_code == 204


def get(app, model, pk, rev, status=200):
    name = model.split('/')[-1]
    resp = app.get(f'{model}/{pk}')
    data = resp.json()
    assert resp.status_code == status, data
    if status == 200:
        data = flat(model, data)
        val = take(name, data)
        assert data == take(all, {
            '_type': model,
            '_id': pk,
            '_revision': rev,
            name: val,
        })
        return val
    else:
        return get_error_codes(data)


def search(app, model, pk, rev, val=NA, by=None):
    name = model.split('/')[-1]
    place = path(model)
    if val is None:
        val = 'null'
    elif isinstance(val, str):
        val = f'"{val}"'
    if by is None:
        by = f'{place}={val}'
    elif by == '_id':
        by = f'_id="{pk}"'
    else:
        by = f'{by}={val}'
    resp = app.get(f'{model}?select(_type,_id,_revision,{place})&{by}')
    data = resp.json()
    assert resp.status_code == 200, data
    assert '_data' in data, data
    val = []
    for d in data['_data']:
        d = flat(model, d)
        v = take(name, d)
        assert d == take(all, {
            '_type': model,
            '_id': pk,
            '_revision': rev,
            name: v,
        })
        val.append(v)
    return val
