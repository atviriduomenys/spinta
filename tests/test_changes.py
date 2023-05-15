import pytest

from spinta.testing.data import send


@pytest.mark.models(
    'backends/postgres/report',
    # 'backends/mongo/report',
)
def test_changes(model, context, app):
    app.authmodel(model, ['insert', 'patch', 'changes'])
    obj = send(app, model, 'insert', {'status': '1'})
    obj = send(app, model, 'patch', obj, {'status': '2'})
    obj = send(app, model, 'patch', obj, {'status': '3'})
    obj = send(app, model, 'patch', obj, {'status': '4'})

    assert send(app, model, 'changes', select=['_cid', '_op', 'status']) == [
        {'_cid': 1, '_op': 'insert', 'status': '1'},
        {'_cid': 2, '_op': 'patch', 'status': '2'},
        {'_cid': 3, '_op': 'patch', 'status': '3'},
        {'_cid': 4, '_op': 'patch', 'status': '4'},
    ]

    assert send(app, model, ':changes?limit(1)', select=['_cid', '_op', 'status']) == [
        {'_cid': 1, '_op': 'insert', 'status': '1'},
    ]

    assert send(app, model, ':changes/2?limit(2)', select=['_cid', '_op', 'status']) == [
        {'_cid': 2, '_op': 'patch', 'status': '2'},
        {'_cid': 3, '_op': 'patch', 'status': '3'},
    ]

    assert send(app, model, ':changes/3?limit(2)', select=['_cid', '_op', 'status']) == [
        {'_cid': 3, '_op': 'patch', 'status': '3'},
        {'_cid': 4, '_op': 'patch', 'status': '4'},
    ]


@pytest.mark.models(
    'backends/postgres/report',
    # 'backends/mongo/report',
)
def test_changes_negative_offset(model, context, app):
    app.authmodel(model, ['insert', 'patch', 'changes'])
    obj = send(app, model, 'insert', {'status': '1'})
    obj = send(app, model, 'patch', obj, {'status': '2'})
    obj = send(app, model, 'patch', obj, {'status': '3'})
    obj = send(app, model, 'patch', obj, {'status': '4'})

    assert send(app, model, ':changes/-1?limit(1)', select=['_cid', '_op', 'status']) == [
        {'_cid': 4, '_op': 'patch', 'status': '4'},
    ]

    assert send(app, model, ':changes/-2?limit(2)', select=['_cid', '_op', 'status']) == [
        {'_cid': 3, '_op': 'patch', 'status': '3'},
        {'_cid': 4, '_op': 'patch', 'status': '4'},
    ]

    assert send(app, model, ':changes/-3?limit(2)', select=['_cid', '_op', 'status']) == [
        {'_cid': 2, '_op': 'patch', 'status': '2'},
        {'_cid': 3, '_op': 'patch', 'status': '3'},
    ]

    assert send(app, model, ':changes/-4?limit(2)', select=['_cid', '_op', 'status']) == [
        {'_cid': 1, '_op': 'insert', 'status': '1'},
        {'_cid': 2, '_op': 'patch', 'status': '2'},
    ]

    assert send(app, model, ':changes/-5?limit(2)', select=['_cid', '_op', 'status']) == [
        {'_cid': 1, '_op': 'insert', 'status': '1'},
        {'_cid': 2, '_op': 'patch', 'status': '2'},
    ]


@pytest.mark.models(
    'backends/postgres/report',
    # 'backends/mongo/report',
)
def test_changes_empty_patch(model, context, app):
    app.authmodel(model, ['insert', 'patch', 'changes'])
    obj = send(app, model, 'insert', {'status': '1'})
    obj = send(app, model, 'patch', obj, {'status': '1'})
    obj = send(app, model, 'patch', obj, {'status': '1'})

    assert send(app, model, ':changes', select=['_cid', '_op', 'status']) == [
        {'_cid': 1, '_op': 'insert', 'status': '1'},
    ]


def test_changes_with_ref(context, app):
    model = 'backends/postgres/country'
    app.authmodel(model, ['insert'])
    country = send(app, model, 'insert', {'title': 'Lithuania'})

    model = 'backends/postgres/city'
    app.authmodel(model, ['insert', 'changes'])
    send(app, model, 'insert', {'title': 'Vilnius', 'country': {'_id': country.id}})
    send(app, model, 'insert', {'title': 'Kaunas', 'country': {'_id': country.id}})

    resp = app.get(f"{model}/:changes/:format/ascii")
    _, header, *lines, _ = resp.text.splitlines()
    header = header.split()
    assert header == [
        '_cid',
        '_created',
        '_op',
        '_id',
        '_txn',
        '_revision',
        'title',
        'country._id',
    ]
    lines = (dict(zip(header, line.split())) for line in lines)
    lines = [
        (
            x['_op'],
            x['title'],
            x['country._id'],
        )
        for x in lines
    ]
    assert lines == [
        ('insert', 'Vilnius', country.id),
        ('insert', 'Kaunas', country.id),
    ]
