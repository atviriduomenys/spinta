import pathlib

import boto3
import pytest

from spinta.testing.utils import get_error_codes


FILE_DATA = b"""Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus accumsan fringilla ligula eget porta. Duis mollis dui et magna ornare, nec mattis nisl posuere. Ut turpis orci, ullamcorper a eros sit amet, volutpat dapibus massa. Vestibulum vestibulum, ligula in ultrices tristique, urna est convallis odio, sit amet consectetur neque turpis non magna. Praesent eu justo eros. Cras aliquet quam id sapien ornare consectetur. Morbi erat diam, egestas quis eros eget, finibus cursus velit. Vestibulum sagittis feugiat ornare. In eleifend tellus fermentum mauris tristique lobortis.

Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Maecenas nec dui ac libero tristique iaculis quis vel leo. Nam et posuere lorem. Nunc tincidunt luctus placerat. Fusce nec nisl libero. Mauris a nibh mi. Integer eu neque felis. Cras vel nisl non ex rutrum condimentum sed a dui.

Quisque id bibendum est. Phasellus egestas sagittis nisi vel rhoncus. Donec sed tortor quam. Pellentesque facilisis vestibulum nisi, eu ultricies ex congue ac. Maecenas in est non nibh egestas feugiat. Donec dictum turpis dolor, vitae vestibulum tellus varius at. Morbi non venenatis magna. Donec semper, nisl in posuere accumsan, velit dolor luctus nunc, in cursus ligula nunc eu sapien. Maecenas eget justo mi.

Quisque tristique eleifend auctor. Mauris auctor libero in turpis malesuada molestie. Donec fringilla mauris lectus, in imperdiet ligula vestibulum cursus. Morbi pretium sed ex eu volutpat. Cras consectetur sapien vitae aliquet vulputate. Vestibulum in gravida metus. Ut venenatis lorem eget neque rutrum eleifend. Maecenas tincidunt sollicitudin lectus, non ullamcorper urna fringilla et. Nunc justo enim, lobortis ut pretium dictum, maximus in metus. Aenean at nulla porttitor, varius sem vel, ullamcorper dolor. Ut semper venenatis accumsan. Donec porttitor dui ipsum, eu tincidunt nisl vehicula ac. Donec blandit sodales elit eu euismod. Pellentesque viverra orci et accumsan condimentum. Morbi vel laoreet lacus, ac viverra fusce."""


def _upload_file(model, app):
    # Create a new s3 file resource.
    title = 'my_file'
    resp = app.post(f'/{model}', json={
        'title': title,
    })
    assert resp.status_code == 201, resp.text
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    resp = app.put(f'/{model}/{id_}/file', data=FILE_DATA, headers={
        'revision': revision,
        'content-type': 'image/png',
        # TODO: with content-disposition header it is possible to specify file
        #       name directly, but there should be option, to use model id as a
        #       file name, but that is currently not implemented.
        'content-disposition': f'attachment; filename="{title}"',
    })
    revision = resp.json()['_revision']
    assert resp.status_code == 200, resp.text
    return id_, revision


@pytest.mark.models(
    'backends/mongo/s3_file',
    'backends/postgres/s3_file',
)
def test_post(model, app):
    app.authmodel(model, ['insert', 'update', 'file_update'])
    _upload_file(model, app)


@pytest.mark.models(
    'backends/mongo/s3_file',
    'backends/postgres/s3_file',
)
def test_post_no_revision(model, app):
    app.authmodel(model, ['insert', 'update', 'file_update', 'getone'])

    title = 'my_file'
    resp = app.post(f'/{model}', json={
        'title': title,
    })
    assert resp.status_code == 201
    data = resp.json()
    id_ = data['_id']
    revision = data['_revision']

    resp = app.put(f'/{model}/{id_}/file', data=FILE_DATA, headers={
        'content-type': 'image/png',
        'content-disposition': f'attachment; filename="{title}"',
    })
    assert resp.status_code == 400
    assert get_error_codes(resp.json()) == ['NoItemRevision']

    resp = app.put(f'/{model}/{id_}/file', data=FILE_DATA, headers={
        'revision': revision,
        'content-type': 'image/png',
        'content-disposition': f'attachment; filename="{title}"',
    })
    old_revision = revision
    revision = resp.json()['_revision']
    assert old_revision != revision
    assert resp.status_code == 200, resp.text


@pytest.mark.models(
    'backends/mongo/s3_file',
    'backends/postgres/s3_file',
)
def test_double_post(model, app):
    # like with `fs` backend double file upload is allowed
    app.authmodel(model, ['insert', 'getone', 'update', 'file_update'])
    id_, revision = _upload_file(model, app)

    resp = app.get(f'/{model}/{id_}/file')
    assert resp.status_code == 200
    assert resp.content == FILE_DATA

    resp = app.put(f'/{model}/{id_}/file', data=b'BINARYDATA2', headers={
        'revision': revision,
        'content-type': 'image/png',
        # TODO: with content-disposition header it is possible to specify file
        #       name directly, but there should be option, to use model id as a
        #       file name, but that is currently not implemented.
        'content-disposition': f'attachment; filename="my_file"',
    })
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/{model}/{id_}/file')
    assert resp.status_code == 200
    assert resp.content == b'BINARYDATA2'


@pytest.mark.models(
    'backends/mongo/s3_file',
    'backends/postgres/s3_file',
)
def test_get(model, app):
    app.authmodel(model, ['insert', 'update', 'file_update', 'getone'])
    id_, revision = _upload_file(model, app)

    resp = app.get(f'/{model}/{id_}')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': id_,
        '_revision': revision,
        '_type': model,
        'title': 'my_file',
        'file': {'_content_type': 'image/png', '_id': 'my_file'},
    }

    resp = app.get(f'/{model}/{id_}/file')
    assert resp.status_code == 200
    assert resp.content == FILE_DATA

    resp = app.get(f'/{model}/{id_}/file:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_id': 'my_file',
        '_revision': revision,
        '_type': f'{model}.file',
        '_content_type': 'image/png',
    }


@pytest.mark.models(
    'backends/mongo/s3_file',
    'backends/postgres/s3_file',
)
def test_delete(model, app):
    app.authmodel(model, ['insert', 'update', 'file_update', 'getone', 'file_delete'])
    id_, revision = _upload_file(model, app)

    resp = app.delete(f'/{model}/{id_}/file')
    assert resp.status_code == 204, resp.text
    assert resp.content == b''

    resp = app.get(f'/{model}/{id_}')
    revision = resp.json()['_revision']
    resp = app.get(f'/{model}/{id_}/file:ref')
    assert resp.status_code == 200
    assert resp.json() == {
        '_type': f'{model}.file',
        '_revision': revision,
        '_id': None,
        '_content_type': None,
    }
    resp = app.get(f'/{model}/{id_}/file')
    assert resp.status_code == 404


@pytest.mark.models(
    'backends/mongo/s3_file',
    'backends/postgres/s3_file',
)
def test_delete_accesslog(model, app, context):
    app.authmodel(model, ['insert', 'update', 'file_update', 'getone', 'file_delete'])
    id_, revision = _upload_file(model, app)

    resp = app.delete(f'/{model}/{id_}/file')
    assert resp.status_code == 204, resp.text
    assert resp.content == b''

    accesslog = context.get('accesslog.stream')
    assert len(accesslog) == 3
    assert accesslog[-1]['http_method'] == 'DELETE'
    assert accesslog[-1]['fields'] == []
    assert accesslog[-1]['resources'][0] == {
        '_type': f'{model}.file',
        '_id': id_,
        '_revision': revision,
    }


@pytest.mark.models(
    'backends/mongo/s3_file',
    'backends/postgres/s3_file',
)
def test_wipe(model, app, rc, tmpdir):
    app.authmodel(model, ['insert', 'update', 'file_update',
                          'file_getone', 'wipe'])

    id_, revision = _upload_file(model, app)

    # add file to S3
    new_file = pathlib.Path(tmpdir) / 'new.file'
    new_file.write_bytes(b'DATA')
    bucket_name = rc.get('backends', 's3', 'bucket', required=False)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    with open(new_file, 'rb') as f:
        bucket.upload_fileobj(f, 'new.file')

    resp = app.delete(f'/{model}/:wipe')
    data = resp.json()
    assert data['wiped'] is True

    resp = app.get(f'/{model}/{id_}/file')
    assert resp.status_code == 404

    s3_file = pathlib.Path(tmpdir) / 's3.file'
    bucket.download_file('new.file', str(s3_file))

    with open(s3_file, 'rb') as f:
        assert f.read() == b'DATA'
