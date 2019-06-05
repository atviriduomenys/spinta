def test_crud(app):
    app.authorize([
        'spinta_photo_insert',
        'spinta_photo_update',
        'spinta_photo_delete',
        'spinta_photo_getone',
    ])

    # Create a new photo resource.
    resp = app.post('/photos', json={
        'type': 'photo',
        'name': 'myphoto',
    })
    assert resp.status_code == 201, resp.text
    id = resp.json()['id']

    # PUT image to just create photo resource.
    resp = app.put(f'/photos/{id}/image', data=b'BINARYDATA', headers={
        'content-type': 'image/png',
        # TODO: with content-disposition header it is possible to specify file
        #       naem dierectly, but there should be option, to use model id as a
        #       file name, but that is currently not implemented.
        'content-disposition': 'attachment; filename="myimg.png"',
    })
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/photos/{id}')
    assert resp.json() == {
        'type': 'photo',
        'id': id,
        # FIXME: revision should not be None.
        'revision': None,
        'content_type': 'image/png',
        'image': 'myimg.png',
        'name': 'myphoto',
    }

    resp = app.get(f'/photos/{id}/image')
    assert resp.content == b'BINARYDATA'

    resp = app.delete(f'/photos/{id}/image')
    assert resp.status_code == 200, resp.text

    resp = app.get(f'/photos/{id}/image')
    assert resp.status_code == 404

    resp = app.get(f'/photos/{id}')
    assert resp.json() == {
        'content_type': None,
        'id': id,
        'image': None,
        'name': 'myphoto',
        # FIXME: revision should not be None.
        'revision': None,
        'type': 'photo',
    }
