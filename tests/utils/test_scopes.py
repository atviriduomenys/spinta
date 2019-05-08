from spinta.utils.scopes import name_to_scope


def test_name_to_scope():
    template = '{prefix}{name}_{action}'
    kwargs = {
        'maxlen': 20,
        'params': {
            'prefix': 'uapi_',
            'action': 'get',
        },
    }
    assert name_to_scope(template, 'name', **kwargs) == 'uapi_name_get'
    assert name_to_scope(template, 'long_long_name', **kwargs) == 'uapi_lonc3517952_get'
    assert name_to_scope(template, 'Name/SPACE', **kwargs) == 'uapi_name_space_get'
