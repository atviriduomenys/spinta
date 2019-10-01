from typing import List

from pathlib import Path

from ruamel.yaml import YAML

from spinta.utils.scopes import name_to_scope

yaml = YAML(typ='safe')


def create_manifest_files(tmpdir, manifest):
    for file, data in manifest.items():
        path = Path(tmpdir) / file
        path.parent.mkdir(parents=True, exist_ok=True)
        yaml.dump(data, path)


def get_error_codes(response):
    assert response["errors"]
    return [err["code"] for err in response["errors"]]


def get_error_context(response, error_code, ctx_keys: List[str] = None):
    assert response["errors"]
    for err in response["errors"]:
        # FIXME: probably shouldn't return the context of the first found error
        # If more than one error is found and it's not intended - error
        # should be thrown
        if err["code"] == error_code:
            if ctx_keys is None:
                return err["context"]
            else:
                return {k: v for k, v in err["context"].items() if k in ctx_keys}
    else:
        assert False


def get_model_scopes(context, model, actions: List[str]):
    config = context.get('config')
    return [
        name_to_scope('{prefix}{name}_{action}', model, maxlen=config.scope_max_length, params={
            'prefix': config.scope_prefix,
            'action': action,
        })
        for action in actions
    ]
