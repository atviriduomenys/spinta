from typing import List

from pathlib import Path

import requests

from ruamel.yaml import YAML

yaml = YAML(typ='safe')


def create_manifest_files(tmpdir, manifest):
    for file, data in manifest.items():
        path = Path(tmpdir) / file
        path.parent.mkdir(parents=True, exist_ok=True)
        yaml.dump(data, path)


def get_error_codes(response):
    assert 'errors' in response, response
    return [err['code'] for err in response['errors']]


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


class RowIds:

    def __init__(self, ids, id_key='_id', search_resp_key='_data'):
        self.ids = {k: v for v, k in enumerate(self._cast(ids))}
        self.idsr = {k: v for v, k in self.ids.items()}
        self.id_key = id_key
        self.search_resp_key = search_resp_key

    def __call__(self, ids):
        return [self.ids.get(i, i) for i in self._cast(ids)]

    def __getitem__(self, i):
        return self.idsr[i]

    def _cast(self, ids):
        if isinstance(ids, requests.models.Response):
            resp = ids
            assert resp.status_code == 200, resp.json()
            ids = resp.json()
        if isinstance(ids, dict):
            ids = ids[self.search_resp_key]
        if isinstance(ids, list) and len(ids) > 0 and isinstance(ids[0], dict):
            ids = [r[self.id_key] for r in ids]
        return ids
