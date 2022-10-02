from typing import List, Iterable

from pathlib import Path

import jsonpatch
import requests
import httpx

from ruamel.yaml import YAML

from spinta.utils.schema import NA

yaml = YAML(typ='safe')


def create_manifest_files(tmpdir, manifest):
    for file, data in manifest.items():
        if data is None:
            data = Path('tests/manifest') / file
            data = next(yaml.load_all(data.read_text()))
        path = Path(tmpdir) / file
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(data, dict):
            yaml.dump(data, path)
        elif isinstance(data, list):
            yaml.dump_all(data, path)


def update_manifest_files(tmpdir, manifest):
    for file, patch in manifest.items():
        path = Path(tmpdir) / file
        versions = list(yaml.load_all(path.read_text()))
        patch = jsonpatch.JsonPatch(patch)
        versions[0] = patch.apply(versions[0])
        with path.open('w') as f:
            yaml.dump_all(versions, f)


def read_manifest_files(tmpdir):
    path = Path(tmpdir)
    yaml = YAML(typ='safe')
    manifests = {}
    for fp in path.glob('**/*.yml'):
        data = list(yaml.load_all(fp.read_text()))
        manifest_file = str(fp)[len(str(tmpdir)) + 1:]
        manifests[manifest_file] = data
    return manifests


def readable_manifest_files(manifest):
    ids = {}
    for filename, versions in manifest.items():
        manifest[filename] = [versions[0]]
        version = versions[0]
        name = version['name']

        # Fill ids dict
        for i, version in enumerate(versions):
            if 'id' in version:
                ids[version['id']] = f'{name}:{i}'

        # Read only last version.
        for i, version in enumerate(versions[-1:], len(versions) - 1):
            del version['date']
            del version['changes']
            for action in version['migrate']:
                action['upgrade'] = action['upgrade'].splitlines()
                action['downgrade'] = action['downgrade'].splitlines()
            manifest[filename] += [version]

    # Replaces UUIDs to human version numbers
    for filename, versions in manifest.items():
        for version in versions:
            if 'id' in version:
                version['id'] = ids.get(version['id'], '(UNKNOWN)')
            if 'version' in version and isinstance(version['version'], str):
                version['version'] = ids.get(version['version'], '(UNKNOWN)')

    return manifest


def errors(resp, *keys, status: int = None):
    errors = _get_errors_data(resp, status)
    return [_extract_error(err, keys) for err in errors]


def error(resp, *keys, status: int = None):
    errors = _get_errors_data(resp, status)
    assert len(errors) == 1, errors
    return _extract_error(errors[0], keys)


def _get_errors_data(resp, status: int = None):
    if resp.headers['content-type'].startswith('text/html'):
        data = resp.context
    else:
        data = resp.json()
    status = status or resp.status_code
    assert resp.status_code == status, data
    assert 'errors' in data, data
    return data['errors']


def _extract_error(err: dict, keys: Iterable[str]):
    res = {}
    if keys:
        for k in keys:
            if isinstance(k, list):
                ctx = err.get('context', {})
                res['context'] = {k: ctx.get(k, NA) for k in k}
            else:
                res[k] = err.get(k, NA)
    else:
        return err.get('code', NA)
    return res


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
        self.id_key = id_key
        self.search_resp_key = search_resp_key
        self.ids = {k: v for v, k in enumerate(self._cast(ids))}
        self.idsr = {k: v for v, k in self.ids.items()}

    def __call__(self, ids):
        return [self.ids.get(i, i) for i in self._cast(ids)]

    def __getitem__(self, i):
        return self.idsr[i]

    def _cast(self, ids):
        if isinstance(ids, (requests.models.Response, httpx.Response)):
            resp = ids
            assert resp.status_code == 200, resp.json()
            ids = resp.json()
        if isinstance(ids, dict):
            ids = ids[self.search_resp_key]
        if isinstance(ids, list) and len(ids) > 0 and isinstance(ids[0], dict):
            ids = [r[self.id_key] for r in ids]
        return ids
