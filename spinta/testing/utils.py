from pathlib import Path

from ruamel.yaml import YAML

yaml = YAML(typ='safe')


def create_manifest_files(tmpdir, manifest):
    for file, data in manifest.items():
        path = Path(tmpdir) / file
        path.parent.mkdir(parents=True, exist_ok=True)
        yaml.dump(data, path)


def get_error_codes(response):
    assert response["errors"]
    return [err["code"] for err in response["errors"]]


def get_error_context(response, error_code):
    assert response["errors"]
    for err in response["errors"]:
        # FIXME: probably shouldn't return the context of the first found error
        # If more than one error is found and it's not intended - error
        # should be thrown
        if err["code"] == error_code:
            return err["context"]
    else:
        assert False
