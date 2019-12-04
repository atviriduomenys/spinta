from typing import Iterable, List

import datetime

import jsonpatch


def get_schema_from_changes(versions: Iterable[dict]) -> dict:
    new = {}
    old = {}
    version = {}
    for i, version in enumerate(versions):
        if i == 0:
            new = version
            continue
        patch = version.get('changes')
        if patch:
            patch = jsonpatch.JsonPatch(patch)
            old = patch.apply(old)
    nextvnum = version.get('version', {}).get('number', 0) + 1
    return old, new, nextvnum


def get_schema_changes(old: dict, new: dict) -> List[dict]:
    return [
        change
        for change in jsonpatch.make_patch(old, new)
        if not change['path'].startswith('/version/')
    ]


def get_new_schema_version(
    old: dict,
    changes: List[dict],
    migrate: dict,
    nexvnum: int,
) -> dict:
    return {
        'version': {
            'number': nexvnum,
            'date': datetime.datetime.now(datetime.timezone.utc).astimezone(),
        },
        'changes': changes,
        'migrate': {
            'schema': migrate,
        },
    }
