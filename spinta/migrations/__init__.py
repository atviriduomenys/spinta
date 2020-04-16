from typing import List

import datetime
import uuid

import jsonpatch


class SchemaVersion:

    def __init__(self, id_, date, changes):
        self.id = id_
        self.date = date
        self.changes = changes
        self.parents = []
        self.actions = []


def get_schema_changes(old: dict, new: dict) -> List[dict]:
    return [
        change
        for change in jsonpatch.make_patch(old, new)
        if not change['path'].startswith('/version/')
    ]


def get_new_schema_version(changes: List[dict]) -> dict:
    return SchemaVersion(
        str(uuid.uuid4()),
        datetime.datetime.now(datetime.timezone.utc),
        changes
    )
