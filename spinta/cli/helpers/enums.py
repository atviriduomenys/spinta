from enum import Enum


class CommentPart(str, Enum):
    missing_external_refs = "missing-external-refs"
