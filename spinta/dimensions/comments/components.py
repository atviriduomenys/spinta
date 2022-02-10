import dataclasses

from spinta.components import Component
from spinta.core.enums import Access


@dataclasses.dataclass
class CommentGiven:
    access: str = None


@dataclasses.dataclass
class Comment(Component):
    id: str
    parent: str
    author: str
    access: Access
    # TODO: should be datetime
    created: str
    comment: str
    given: CommentGiven
