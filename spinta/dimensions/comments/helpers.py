from typing import List
from typing import Optional

from spinta.components import Component
from spinta.core.enums import Access
from spinta.dimensions.comments.components import Comment
from spinta.dimensions.comments.components import CommentGiven
from spinta.manifests.tabular.components import CommentRow
from spinta.utils.enums import enum_by_name


def load_comments(
    parent: Component,
    data: Optional[List[CommentRow]],
) -> List[Comment]:
    comments = []
    if data:
        for comment_data in data:
            params = comment_data.copy()
            access_given = params.pop('access')
            access = enum_by_name(parent, 'access', Access, access_given)
            access = access or Access.private
            comments.append(Comment(
                access=access,
                given=CommentGiven(
                    access=access_given,
                ),
                **params,
            ))
    return comments
