from spinta.components import Component
from spinta.core.enums import Access, Status
from spinta.dimensions.comments.components import Comment, CommentGiven
from spinta.manifests.tabular.components import CommentRow
from spinta.utils.enums import enum_by_name


def load_comments(
    parent: Component,
    data: list[CommentRow] | None,
) -> list[Comment]:
    comments = []
    if data:
        for comment_data in data:
            params = comment_data.copy()
            access_given = params.pop("access")
            access = enum_by_name(parent, "access", Access, access_given)
            access = access or Access.private
            level = int(level) if (level := params.pop("level", None)) else None
            status_given = params.pop("status", None)
            status = enum_by_name(parent, "status", Status, status_given)
            comments.append(
                Comment(
                    access=access,
                    given=CommentGiven(
                        access=access_given,
                        status=status_given,
                    ),
                    level=level,
                    status=status,
                    **params,
                )
            )
    return comments
