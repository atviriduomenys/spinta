from __future__ import annotations

from typing import Iterable
from typing import TYPE_CHECKING
from typing import Union

from spinta.core.enums import Access
from spinta.utils.enums import enum_by_name

if TYPE_CHECKING:
    from spinta.dimensions.enum.components import EnumItem
    from spinta.components import Model
    from spinta.components import Namespace
    from spinta.components import Property
    from spinta.datasets.components import Dataset
    from spinta.datasets.components import Resource
    from spinta.manifests.components import Manifest


def load_access_param(
    component: Union[
        Dataset,
        Resource,
        Namespace,
        Model,
        Property,
        EnumItem,
    ],
    given_access: str,
    parents: Iterable[Union[
        Manifest,
        Dataset,
        Namespace,
        Model,
        Property,
    ]] = (),
) -> None:
    access = enum_by_name(component, 'access', Access, given_access)

    # If child has higher access than parent, increase parent access.
    if access is not None:
        for parent in parents:
            if parent.access is None or access > parent.access:
                parent.access = access

    component.access = access
    component.given.access = given_access


def link_access_param(
    component: Union[
        Dataset,
        Resource,
        Namespace,
        Model,
        Property,
        EnumItem,
    ],
    parents: Iterable[Union[
        Manifest,
        Dataset,
        Namespace,
        Model,
        Property,
    ]] = (),
) -> None:
    if component.access is None:
        for parent in parents:
            if parent.access and parent.given.access:
                component.access = parent.access
                break
        else:
            component.access = Access.protected
