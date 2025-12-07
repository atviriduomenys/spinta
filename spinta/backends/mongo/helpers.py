from spinta.utils.schema import NA
from spinta.components import DataSubItem
from spinta.core.enums import Action


def inserting(data: DataSubItem):
    return data.root.action == Action.INSERT or (data.root.action == Action.UPSERT and data.saved is NA)
