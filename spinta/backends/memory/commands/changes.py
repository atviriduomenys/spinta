from typing import AsyncIterator

from spinta import commands
from spinta.components import Context, Property, DataItem
from spinta.backends.nobackend.components import NoBackend


@commands.create_changelog_entry.register(Context, Property, NoBackend)
def create_changelog_entry(
    context: Context,
    model: Property,
    backend: NoBackend,
    *,
    dstream: AsyncIterator[DataItem],
) -> AsyncIterator[DataItem]:
    # FileSystem properties don't have change log, change log entry will be
    # created on property model backend.
    return dstream
