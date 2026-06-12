from typing import AsyncIterator

from spinta import commands
from spinta.backends.fs.components import FileSystem
from spinta.components import Context, DataItem, Property


@commands.create_changelog_entry.register(Context, Property, FileSystem)
def create_changelog_entry(
    context: Context,
    model: Property,
    backend: FileSystem,
    *,
    dstream: AsyncIterator[DataItem],
) -> AsyncIterator[DataItem]:
    # FileSystem properties don't have change log, change log entry will be
    # created on property model backend.
    return dstream
