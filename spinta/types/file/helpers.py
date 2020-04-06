import pathlib

from spinta.backends.components import BackendFeatures
from spinta.components import DataSubItem, Action
from spinta.types.datatype import File
from spinta.utils.data import take


def prepare_patch_data(
    dtype: File,
    data: DataSubItem,
) -> dict:
    if data.root.action == Action.DELETE:
        patch = {
            '_id': None,
            '_content_type': None,
            '_size': None,
        }
    else:
        patch = take(['_id', '_content_type', '_size'], data.patch)

    if BackendFeatures.FILE_BLOCKS in dtype.backend.features:
        if data.root.action == Action.DELETE:
            patch.update({
                '_blocks': [],
                '_bsize': None,
            })
        else:
            patch.update(take(['_blocks', '_bsize'], data.patch))

    if isinstance(patch.get('_id'), pathlib.Path):
        # On FileSystem backend '_id' is a Path.
        # XXX: It would be nice to decouple this bey visiting each file property
        #      separaterly.
        patch['_id'] = str(patch['_id'])

    return patch
