from typing import Any

from spinta.components import Property
from spinta.datasets.keymaps.components import KeyMap
from spinta.exceptions import NotImplementedFeature


def handle_ref_key_assignment(keymap: KeyMap, value: Any, prop: Property) -> dict:
    if not prop.level or prop.level.value > 3:
        val = keymap.encode(prop.dtype.model.model_type(), value)
        val = {'_id': val}
    else:
        if len(prop.dtype.refprops) > 1 and not prop.model.external.unknown_primary_key:
            raise NotImplementedFeature(prop, feature="Ability to have multiple refprops")
        if len(prop.dtype.refprops) == 1:
            val = {
                prop.dtype.refprops[0].name: value
            }
        elif not prop.model.external.unknown_primary_key and prop.model.external.pkeys:
            val = {
                prop.model.external.pkeys[0].name: value
            }
        else:
            val = keymap.encode(prop.dtype.model.model_type(), value)
            val = {'_id': val}
    return val
