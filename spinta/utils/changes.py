def get_patch_changes(old, new):
    changes = {}
    for k, v in new.items():
        if old.get(k) != v:
            changes[k] = v
    return changes


def get_patch_with_defaults(patch, props):
    for k, prop in props.items():
        if not k.startswith('_') and k not in patch.keys():
            patch[k] = prop.default
    return patch
