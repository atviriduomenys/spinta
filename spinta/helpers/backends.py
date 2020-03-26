from spinta.components.node import Model


def is_valid_sort_key(key: str, model: Model):
    # sort key must be a leaf node in the model schema.
    # we cannot sort using intermediate node, because it's type would
    # be `array` or `object`.
    #
    # is_valid_sort_key('certificates', report_model) == False
    # is_valid_sort_key('certificates.notes.note_type', report_model) == True
    leaf_key = key.split('.')[-1]
    if leaf_key not in model.leafprops:
        return False
    return True
