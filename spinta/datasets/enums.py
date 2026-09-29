from enum import Enum


class ExternalIdPattern(Enum):
    """
    This enum is used to implement external id global handling pattern.
    To use it, external backends need to implement this `_id` resolving pattern:

    For PrimaryKey result should follow this pattern:
    {
        ExternalIdPattern.ID_KEY.value: {
            "<prop_name_1>": <prop_value_1>,
            ...
        },
        ExternalIdPattern.COMBINATIONS_KEY.value: {
            ("<prop_name_1>", "<prop_name_2>"): {
                "<prop_name_1>": <prop_value_1>,
                "<prop_name_2>": <prop_value_2>,
            },
            ...
        }
    }

    For identifiable Ref type, pattern should be this:
    {
        ExternalIdPattern.ID_KEY.value: {
            "<refprop_name_1>": <refprop_value_1>,
            ...
        }
        "<prop_name_1>": <prop_value_1>,
        "<prop_name_2>": <prop_value_2>,
        ...
    }

    ID_KEY is used to define `_id` value including all properties that make it up.
    COMBINATIONS_KEY is used when that reference can be created using multiple combinations, like Model[prop1], etc,
    to register those ids in keymap, we need to fetch all possible required combinations (gotten from model.required_keymap_properties)

    """

    ID_KEY = "_id"
    COMBINATIONS_KEY = "_combinations"
