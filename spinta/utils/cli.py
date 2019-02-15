import copy


def update_config_from_cli(config, custom, options):
    for option in options:
        name, value = option.split('=', 1)

        this = config
        keys = name.split('.')
        path = ()
        for key in keys[:-1]:
            if isinstance(this, dict):
                if key in this:
                    this = this[key]
                elif path in custom:
                    this[key] = copy.deepcopy(this[custom[path]])
                    this = this[key]
            else:
                raise Exception(f"Unknown configuration option {name!r}.")

            path += (key,)

        key = keys[-1]
        if key in this:
            Type = type(this[key])
            this[key] = Type(value)
        else:
            raise Exception(f"Unknown configuration option {name!r}.")
