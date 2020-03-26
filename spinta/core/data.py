from typing import Any, List


class NotAvailable:

    def __repr__(self):
        return "<NA>"

    def __bool__(self):
        return False


NA = NotAvailable()


def getval(context, val: Any, name: List[str]):
    if len(name) == 2 and name[0] == 'context':
        val = context.get(name[1])
        name = name[2:]
    for key in name:
        if isinstance(val, dict):
            val = val[key]
        else:
            val = getattr(val, key)
    return val
