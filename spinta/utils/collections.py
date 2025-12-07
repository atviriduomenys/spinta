from collections import defaultdict


class keydefaultdict(defaultdict):
    def __missing__(self, key: object):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret
