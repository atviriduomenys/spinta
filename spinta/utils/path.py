import pathlib

# Remove when support for Python 3.9 is dropped.
# TODO: https://github.com/atviriduomenys/spinta/issues/1374
try:
    # for Python >=3.10
    from importlib import resources
    from importlib.resources import abc
except ImportError:
    # Python <=3.9
    from importlib import resources, abc


def is_ignored(rules, base, path):
    path = pathlib.Path("/") / path.relative_to(base)
    for rule in rules:
        if rule.endswith("/"):
            rule_parts_count = rule.count("/")
            if rule.startswith("/"):
                stars = len(path.parts) - rule_parts_count
                if stars > 0 and path.match(rule + "/".join(["*"] * stars)):
                    return True
            else:
                for i in range(len(path.parts)):
                    p = pathlib.Path(*path.parts[i:])
                    stars = len(p.parts) - rule_parts_count
                    if stars > 0 and p.match(rule + "/".join(["*"] * stars)):
                        return True
            if rule and path.match(rule):
                return True
        else:
            if path.match(rule):
                return True
    return False


def resource_filename(package: str, target: str) -> abc.Traversable:
    return resources.files(package).joinpath(target)
