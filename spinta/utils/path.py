import atexit
import importlib.resources
import pathlib
from contextlib import ExitStack
from pathlib import Path


def is_ignored(rules, base, path):
    path = pathlib.Path('/') / path.relative_to(base)
    for rule in rules:
        if rule.endswith('/'):
            rule_parts_count = rule.count('/')
            if rule.startswith('/'):
                stars = len(path.parts) - rule_parts_count
                if stars > 0 and path.match(rule + '/'.join(['*'] * stars)):
                    return True
            else:
                for i in range(len(path.parts)):
                    p = pathlib.Path(*path.parts[i:])
                    stars = len(p.parts) - rule_parts_count
                    if stars > 0 and p.match(rule + '/'.join(['*'] * stars)):
                        return True
            if rule and path.match(rule):
                return True
        else:
            if path.match(rule):
                return True
    return False


def resource_filename(package: str, target: str) -> Path:
    resource = importlib.resources.files(package) / target
    if isinstance(resource, Path):
        return resource
    else:
        raise RuntimeError(f"Resource {target} in package {package} could not be resolved as a local filesystem path.")
