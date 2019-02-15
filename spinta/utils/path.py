import pathlib


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
