import itertools
import json

from spinta.cli import pull as pull_


def pull(cli, rc, dataset, model=None, *, push=True):
    cmd = [
        [dataset],
        ['--push'] if push else [],
        ['--model', model] if model else [],
        ['-e', 'stdout:jsonl'],
    ]
    cmd = list(itertools.chain(*cmd))
    result = cli.invoke(rc, pull_, cmd)
    data = []
    for line in result.stdout.splitlines():
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            print(line)
        else:
            data.append(d)
    return data
