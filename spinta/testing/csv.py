from typing import List

from requests.models import Response


def parse_csv(resp: Response) -> List[List[str]]:
    resp.raise_for_status()
    return [
        line.strip().split(',')
        for line in resp.text.splitlines()
    ]
