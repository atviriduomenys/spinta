from typing import Dict, TypedDict


class Lang(TypedDict):
    title: str
    description: str


LangData = Dict[
    str,  # two letter locale code
    Lang,
]
