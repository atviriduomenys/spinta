from typing import TypedDict


class Lang(TypedDict):
    title: str
    description: str


LangData = dict[
    str,  # two letter locale code
    Lang,
]
