from typing import TypeVar

T = TypeVar("T")


def identity(x: T) -> T:
    return x
