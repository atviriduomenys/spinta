from typer import Exit
from typer import echo


class ErrorCounter:
    count: int
    max_error_count: int

    def __init__(self, max_count: int):
        self.max_error_count = max_count
        self.reset()

    def increase(self):
        self.count += 1

    def reset(self):
        self.count = 0

    def has_errors(self) -> bool:
        return self.count > 0

    def has_reached_max(self) -> bool:
        return self.count >= self.max_error_count


def cli_error(
    message: str
):
    echo(message, err=True)
    raise Exit(code=1)
