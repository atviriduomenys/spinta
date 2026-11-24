"""Cell level rules of the ASCII table manifest format.

A table is a text of rows, cells are separated by ` | ` and the cells of a
wrapped row continuation are separated by ` : `. A row whose cell is too
long for the table width is wrapped into several lines, every line except
the last one ends with ` \\`, the next line continues the row.

Cell values escape the characters that would otherwise be read as a
separator or as a wrap marker:

- `|` is written as `\\|`,
- `:` is written as `\\:`, but only when it is surrounded by spaces (or by
  the cell boundary), so ` : ` can never appear inside a value,
- `\\` is written as `\\\\`, but only when it is followed by `|`, `:`, `\\`
  or when it is the last character of the value.
"""

from __future__ import annotations

import re

ASCII_TABLE_WIDTH = 80  # Default width of a table, wider tables are wrapped.
# Text columns are not shrunk below this width to fit `ASCII_TABLE_WIDTH`, so a table
# with many long values can be wider than `ASCII_TABLE_WIDTH`.
WRAP_MIN_WIDTH = 26

WRAP_MARKER = re.compile(r"(?:^|(?<=\s))\\$")
MAIN_SEPARATOR = re.compile(r"(?<!\\)\|")
CONTINUATION_SEPARATOR = re.compile(r"(?:^|(?<= )):(?=\s|$)")
UNESCAPE = re.compile(r"\\([\\|:])")

# Characters after which a long value may be wrapped.
_BREAK_AFTER = "/#?&=.-_,;"


def escape(text: str) -> str:
    """Escape a value, so it can be written inside a cell."""
    result = []
    last = len(text) - 1
    for i, char in enumerate(text):
        if char == "|":
            result.append("\\|")
        elif char == ":":
            before = i == 0 or text[i - 1] == " "
            after = i == last or text[i + 1] == " "
            result.append("\\:" if before and after else ":")
        elif char == "\\":
            following = text[i + 1] if i < last else ""
            result.append("\\\\" if i == last or following in "|:\\" else "\\")
        else:
            result.append(char)
    return "".join(result)


def unescape(text: str) -> str:
    """Reverse of `escape`."""
    return UNESCAPE.sub(r"\1", text)


def split_cells(line: str) -> list[str]:
    """Split a row line into raw (still escaped) cells."""
    return [cell.strip() for cell in MAIN_SEPARATOR.split(line)]


def split_continuation(line: str) -> list[str]:
    """Split a continuation line of a wrapped row into raw (still escaped) cells."""
    return [cell.strip() for cell in CONTINUATION_SEPARATOR.split(line)]


def strip_wrap_marker(line: str) -> tuple[str, bool]:
    """Remove the wrap marker from the end of a line, return the line and whether the row continues."""
    match = WRAP_MARKER.search(line)
    if match is None:
        return line, False
    return line[: match.start()].rstrip(), True


def wrap(text: str, width: int) -> list[str]:
    """Split a raw value into escaped pieces, every piece fits into `width`.

    Pieces are joined back without any separator, so the reader gets exactly
    the same value. Pieces never start or end with a space, because cells are
    stripped when they are read.
    """
    width = max(width, 1)
    if len(escape(text)) <= width:
        return [escape(text)]

    pieces = []
    rest = text
    while rest:
        cut = _cut(rest, width)
        pieces.append(escape(rest[:cut]))
        rest = rest[cut:]
    return pieces


def _cut(text: str, width: int) -> int:
    """Find where to cut `text`, so that the escaped beginning fits into `width`."""
    longest = min(len(text), width)
    limit = next(
        (n for n in range(longest, 0, -1) if len(escape(text[:n])) <= width),
        1,
    )
    if limit >= len(text):
        return len(text)

    # Prefer to cut after a punctuation mark, for example after `/` in URLs.
    for n in range(limit, max(1, limit // 2) - 1, -1):
        if text[n - 1] in _BREAK_AFTER and _clean_cut(text, n):
            return n

    # Otherwise cut anywhere, but not next to a space, and not too far from the width.
    for n in range(limit, max(1, limit // 2) - 1, -1):
        if _clean_cut(text, n):
            return n

    # There is no place to cut inside the width (for example, the value has only
    # single letter words), so the piece is longer than the width.
    for n in range(limit + 1, len(text)):
        if _clean_cut(text, n):
            return n
    return len(text)


def _clean_cut(text: str, n: int) -> bool:
    if n >= len(text):
        return True
    return text[n - 1] != " " and text[n] != " "
