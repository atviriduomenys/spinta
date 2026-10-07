from dataclasses import dataclass, replace


@dataclass(frozen=True)
class ETag:
    """An HTTP entity tag with an opaque value and explicit strength."""

    _value: str
    weak: bool = False

    def __post_init__(self) -> None:
        # Double quotes delimit the tag and cannot appear inside its value.
        # 0x21 is '!', the first visible ASCII character (excluding space).
        # 0x7F is DEL, a forbidden control character; 0xFF is the Latin-1 limit.
        if any(char == '"' or ord(char) == 0x7F or not 0x21 <= ord(char) <= 0xFF for char in self._value):
            raise ValueError(f"Invalid ETag value: {self._value!r}")

    @classmethod
    def from_header(cls, header: str) -> "ETag":
        # HTTP optional whitespace is only space and horizontal tab (\t).
        # Plain strip() would also remove newlines and other invalid whitespace.
        tag = header.strip(" \t")
        weak = tag.startswith("W/")
        if weak:
            tag = tag[2:]
        if len(tag) < 2 or not tag.startswith('"') or not tag.endswith('"'):
            raise ValueError(f"Invalid ETag header: {header!r}")
        return cls(tag[1:-1], weak=weak)

    def __str__(self) -> str:
        prefix = "W/" if self.weak else ""
        return f'{prefix}"{self._value}"'

    def is_weak(self) -> bool:
        return self.weak

    def value(self) -> str:
        return self._value

    def to_weak(self) -> "ETag":
        return replace(self, weak=True)

    def to_strong(self) -> "ETag":
        """Remove the weak marker; the caller must ensure byte-for-byte identity."""
        return replace(self, weak=False)

    def matches(self, if_none_match: str) -> bool:
        """Use RFC 9110 weak comparison for an If-None-Match header."""
        # Trim only HTTP optional whitespace (space and horizontal tab), as above.
        if if_none_match.strip(" \t") == "*":
            return True
        # If-None-Match can list several cached ETags; any matching value suffices.
        # Commas separate tags only outside quotes. Backslashes do not escape
        # quotes in entity tags (unlike HTTP quoted strings).
        candidates = []
        start = 0
        quoted = False
        for index, char in enumerate(if_none_match):
            if char == '"':
                quoted = not quoted
            elif char == "," and not quoted:
                candidates.append(if_none_match[start:index])
                start = index + 1
        candidates.append(if_none_match[start:])

        # Validate the entire list before accepting a match: a later malformed
        # tag must invalidate the header even if an earlier tag already matched.
        matched = False
        try:
            for candidate in candidates:
                # Only space and horizontal tab may surround each list element.
                candidate = candidate.strip(" \t")
                if candidate:
                    etag = self.from_header(candidate)
                    matched = matched or self.value() == etag.value()
        except ValueError:
            return False
        return matched
