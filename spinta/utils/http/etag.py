from dataclasses import dataclass, replace


@dataclass(frozen=True)
class ETag:
    """An HTTP entity tag with an opaque value and explicit strength."""

    _value: str
    weak: bool = False

    @classmethod
    def from_header(cls, header: str) -> "ETag":
        weak = header.startswith("W/")
        value = header.removeprefix("W/").removeprefix('"').removesuffix('"')
        return cls(value, weak=weak)

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
        """Compare If-None-Match using the original weak comparison rules."""
        if if_none_match.strip() == "*":
            return True

        # The header can list several cached ETags; any matching value suffices.
        # Ignore W/ so strong and weak tags compare equally for If-None-Match.
        expected = str(self.to_strong())
        return any(candidate.strip().removeprefix("W/") == expected for candidate in if_none_match.split(","))
