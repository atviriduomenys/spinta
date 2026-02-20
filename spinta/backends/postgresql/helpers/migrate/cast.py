from __future__ import annotations

import enum

import sqlalchemy as sa


class CastSupport(enum.Enum):
    # Doest not support casting
    INVALID = 0
    # Supports based on context (can only be resolved runtime, which can cause unexpected errors)
    UNSAFE = 1
    # Has direct support from backend
    VALID = 2


class CastMatrix:
    _cache: dict[tuple[str, str], CastSupport]
    engine: sa.engine.Engine

    def __init__(self, engine: sa.engine.Engine):
        self._cache = {}
        self.engine = engine

    def supports(self, from_type: str, to_type: str) -> CastSupport:
        key = (from_type, to_type)

        if key in self._cache:
            return self._cache[key]

        self._cache[key] = self.__supports_exec(from_type, to_type)
        return self._cache[key]

    def __supports_exec(self, from_type: str, to_type: str) -> CastSupport:
        """
        Checks postgresql cast table between given type strings
        """

        with self.engine.connect() as conn:
            result = conn.execute(
                sa.text("""
            SELECT 1
            FROM pg_cast
            WHERE castsource = CAST(:source AS regtype)
              AND casttarget = CAST(:target AS regtype)
            LIMIT 1
            """),
                {"source": from_type, "target": to_type},
            ).scalar()

        result = result is not None
        if result:
            return CastSupport.VALID

        result = self.__runtime_cast_exec(from_type, to_type)
        return result

    def __runtime_cast_exec(self, from_type: str, to_type: str) -> CastSupport:
        """
        Checks for unsafe casting between 2 types using runtime
        """
        with self.engine.connect() as conn:
            try:
                conn.execute(
                    sa.text("SELECT NULL::" + from_type + "::" + to_type),
                ).scalar()
                return CastSupport.UNSAFE
            except Exception as _:
                return CastSupport.INVALID
