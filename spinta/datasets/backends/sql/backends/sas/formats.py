"""
SAS Format Constants and Type Mapping.

This module provides SAS format definitions and type mapping logic for
converting SAS data types to SQLAlchemy types.

SAS uses formats to indicate how data should be displayed and what the
underlying data type represents. This module provides:
- Format constant definitions for categorization
- Type mapping logic from SAS types to SQLAlchemy types

SAS has only two basic data types (numeric and character), but formats
are used to indicate specialized types like dates, times, booleans,
and formatted numbers.
"""

import re
import logging
from sqlalchemy import types as sqltypes

try:
    from geoalchemy2.types import Geometry
except ImportError:
    Geometry = None


logger = logging.getLogger(__name__)


# =============================================================================
# ISO 8601 Format Prefixes
# =============================================================================

ISO_DATE_PREFIXES = ("E8601DA",)
ISO_DATETIME_PREFIXES = ("E8601DT",)
ISO_TIME_PREFIXES = ("E8601TM",)


# =============================================================================
# Standard SAS Date Formats
# =============================================================================

DATE_FORMATS = frozenset(
    [
        "DATE",
        "DAY",
        "DDMMYY",
        "MMDDYY",
        "YYMMDD",
        "YYMM",
        "YYMON",
        "YYQ",
        "YYQR",
        "JULDAY",
        "JULIAN",
        "MONYY",
        "MONTH",
        "QTRR",
        "WEEKDATE",
        "WEEKDAY",
        "WORDDATE",
        "YEAR",
        "NENGO",
        "MINGUO",
        "PDJULG",
        "PDJULI",
        "EURDFDD",
        "EURDFDE",
        "EURDFDN",
        "EURDFDMY",
        "EURDFMY",
        "EURDFWDX",
        "EURDFWKX",
        "WEEKV",
    ]
)

# Formats that render dates as strings (names, etc.)
DATE_STRING_FORMATS = frozenset(
    [
        "DOWNAME",
        "MONNAME",
        "QTR",
        "WEEKDATX",
        "WORDDATX",
        "YYQ",
        "YYQC",
    ]
)


# =============================================================================
# SAS DateTime Formats
# =============================================================================

DATETIME_FORMATS = frozenset(
    [
        "DATETIME",
        "DTYYQC",
    ]
)

TIMESTAMP_FORMATS = frozenset(
    [
        "TODSTAMP",
        "DTMONYY",
        "DTWKDATX",
        "DTYEAR",
        "DTYYQC",
    ]
)


# =============================================================================
# SAS Time Formats
# =============================================================================

TIME_FORMATS = frozenset(
    [
        "TIME",
        "TIMEAMPM",
        "TOD",
        "HHMM",
        "HOUR",
        "MMSS",
        "NLTIME",
        "NLTIMAP",
        "STIMER",
    ]
)


# =============================================================================
# Boolean Formats
# =============================================================================

BOOLEAN_FORMATS = frozenset(
    [
        "YESNO",
        "YN",
        "BOOLEAN",
    ]
)


# =============================================================================
# Numeric Formats (with potential decimal places)
# =============================================================================

NUMERIC_FORMATS = frozenset(
    [
        "COMMA",
        "COMMAX",
        "EURX",
        "EURO",
        "PERCENT",
        "BEST",
        "NLMNY",
        "NLMNYI",
        "NLNUM",
        "NLNUMI",
        "NLPCT",
        "NLPCTI",
        "SSN",
        "PVALUE",
        "NEGPAREN",
        "ROMAN",
        "WORDS",
        "WORDF",
    ]
)


# =============================================================================
# Money Formats
# =============================================================================

MONEY_FORMATS = frozenset(
    [
        "DOLLAR",
        "NLMNY",
    ]
)


# =============================================================================
# Integer Formats (no decimals)
# =============================================================================

INTEGER_FORMATS = frozenset(
    [
        "Z",
        "ZD",
        "BINARY",
        "HEX",
        "OCTAL",
        "IB",
        "PD",
        "PK",
        "RB",
        "PIB",
        "ZIP",
        "S370FF",
        "S370FIB",
        "S370FPIB",
        "S370FPD",
        "S370FRB",
        "S370FZD",
    ]
)


# =============================================================================
# Standard Numeric Format Identifiers
# =============================================================================

STANDARD_NUMERIC_FORMATS = frozenset(
    [
        "F",
        "NUMX",
        "NUMERIC",
    ]
)


# =============================================================================
# Format Parsing Functions
# =============================================================================

# Pre-compiled regex for performance
_SAS_FORMAT_REGEX = re.compile(r"^([A-Z]+\$?)(\d+)?(?:\.(\d+)?)?\.?$", re.IGNORECASE)


def parse_sas_format(format_str: str) -> dict:
    """
    Parse a SAS format string to extract format name, width, and decimals.

    SAS formats follow the pattern: NAME[width[.decimals]]

    Examples:
        "COMMA12.2" -> {"format": "COMMA", "width": 12, "decimals": 2}
        "DATE9." -> {"format": "DATE", "width": 9, "decimals": None}
        "DATETIME20." -> {"format": "DATETIME", "width": 20, "decimals": None}
        "$20." -> {"format": "$", "width": 20, "decimals": None}

    Args:
        format_str: SAS format string (e.g., "COMMA12.2", "DATE9.")

    Returns:
        Dictionary with 'format', 'width', and 'decimals' keys
    """
    if not format_str:
        return {"format": None, "width": None, "decimals": None}

    try:
        # Match SAS format pattern: NAME[width[.decimals]]
        match = _SAS_FORMAT_REGEX.match(format_str.strip())

        if match:
            # Group 1 is format name (case insensitive in regex, but we upper it)
            format_name = match.group(1).upper()
            width = int(match.group(2)) if match.group(2) else None
            decimals = int(match.group(3)) if match.group(3) else None

            return {"format": format_name, "width": width, "decimals": decimals}
        else:
            # If regex doesn't match, try simpler parse
            clean_format = format_str.upper().strip().rstrip(".")
            return {"format": clean_format, "width": None, "decimals": None}
    except Exception as e:
        logger.warning(f"Failed to parse SAS format '{format_str}': {e}")
        return {"format": format_str.upper() if format_str else None, "width": None, "decimals": None}


# =============================================================================
# Type Mapping Functions
# =============================================================================


def map_sas_type_to_sqlalchemy(sas_type: str | None, length, format_str: str | None, cache: dict | None = None):
    """
    Map SAS data types to SQLAlchemy types with comprehensive format support.

    SAS has two basic types (numeric and character) but uses formats
    to indicate specialized types like dates, times, booleans, and formatted numbers.

    This function handles 50+ SAS formats including date/time, numeric, and special formats.

    Args:
        sas_type: SAS type ('num' or 'char')
        length: Column length (can be string representation of int or float)
        format_str: SAS format string (e.g., 'DATE9.', 'DATETIME20.', 'COMMA12.2')
        cache: Optional cache dictionary for performance optimization

    Returns:
        SQLAlchemy type instance
    """

    def _cache_and_return(sa_type):
        """Helper to cache and return a type."""
        if cache is not None and cache_key:
            cache[cache_key] = sa_type
        return sa_type

    try:
        # Check cache first for performance
        cache_key = f"{sas_type}_{format_str}" if cache is not None else None
        if cache_key and cache_key in cache:
            return cache[cache_key]

        # Handle character types
        if sas_type and sas_type.lower() == "char":
            char_len = _parse_char_length(length)

            # Special character formats
            if format_str and format_str.upper().startswith("$GEOREF"):
                return _cache_and_return(Geometry() if Geometry else sqltypes.VARCHAR(length=char_len))

            return _cache_and_return(sqltypes.VARCHAR(length=char_len))

        # Numeric type - determine specific type based on format
        if format_str:
            format_info = parse_sas_format(format_str)
            format_name = format_info["format"]

            if format_name:
                # Try to map format to SQLAlchemy type
                sa_type = _map_numeric_format(format_name, format_info, format_str)
                if sa_type:
                    return _cache_and_return(sa_type)

        # Default numeric type for unformatted or unrecognized formats
        return _cache_and_return(sqltypes.NUMERIC())

    except Exception as e:
        return _handle_mapping_error(e, sas_type, length, format_str)


def _parse_char_length(length) -> int:
    """Parse character length from potentially float string."""
    try:
        return int(float(length))
    except (ValueError, TypeError):
        return 255


def _map_numeric_format(format_name: str, format_info: dict, format_str: str):
    """
    Map a numeric format name to SQLAlchemy type.

    Returns the SQLAlchemy type or None if format not recognized.
    """
    decimals = format_info["decimals"]

    # ISO 8601 formats
    if format_name.startswith(ISO_DATE_PREFIXES):
        return sqltypes.DATE()
    if format_name.startswith(ISO_DATETIME_PREFIXES):
        return sqltypes.DATETIME()
    if format_name.startswith(ISO_TIME_PREFIXES):
        return sqltypes.TIME()

    # DateTime and Timestamp formats (must check before DATE)
    if format_name.startswith("DATETIME") or format_name in DATETIME_FORMATS or format_name in TIMESTAMP_FORMATS:
        return sqltypes.DATETIME()

    # Date string formats
    if format_name in DATE_STRING_FORMATS:
        return sqltypes.VARCHAR(length=255)

    # Standard SAS date formats
    if _matches_any_format(format_name, DATE_FORMATS):
        return sqltypes.DATE()

    # Time formats
    if _matches_any_format(format_name, TIME_FORMATS):
        return sqltypes.TIME()

    # Boolean formats
    if format_name in BOOLEAN_FORMATS:
        return sqltypes.BOOLEAN()

    # Money formats - always return Numeric
    if _matches_any_format(format_name, MONEY_FORMATS):
        return sqltypes.NUMERIC(precision=format_info.get("width"), scale=decimals)

    # Binary formats
    if format_name.startswith("HEX"):
        return sqltypes.LargeBinary()

    # Numeric formats with potential decimal places
    if _matches_any_format(format_name, NUMERIC_FORMATS):
        return sqltypes.NUMERIC(precision=format_info.get("width"), scale=decimals) if decimals else sqltypes.INTEGER()

    # Explicit integer formats
    if _matches_any_format(format_name, INTEGER_FORMATS):
        return sqltypes.INTEGER()

    # Standard numeric formats - check for decimals
    if format_name in STANDARD_NUMERIC_FORMATS:
        return sqltypes.NUMERIC(precision=format_info.get("width"), scale=decimals) if decimals else sqltypes.INTEGER()

    # Special numeric cases
    if format_name.isdigit():
        if decimals:
            return sqltypes.NUMERIC(precision=int(format_name), scale=decimals)
        return sqltypes.INTEGER()

    # Unrecognized format
    logger.debug(f"Unrecognized SAS format: {format_str}, using default NUMERIC type")
    return None


def _matches_any_format(format_name: str, format_set: frozenset) -> bool:
    """Check if format_name starts with any format in the set."""
    return any(format_name.startswith(fmt) for fmt in format_set)


def _handle_mapping_error(error: Exception, sas_type: str | None, length, format_str: str | None):
    """Handle errors in type mapping and return safe defaults."""
    logger.warning(
        f"Error mapping SAS type to SQLAlchemy: sas_type={sas_type}, "
        f"length={length}, format={format_str}, error={error}"
    )

    if sas_type and sas_type.lower() == "char":
        return sqltypes.VARCHAR(length=_parse_char_length(length))
    return sqltypes.NUMERIC()
