from __future__ import annotations
import polars as pl
from ._utils import pl_plugin


def isin_country_code(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns country code from the ISIN, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_isin_country_code",
        is_elementwise=True,
    )


def isin_check_digit(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns check digits from the ISIN, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_isin_check_digit",
        is_elementwise=True,
    )


def isin_security_id(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns the 9-digit security identifier of the ISIN, or null if it cannot
    be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_isin_security_id",
        is_elementwise=True,
    )


def isin_is_valid(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns a boolean indicating whether the string is a valid ISIN string.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_isin_is_valid",
        is_elementwise=True,
    )


def isin_extract_all(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns all information from ISIN and return as a struct. Empty string means the part cannot
    be extracted. Running this can be faster than running the corresponding single queries together.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_isin_full",
        is_elementwise=True,
    )
