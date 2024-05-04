from __future__ import annotations
import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from ._utils import pl_plugin, StrOrExpr

_lib = _get_shared_lib_location(__file__)


def isin_country_code(x: StrOrExpr) -> pl.Expr:
    """
    Returns country code from the ISIN, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_isin_country_code",
        is_elementwise=True,
    )


def isin_check_digit(x: StrOrExpr) -> pl.Expr:
    """
    Returns check digits from the ISIN, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_isin_check_digit",
        is_elementwise=True,
    )


def isin_security_id(x: StrOrExpr) -> pl.Expr:
    """
    Returns the 9-digit security identifier of the ISIN, or null if it cannot
    be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_isin_security_id",
        is_elementwise=True,
    )


def isin_is_valid(x: StrOrExpr) -> pl.Expr:
    """
    Returns a boolean indicating whether the string is a valid ISIN string.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_isin_is_valid",
        is_elementwise=True,
    )


def isin_extract_all(x: StrOrExpr) -> pl.Expr:
    """
    Returns all information from ISIN and return as a struct. Empty string means the part cannot
    be extracted. Running this can be faster than running the corresponding single queries together.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_isin_full",
        is_elementwise=True,
    )


@pl.api.register_expr_namespace("isin")
class IsinExt:
    """
    This class contains tools for parsing ISIN format data.

    Polars Namespace: isin

    Example: pl.col("isin_str").isin.country_code()
    """

    def __init__(self, expr: pl.Expr):
        self._expr: pl.Expr = expr

    def country_code(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_isin_country_code",
            is_elementwise=True,
        )

    def check_digit(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_isin_check_digit",
            is_elementwise=True,
        )

    def security_id(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_isin_security_id",
            is_elementwise=True,
        )

    def is_valid(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_isin_is_valid",
            is_elementwise=True,
        )

    def extract_all(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_isin_full",
            is_elementwise=True,
        )
