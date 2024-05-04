from __future__ import annotations
import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from ._utils import pl_plugin, StrOrExpr

_lib = _get_shared_lib_location(__file__)


def cusip_extract_all(x: StrOrExpr) -> pl.Expr:
    """
    Returns a struct containing country_code, issue_num, issuer_num, check_digit,
    or null, if it cannot be parsed.

    Country Code is null for valid CUSIPs which are not (extended) CINS
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_full",
        is_elementwise=True,
    )


def cusip_issue_num(x: StrOrExpr) -> pl.Expr:
    """
    Returns the issue number from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_issue_num",
        is_elementwise=True,
    )


def cusip_issuer_num(x: StrOrExpr) -> pl.Expr:
    """
    Returns the issuer number from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_issuer_num",
        is_elementwise=True,
    )


def cusip_check_digit(x: StrOrExpr) -> pl.Expr:
    """
    Returns check digit from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_check_digit",
        is_elementwise=True,
    )


def cusip_country_code(x: StrOrExpr) -> pl.Expr:
    """
    Returns the country code from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_country_code",
        is_elementwise=True,
    )


def cusip_payload(x: StrOrExpr) -> pl.Expr:
    """
    Returns the payload (CUSIP ex. check digit) from the CUSIP, or null if it
    cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_payload",
        is_elementwise=True,
    )


def cusip_is_private_issue(x: StrOrExpr) -> pl.Expr:
    """
    Returns true if the issue number is reserved for private use.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_is_private_issue",
        is_elementwise=True,
    )


def cusip_has_private_issuer(x: StrOrExpr) -> pl.Expr:
    """
    Returns true if the issuer is reserved for private use.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_cusip_has_private_issuer",
        is_elementwise=True,
    )


def cusip_is_private_use(x: StrOrExpr) -> pl.Expr:
    """
    Returns True if either the issuer or issue number is reserved for
    private use.
    """
    return pl_plugin(args=[x], lib=_lib, symbol="pl_cusip_is_private_use", is_elementwise=True)


def cusip_is_cins(x: StrOrExpr) -> pl.Expr:
    """
    Returns true if this CUSIP number is actually a
    CUSIP International Numbering System (CINS) number,
    false otherwise (i.e., that it has a letter as the first character of its issuer number).
    See also is_cins_base() and is_cins_extended().

    Null if unable to parse.
    """
    return pl_plugin(args=[x], lib=_lib, symbol="pl_cusip_is_cins", is_elementwise=True)


def cusip_is_cins_base(x: StrOrExpr) -> pl.Expr:
    """
    Returns true if this CUSIP identifier is actually a CUSIP International
    Numbering System (CINS) identifier (with the further restriction that
    it does not use `I`, `O` or `Z` as its country code), false otherwise.
    See also is_cins() and is_cins_extended().

    Null if unable to parse.
    """
    return pl_plugin(args=[x], lib=_lib, symbol="pl_cusip_is_cins_base", is_elementwise=True)


def cusip_is_cins_extended(x: StrOrExpr) -> pl.Expr:
    """
    Returns true if this CUSIP identifier is actually a CUSIP International
    Numbering System (CINS) identifier (with the further restriction that
    it does not use `I`, `O` or `Z` as its country code), false otherwise.
    See also is_cins() and is_cins_extended().

    Null if unable to parse.
    """
    return pl_plugin(args=[x], lib=_lib, symbol="pl_cusip_is_cins_extended", is_elementwise=True)


@pl.api.register_expr_namespace("cusip")
class CusipExt:
    """
    This class contains tools for parsing CUSIP/CINS and Extended CINS format data.

    Polars Namespace: cusip

    Example: pl.col("cusip_cins_string").cusip.country_code()
    """

    def __init__(self, expr: pl.Expr):
        self._expr: pl.Expr = expr

    def extract_all(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_full",
            is_elementwise=True,
        )

    def issue_num(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_issue_num",
            is_elementwise=True,
        )

    def issuer_num(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_issuer_num",
            is_elementwise=True,
        )

    def check_digit(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_check_digit",
            is_elementwise=True,
        )

    def country_code(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_country_code",
            is_elementwise=True,
        )

    def payload(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_payload",
            is_elementwise=True,
        )

    def is_private_issue(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_is_private_issue",
            is_elementwise=True,
        )

    def has_private_issuer(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_cusip_has_private_issuer",
            is_elementwise=True,
        )

    def is_private_use(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib, symbol="pl_cusip_is_private_use", is_elementwise=True
        )

    def is_cins(self) -> pl.Expr:
        return self._expr.register_plugin(lib=_lib, symbol="pl_cusip_is_cins", is_elementwise=True)

    def is_cins_base(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib, symbol="pl_cusip_is_cins_base", is_elementwise=True
        )

    def is_cins_extended(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib, symbol="pl_cusip_is_cins_extended", is_elementwise=True
        )
