from __future__ import annotations
import polars as pl
from ._utils import pl_plugin


def cusip_extract_all(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns a struct containing country_code, issue_num, issuer_num, check_digit,
    or null, if it cannot be parsed.

    Country Code is null for valid CUSIPs which are not (extended) CINS
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_full",
        is_elementwise=True,
    )


def cusip_issue_num(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns the issue number from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_issue_num",
        is_elementwise=True,
    )


def cusip_issuer_num(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns the issuer number from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_issuer_num",
        is_elementwise=True,
    )


def cusip_check_digit(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns check digit from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_check_digit",
        is_elementwise=True,
    )


def cusip_country_code(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns the country code from the CUSIP, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_country_code",
        is_elementwise=True,
    )


def cusip_payload(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns the payload (CUSIP ex. check digit) from the CUSIP, or null if it
    cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_payload",
        is_elementwise=True,
    )


def cusip_is_private_issue(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns true if the issue number is reserved for private use.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_is_private_issue",
        is_elementwise=True,
    )


def cusip_has_private_issuer(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns true if the issuer is reserved for private use.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_cusip_has_private_issuer",
        is_elementwise=True,
    )


def cusip_is_private_use(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns True if either the issuer or issue number is reserved for
    private use.
    """
    return pl_plugin(args=[x], symbol="pl_cusip_is_private_use", is_elementwise=True)


def cusip_is_cins(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns true if this CUSIP number is actually a
    CUSIP International Numbering System (CINS) number,
    false otherwise (i.e., that it has a letter as the first character of its issuer number).
    See also is_cins_base() and is_cins_extended().

    Null if unable to parse.
    """
    return pl_plugin(args=[x], symbol="pl_cusip_is_cins", is_elementwise=True)


def cusip_is_cins_base(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns true if this CUSIP identifier is actually a CUSIP International
    Numbering System (CINS) identifier (with the further restriction that
    it does not use `I`, `O` or `Z` as its country code), false otherwise.
    See also is_cins() and is_cins_extended().

    Null if unable to parse.
    """
    return pl_plugin(args=[x], symbol="pl_cusip_is_cins_base", is_elementwise=True)


def cusip_is_cins_extended(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns true if this CUSIP identifier is actually a CUSIP International
    Numbering System (CINS) identifier (with the further restriction that
    it does not use `I`, `O` or `Z` as its country code), false otherwise.
    See also is_cins() and is_cins_extended().

    Null if unable to parse.
    """
    return pl_plugin(args=[x], symbol="pl_cusip_is_cins_extended", is_elementwise=True)
