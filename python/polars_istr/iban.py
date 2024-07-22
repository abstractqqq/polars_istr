from __future__ import annotations
import polars as pl
from ._utils import pl_plugin


def iban_country_code(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns country code from the IBAN, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_country_code",
        is_elementwise=True,
    )


def iban_check_digits(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns check digits from the IBAN, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_check_digits",
        is_elementwise=True,
    )


def iban_bban(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns BBAN string from the IBAN, or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_bban",
        is_elementwise=True,
    )


def iban_bank_id(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns bank identifier from the BBAN portion of the IBAN string,
    or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_bank_identifier",
        is_elementwise=True,
    )


def iban_branch_id(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns branch identifier from the BBAN portion of the IBAN string,
    or null if it cannot be parsed.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_branch_identifier",
        is_elementwise=True,
    )


def iban_is_valid(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns a boolean indicating whether the string is a valid IBAN string.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_is_valid",
        is_elementwise=True,
    )


def iban_check(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns a string that explains whether the IBAN string is valid or not.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_check",
        is_elementwise=True,
    )


def iban_extract_all(x: pl.Series | pl.Expr) -> pl.Expr:
    """
    Returns all information from IBAN and return as a struct. Running this can be
    faster than running the corresponding single queries together.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_iban_extract_all",
        is_elementwise=True,
    )
