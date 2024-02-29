from __future__ import annotations
import polars as pl
from polars.utils.udfs import _get_shared_lib_location

_lib = _get_shared_lib_location(__file__)


@pl.api.register_expr_namespace("iban")
class IbanExt:

    """
    This class contains tools for parsing IBAN format data.

    Polars Namespace: iban

    Example: pl.col("iban_str").iban.country_code()
    """

    def __init__(self, expr: pl.Expr):
        self._expr: pl.Expr = expr

    def country_code(self) -> pl.Expr:
        """
        Returns country code from the IBAN, or null if it cannot be parsed.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_country_code",
            is_elementwise=True,
        )

    def check_digits(self) -> pl.Expr:
        """
        Returns check digits from the IBAN, or null if it cannot be parsed.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_check_digits",
            is_elementwise=True,
        )

    def bban(self) -> pl.Expr:
        """
        Returns BBAN string from the IBAN, or null if it cannot be parsed.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_bban",
            is_elementwise=True,
        )

    def bank_id(self) -> pl.Expr:
        """
        Returns bank identifier from the BBAN portion of the IBAN string,
        or null if it cannot be parsed.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_bank_identifier",
            is_elementwise=True,
        )

    def branch_id(self) -> pl.Expr:
        """
        Returns branch identifier from the BBAN portion of the IBAN string,
        or null if it cannot be parsed.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_branch_identifier",
            is_elementwise=True,
        )

    def is_valid(self) -> pl.Expr:
        """
        Returns a boolean indicating whether the string is a valid IBAN string.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_valid",
            is_elementwise=True,
        )

    def check(self) -> pl.Expr:
        """
        Returns a string that explains whether the IBAN string is valid or not.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_check",
            is_elementwise=True,
        )

    def extract_all(self) -> pl.Expr:
        """
        Returns all information from IBAN and return as a struct. Running this can be
        faster than running the corresponding single queries together.
        """
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_iban_extract_all",
            is_elementwise=True,
        )
