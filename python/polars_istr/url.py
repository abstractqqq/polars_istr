from __future__ import annotations
import polars as pl
from polars.utils.udfs import _get_shared_lib_location
from ._utils import pl_plugin, StrOrExpr

_lib = _get_shared_lib_location(__file__)


def url_is_special(x: StrOrExpr) -> pl.Expr:
    """
    Returns a boolean indicating whether the URL has a special scheme or not.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_is_special",
        is_elementwise=True,
    )


def url_host(x: StrOrExpr) -> pl.Expr:
    """
    Returns the host of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_host",
        is_elementwise=True,
    )


def url_path(x: StrOrExpr) -> pl.Expr:
    """
    Returns the path part of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_path",
        is_elementwise=True,
    )


def url_domain(x: StrOrExpr) -> pl.Expr:
    """
    Returns the domain of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_domain",
        is_elementwise=True,
    )


def url_fragment(x: StrOrExpr) -> pl.Expr:
    """
    Returns the fragment of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_fragment",
        is_elementwise=True,
    )


def url_query(x: StrOrExpr) -> pl.Expr:
    """
    Returns the query part of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_query",
        is_elementwise=True,
    )


def url_is_valid(x: StrOrExpr) -> pl.Expr:
    """
    Returns a boolean indicating whether the string is a valid URL string.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_is_valid",
        is_elementwise=True,
    )


def url_check(x: StrOrExpr) -> pl.Expr:
    """
    Returns a string that explains whether the URL string is valid or not.
    """
    return pl_plugin(
        args=[x],
        lib=_lib,
        symbol="pl_url_check",
        is_elementwise=True,
    )


@pl.api.register_expr_namespace("url")
class UrlExt:
    """
    This class contains tools for parsing URL strings.

    Polars Namespace: url

    Example: pl.col("url_str").url.query()
    """

    def __init__(self, expr: pl.Expr):
        self._expr: pl.Expr = expr

    def is_special(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_is_special",
            is_elementwise=True,
        )

    def host(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_host",
            is_elementwise=True,
        )

    def path(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_path",
            is_elementwise=True,
        )

    def domain(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_domain",
            is_elementwise=True,
        )

    def fragment(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_fragment",
            is_elementwise=True,
        )

    def query(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_query",
            is_elementwise=True,
        )

    def is_valid(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_is_valid",
            is_elementwise=True,
        )

    def check(self) -> pl.Expr:
        return self._expr.register_plugin(
            lib=_lib,
            symbol="pl_url_check",
            is_elementwise=True,
        )
