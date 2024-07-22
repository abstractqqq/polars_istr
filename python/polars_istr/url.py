from __future__ import annotations
import polars as pl
from ._utils import pl_plugin


def url_is_special(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns a boolean indicating whether the URL has a special scheme or not.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_is_special",
        is_elementwise=True,
    )


def url_host(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns the host of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_host",
        is_elementwise=True,
    )


def url_path(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns the path part of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_path",
        is_elementwise=True,
    )


def url_domain(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns the domain of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_domain",
        is_elementwise=True,
    )


def url_fragment(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns the fragment of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_fragment",
        is_elementwise=True,
    )


def url_query(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns the query part of the URL, if possible.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_query",
        is_elementwise=True,
    )


def url_is_valid(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns a boolean indicating whether the string is a valid URL string.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_is_valid",
        is_elementwise=True,
    )


def url_check(x: pl.Expr | pl.Series) -> pl.Expr:
    """
    Returns a string that explains whether the URL string is valid or not.
    """
    return pl_plugin(
        args=[x],
        symbol="pl_url_check",
        is_elementwise=True,
    )
