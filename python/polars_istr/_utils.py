import polars as pl
from typing import Any, Optional, List, Dict
from .type_alias import StrOrExpr


def str_to_expr(x: StrOrExpr) -> pl.Expr:
    if isinstance(x, str):
        return pl.col(x)
    elif isinstance(x, pl.Expr):
        return x
    else:
        raise ValueError("Can only parse str (column name) or Polars expressions.")


def pl_plugin(
    *,
    lib: str,
    symbol: str,
    args: List[StrOrExpr],
    kwargs: Optional[Dict[str, Any]] = None,
    is_elementwise: bool = False,
    returns_scalar: bool = False,
    changes_length: bool = False,
    cast_to_supertype: bool = False,
) -> pl.Expr:
    # pl.__version__ should always be a valid version number, so split returns always 3 strs
    if tuple(int(x) for x in pl.__version__.split(".")) < (0, 20, 16):
        # This will eventually be deprecated?
        first = str_to_expr(args[0])
        return first.register_plugin(
            lib=lib,
            symbol=symbol,
            args=[str_to_expr(x) for x in args[1:]],
            kwargs=kwargs,
            is_elementwise=is_elementwise,
            returns_scalar=returns_scalar,
            changes_length=changes_length,
            cast_to_supertypes=cast_to_supertype,
        )

    from polars.plugins import register_plugin_function

    return register_plugin_function(
        plugin_path=lib,
        args=[str_to_expr(x) for x in args],
        function_name=symbol,
        kwargs=kwargs,
        is_elementwise=is_elementwise,
        returns_scalar=returns_scalar,
        changes_length=changes_length,
        cast_to_supertype=cast_to_supertype,
    )
