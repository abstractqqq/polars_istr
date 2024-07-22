import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl

from .type_alias import StrOrExpr

_POLARS_LEGACY_SUPPORT = tuple(int(re.sub("[^0-9]", "", x)) for x in pl.__version__.split(".")) < (
    0,
    20,
    16,
)
_IS_POLARS_V1 = pl.__version__.startswith("1")

_PLUGIN_PATH = Path(__file__).parent

_PLUGIN_LIB_LEGACY = os.path.join(
    os.path.dirname(__file__),
    next(
        filter(
            lambda file: file.endswith((".so", ".dll", ".pyd")),
            os.listdir(os.path.dirname(__file__)),
        )
    ),
)


def str_to_expr(x: StrOrExpr) -> pl.Expr:
    if isinstance(x, str):
        return pl.col(x)
    elif isinstance(x, pl.Expr):
        return x
    else:
        raise ValueError("Can only parse str (column name) or Polars expressions.")


def pl_plugin(
    *,
    symbol: str,
    args: List[Union[pl.Series, pl.Expr]],
    kwargs: Optional[Dict[str, Any]] = None,
    is_elementwise: bool = False,
    returns_scalar: bool = False,
    changes_length: bool = False,
    cast_to_supertype: bool = False,
) -> pl.Expr:
    if _POLARS_LEGACY_SUPPORT:
        # This will eventually be deprecated, yes
        return args[0].register_plugin(
            lib=_PLUGIN_LIB_LEGACY,
            symbol=symbol,
            args=args[1:],
            kwargs=kwargs,
            is_elementwise=is_elementwise,
            returns_scalar=returns_scalar,
            changes_length=changes_length,
            cast_to_supertypes=cast_to_supertype,
        )

    from polars.plugins import register_plugin_function

    return register_plugin_function(
        plugin_path=_PLUGIN_PATH,
        args=args,
        function_name=symbol,
        kwargs=kwargs,
        is_elementwise=is_elementwise,
        returns_scalar=returns_scalar,
        changes_length=changes_length,
        cast_to_supertype=cast_to_supertype,
    )
