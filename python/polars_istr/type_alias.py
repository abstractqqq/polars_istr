from __future__ import annotations
from typing import Union
import sys
import polars as pl

if sys.version_info >= (3, 10):
    from typing import TypeAlias  # noqa
else:  # 3.9, 3.8
    from typing_extensions import TypeAlias  # noqa

StrOrExpr: TypeAlias = Union[str, pl.Expr]
