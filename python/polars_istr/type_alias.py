from __future__ import annotations
import sys

if sys.version_info >= (3, 10):
    from typing import TypeAlias  # noqa
else:  # 3.9, 3.8
    from typing_extensions import TypeAlias  # noqa
