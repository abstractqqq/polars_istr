from __future__ import annotations

import pytest
import polars as pl

import polars_standards  # noqa: F401

from polars.testing import assert_frame_equal
from typing import List


@pytest.mark.parametrize(
    "df, cc, cd, reason, is_valid, bban, bank_id, branch_id",
    [
        (
            pl.DataFrame(
                {
                    "iban": [
                        "AA110011123Z5678",
                        "DE44500105175407324931",
                        "AD1200012030200359100100",
                        "MR0000020001010000123456754",
                    ]
                }
            ),
            [None, "DE", "AD", None],
            [None, "44", "12", None],
            ["Invalid country code", "ok", "ok", "Invalid checksum"],
            [False, True, True, False],
            [None, "500105175407324931", "00012030200359100100", None],
            [None, "50010517", "0001", None],
            [None, None, "2030", None],
        )
    ],
)
def test_iban1(
    df: pl.DataFrame,
    cc: List[str],
    cd: List[str],
    reason: List[str],
    is_valid: List[bool],
    bban: List[str],
    bank_id: List[str],
    branch_id: List[str],
):
    test1 = df.select(
        pl.col("iban").iban.country_code().alias("country_code"),
        pl.col("iban").iban.check_digits().alias("check_digits"),
        pl.col("iban").iban.check().alias("reason"),
        pl.col("iban").iban.is_valid().alias("is_valid"),
        pl.col("iban").iban.bban().alias("bban"),
        pl.col("iban").iban.bank_id().alias("bank_id"),
        pl.col("iban").iban.branch_id().alias("branch_id"),
    )

    test2 = (
        df.lazy()
        .select(
            pl.col("iban").iban.country_code().alias("country_code"),
            pl.col("iban").iban.check_digits().alias("check_digits"),
            pl.col("iban").iban.check().alias("reason"),
            pl.col("iban").iban.is_valid().alias("is_valid"),
            pl.col("iban").iban.bban().alias("bban"),
            pl.col("iban").iban.bank_id().alias("bank_id"),
            pl.col("iban").iban.branch_id().alias("branch_id"),
        )
        .collect()
    )

    ans = pl.DataFrame(
        {
            "country_code": cc,
            "check_digits": cd,
            "reason": reason,
            "is_valid": is_valid,
            "bban": bban,
            "bank_id": bank_id,
            "branch_id": branch_id,
        }
    )

    assert_frame_equal(test1, ans)
    assert_frame_equal(test2, ans)
