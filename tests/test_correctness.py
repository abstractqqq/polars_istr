from __future__ import annotations

import pytest
import polars as pl

import polars_istr  # noqa: F401

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

@pytest.mark.parametrize(
        "df, cc, cd, sec_id, is_valid",
        [
            (
                pl.DataFrame(
                    {
                        "isin": [
                            "US0378331005", # AAPL
                            "US0378331008", # AAPL w/ bad check digit
                            "US037833100", # AAPL w/o check digit
                            "CA00206RGB20", # Canadian
                            "XS1550212416", # Other
                            None,
                        ]
                    }
                ),
            ["US", None, None, "CA", "XS",None],
            ["5", None, None, "0", "6", None],
            ["037833100", None, None, "00206RGB2", "155021241", None],
            [True, False, False, True, True, False]
            )
        ],
)
def test_isin1(
        df: pl.DataFrame,
        cc: List[str],
        cd: List[str],
        sec_id: List[str],
        is_valid: List[str],
        ):
    test1 = df.select(
            pl.col("isin").isin.country_code().alias("country_code"),
            pl.col("isin").isin.check_digit().alias("check_digit"),
            pl.col("isin").isin.security_id().alias("security_id"),
            pl.col("isin").isin.is_valid().alias("is_valid"),
        )
    test2 = df.lazy().select(
            pl.col("isin").isin.country_code().alias("country_code"),
            pl.col("isin").isin.check_digit().alias("check_digit"),
            pl.col("isin").isin.security_id().alias("security_id"),
            pl.col("isin").isin.is_valid().alias("is_valid"),
        ).collect()

    ans = pl.DataFrame({
        "country_code": cc,
        "check_digit": cd,
        "security_id": sec_id,
        "is_valid": is_valid,
    })

    assert_frame_equal(test1, ans)
    assert_frame_equal(test2, ans)
    
