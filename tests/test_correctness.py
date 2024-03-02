import polars as pl

import polars_standards # noqa: F401

from polars.testing import assert_frame_equal

from typing import List

import pytest


# There are no valid test cases for Extended CINS or Private Issue(r) since I could not 
# find any existing cusips readily available (including in the cusip crate).
# If anyone has encountered one and would like it added to the tests, please raise an
# issue.
@pytest.mark.parametrize(
        """df, issue_num, issuer_num, check_digit, country_code,
    payload, is_private_issue, has_private_issuer, is_private_use, is_cins,
    is_cins_base, is_cins_extended""",
    [
        (
            pl.DataFrame({
                "cusip": [
                    "303075105", # regular cusip (FactSet - Common Stock)
                    "30307510", # regular cusip ex. check digit
                    "G0052B105", # regular CINS (Abingdon Capital PLC - Shares)
                    "HELLOWORLD", # Invalid
                    ]
                }),
            ["10", None, "10", None],
            ["303075", None, "0052B", None],
            ["5", None, "5", None],
            [None, None, "G", None],
            ["30307510", None, "G0052B10", None],
            [False, None, False, None],
            [False, None, False, None],
            [False, None, False, None],
            [False, None, True, None],
            [False, None, True, None],
            [False, None, False, None]
        )
    ],
    )
def test_cusip(
        df: pl.DataFrame,
        issue_num: List[str],
        issuer_num: List[str],
        check_digit: List[str],
        country_code: List[str],
        payload: List[str],
        is_private_issue: List[str],
        has_private_issuer: List[str],
        is_private_use: List[str],
        is_cins: List[str],
        is_cins_base: List[str],
        is_cins_extended: List[str]
        ):

    test1 = df.select(
            pl.col("cusip").cusip.issue_num().alias("issue_num"),
            pl.col("cusip").cusip.issuer_num().alias("issuer_num"),
            pl.col("cusip").cusip.check_digit().alias("check_digit"),
            pl.col("cusip").cusip.country_code().alias("country_code"),
            pl.col("cusip").cusip.payload().alias("payload"),
            pl.col("cusip").cusip.is_private_issue().alias("is_private_issue"),
            pl.col("cusip").cusip.has_private_issuer().alias("has_private_issuer"),
            pl.col("cusip").cusip.is_private_use().alias("is_private_use"),
            pl.col("cusip").cusip.is_cins().alias("is_cins"),
            pl.col("cusip").cusip.is_cins_base().alias("is_cins_base"),
            pl.col("cusip").cusip.is_cins_extended().alias("is_cins_extended"),
        )

    test2 = (
            df.lazy()
            .select(
                pl.col("cusip").cusip.issue_num().alias("issue_num"),
                pl.col("cusip").cusip.issuer_num().alias("issuer_num"),
                pl.col("cusip").cusip.check_digit().alias("check_digit"),
                pl.col("cusip").cusip.country_code().alias("country_code"),
                pl.col("cusip").cusip.payload().alias("payload"),
                pl.col("cusip").cusip.is_private_issue().alias("is_private_issue"),
                pl.col("cusip").cusip.has_private_issuer().alias("has_private_issuer"),
                pl.col("cusip").cusip.is_private_use().alias("is_private_use"),
                pl.col("cusip").cusip.is_cins().alias("is_cins"),
                pl.col("cusip").cusip.is_cins_base().alias("is_cins_base"),
                pl.col("cusip").cusip.is_cins_extended().alias("is_cins_extended"),
                ).collect()
            )
    ans = pl.DataFrame({
        "issue_num": issue_num,
        "issuer_num": issuer_num,
        "check_digit": check_digit,
        "country_code": country_code,
        "payload": payload,
        "is_private_issue": is_private_issue,
        "has_private_issuer": has_private_issuer,
        "is_private_use": is_private_use,
        "is_cins": is_cins,
        "is_cins_base": is_cins_base,
        "is_cins_extended": is_cins_extended,
        })
    
    assert_frame_equal(test1, ans)
    assert_frame_equal(test2, ans)
