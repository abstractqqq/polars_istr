import polars as pl


from polars.testing import assert_frame_equal

from typing import List

import pytest


import polars_istr  # noqa: F401

from typing import Optional


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
            pl.DataFrame(
                {
                    "cusip": [
                        "303075105",  # regular cusip (FactSet - Common Stock)
                        "30307510",  # regular cusip ex. check digit
                        "G0052B105",  # regular CINS (Abingdon Capital PLC - Shares)
                        "HELLOWORLD",  # Invalid
                    ]
                }
            ),
            ["10", None, "10", None],
            ["303075", None, "0052B", None],
            ["5", None, "5", None],
            [None, None, "G", None],
            ["30307510", None, "G0052B10", None],
            [False, None, False, None],
            [False, None, False, None],
            [False, None, False, None],
            [False, None, True, None],
            [
                None,
                None,
                True,
                None,
            ],  # TODO for these last 2, should regular cusips return False or None?
            [None, None, False, None],
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
    is_cins_extended: List[str],
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
        )
        .collect()
    )
    ans = pl.DataFrame(
        {
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
        }
    )

    assert_frame_equal(test1, ans)
    assert_frame_equal(test2, ans)


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
    cc: List[Optional[str]],
    cd: List[Optional[str]],
    reason: List[Optional[str]],
    is_valid: List[Optional[bool]],
    bban: List[Optional[str]],
    bank_id: List[Optional[str]],
    branch_id: List[Optional[str]],
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
    "df, host, domain, fragment, path, query, check, is_valid, is_special",
    [
        (
            pl.DataFrame(
                {
                    "url": [
                        "https://example.com/data.csv#row=4",
                        "google.com",
                        "ww.google.com",
                        "abc123@email.com",
                        "https://127.0.0.1/",
                        "https://test.com/",
                        "file:///tmp/foo",
                        "https://example.com/products?page=2&sort=desc",
                        None,
                    ]
                }
            ),
            ["example.com", None, None, None, "127.0.0.1", "test.com", None, "example.com", None],
            ["example.com", None, None, None, None, "test.com", None, "example.com", None],
            ["row=4", None, None, None, None, None, None, None, None],
            ["/data.csv", None, None, None, "/", "/", "/tmp/foo", "/products", None],
            [None, None, None, None, None, None, None, "page=2&sort=desc", None],
            [
                "ok",
                "relative URL without a base",
                "relative URL without a base",
                "relative URL without a base",
                "ok",
                "ok",
                "ok",
                "ok",
                None,
            ],
            [True, False, False, False, True, True, True, True, None],
            [True, None, None, None, True, True, True, True, None],
        )
    ],
)
def test_url1(
    df: pl.DataFrame,
    host: List[Optional[str]],
    domain: List[Optional[str]],
    fragment: List[Optional[str]],
    path: List[Optional[str]],
    query: List[Optional[str]],
    check: List[Optional[str]],
    is_valid: List[Optional[bool]],
    is_special: List[Optional[bool]],
):
    test1 = df.select(
        pl.col("url").url.host().alias("host"),
        pl.col("url").url.domain().alias("domain"),
        pl.col("url").url.fragment().alias("fragment"),
        pl.col("url").url.path().alias("path"),
        pl.col("url").url.query().alias("query"),
        pl.col("url").url.check().alias("check"),
        pl.col("url").url.is_valid().alias("is_valid"),
        pl.col("url").url.is_special().alias("is_special"),
    )

    test2 = (
        df.lazy()
        .select(
            pl.col("url").url.host().alias("host"),
            pl.col("url").url.domain().alias("domain"),
            pl.col("url").url.fragment().alias("fragment"),
            pl.col("url").url.path().alias("path"),
            pl.col("url").url.query().alias("query"),
            pl.col("url").url.check().alias("check"),
            pl.col("url").url.is_valid().alias("is_valid"),
            pl.col("url").url.is_special().alias("is_special"),
        )
        .collect()
    )

    ans = pl.DataFrame(
        {
            "host": host,
            "domain": domain,
            "fragment": fragment,
            "path": path,
            "query": query,
            "check": check,
            "is_valid": is_valid,
            "is_special": is_special,
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
                        "US0378331005",  # AAPL
                        "US0378331008",  # AAPL w/ bad check digit
                        "US037833100",  # AAPL w/o check digit
                        "CA00206RGB20",  # Canadian
                        "XS1550212416",  # Other
                        None,
                    ]
                }
            ),
            ["US", None, None, "CA", "XS", None],
            ["5", None, None, "0", "6", None],
            ["037833100", None, None, "00206RGB2", "155021241", None],
            [True, False, False, True, True, False],
        )
    ],
)
def test_isin1(
    df: pl.DataFrame,
    cc: List[Optional[str]],
    cd: List[Optional[str]],
    sec_id: List[Optional[str]],
    is_valid: List[Optional[str]],
):
    test1 = df.select(
        pl.col("isin").isin.country_code().alias("country_code"),
        pl.col("isin").isin.check_digit().alias("check_digit"),
        pl.col("isin").isin.security_id().alias("security_id"),
        pl.col("isin").isin.is_valid().alias("is_valid"),
    )
    test2 = (
        df.lazy()
        .select(
            pl.col("isin").isin.country_code().alias("country_code"),
            pl.col("isin").isin.check_digit().alias("check_digit"),
            pl.col("isin").isin.security_id().alias("security_id"),
            pl.col("isin").isin.is_valid().alias("is_valid"),
        )
        .collect()
    )

    ans = pl.DataFrame(
        {
            "country_code": cc,
            "check_digit": cd,
            "security_id": sec_id,
            "is_valid": is_valid,
        }
    )

    assert_frame_equal(test1, ans)
    assert_frame_equal(test2, ans)
