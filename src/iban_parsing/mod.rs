use iban::{Iban, IbanLike};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::str::FromStr;

// Using Builder seems to be the fastest way.
fn iban_full_output(_: &[Field]) -> PolarsResult<Field> {
    let cc = Field::new("country_code".into(), DataType::String);
    let cd = Field::new("check_digits".into(), DataType::String);
    let bban = Field::new("bban".into(), DataType::String);
    let bank = Field::new("bank_id".into(), DataType::String);
    let branch = Field::new("branch_id".into(), DataType::String);
    let v: Vec<Field> = vec![cc, cd, bban, bank, branch];
    Ok(Field::new("".into(), DataType::Struct(v)))
}

#[polars_expr(output_type_func=iban_full_output)]
fn pl_iban_extract_all(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut cc_builder = StringChunkedBuilder::new("country_code".into(), ca.len());
    let mut cd_builder = StringChunkedBuilder::new("check_digits".into(), ca.len());
    let mut bban_builder = StringChunkedBuilder::new("bban".into(), ca.len());
    let mut bank_builder = StringChunkedBuilder::new("bank_id".into(), ca.len());
    let mut branch_builder = StringChunkedBuilder::new("branch_id".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                cc_builder.append_value(iban.country_code());
                cd_builder.append_value(iban.check_digits_str());
                bban_builder.append_value(iban.bban());
                bank_builder.append_option(iban.bank_identifier());
                branch_builder.append_option(iban.branch_identifier());
            } else {
                cc_builder.append_null();
                cd_builder.append_null();
                bban_builder.append_null();
                bank_builder.append_null();
                branch_builder.append_null();
            }
        } else {
            cc_builder.append_null();
            cd_builder.append_null();
            bban_builder.append_null();
            bank_builder.append_null();
            branch_builder.append_null();
        }
    });

    let cc = cc_builder.finish().into_series().into_column();
    let cd = cd_builder.finish().into_series().into_column();
    let bban = bban_builder.finish().into_series().into_column();
    let bank = bank_builder.finish().into_series().into_column();
    let branch = branch_builder.finish().into_series().into_column();

    let out = StructChunked::from_columns(
        "iban".into(), 
        cc.len(),
        &[cc, cd, bban, bank, branch]
    )?;
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_iban_country_code(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut cc_builder = StringChunkedBuilder::new("country_code".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                cc_builder.append_value(iban.country_code());
            } else {
                cc_builder.append_null();
            }
        } else {
            cc_builder.append_null();
        }
    });
    let out = cc_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_iban_check_digits(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut cc_builder = StringChunkedBuilder::new("check_digits".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                cc_builder.append_value(iban.check_digits_str());
            } else {
                cc_builder.append_null();
            }
        } else {
            cc_builder.append_null();
        }
    });
    let out = cc_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_iban_bank_identifier(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut ba_builder = StringChunkedBuilder::new("bank_id".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                ba_builder.append_option(iban.bank_identifier());
            } else {
                ba_builder.append_null();
            }
        } else {
            ba_builder.append_null();
        }
    });
    let out = ba_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_iban_branch_identifier(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut br_builder = StringChunkedBuilder::new("branch_id".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                br_builder.append_option(iban.branch_identifier());
            } else {
                br_builder.append_null();
            }
        } else {
            br_builder.append_null();
        }
    });
    let out = br_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_iban_bban(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut cc_builder = StringChunkedBuilder::new("bban".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                cc_builder.append_value(iban.bban());
            } else {
                cc_builder.append_null();
            }
        } else {
            cc_builder.append_null();
        }
    });
    let out = cc_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_iban_is_valid(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: BooleanChunked = ca.apply_nonnull_values_generic(
        DataType::Boolean, 
        |s| s.parse::<Iban>().is_ok()
    );
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_iban_check(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out = ca.apply_values(|s| {
        match Iban::from_str(s) {
            Ok(_) => "ok".into(),
            Err(e) => match e {
                iban::ParseIbanError::InvalidBaseIban { source } => match source {
                    iban::ParseBaseIbanError::InvalidFormat => {
                        "Invalid format (len/char)".into()
                    }
                    iban::ParseBaseIbanError::InvalidChecksum => "Invalid checksum".into(),
                },
                iban::ParseIbanError::InvalidBban(_) => "Invalid Bban".into(),
                iban::ParseIbanError::UnknownCountry(_) => "Invalid country code".into(),
            },
        }

    });
    Ok(out.into_series())
}
