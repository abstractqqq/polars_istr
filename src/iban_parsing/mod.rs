use iban::{Iban, IbanLike};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::{fmt::Write, str::FromStr};

// Using Builder is the fastest way after my testing.

fn iban_full_output(_: &[Field]) -> PolarsResult<Field> {
    let cc = Field::new("country_code", DataType::String);
    let cd = Field::new("check_digits", DataType::String);
    let bban = Field::new("bban", DataType::String);
    let bank = Field::new("bank_id", DataType::String);
    let branch = Field::new("branch_id", DataType::String);
    let v: Vec<Field> = vec![cc, cd, bban, bank, branch];
    Ok(Field::new("", DataType::Struct(v)))
}

#[polars_expr(output_type_func=iban_full_output)]
fn pl_iban_full(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut cc_builder = StringChunkedBuilder::new("country_code", ca.len());
    let mut cd_builder = StringChunkedBuilder::new("check_digits", ca.len());
    let mut bban_builder = StringChunkedBuilder::new("bban", ca.len());
    let mut bank_builder = StringChunkedBuilder::new("bank_id", ca.len());
    let mut branch_builder = StringChunkedBuilder::new("branch_id", ca.len());

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

    let cc = cc_builder.finish().into_series();
    let cd = cd_builder.finish().into_series();
    let bban = bban_builder.finish().into_series();
    let bank = bank_builder.finish().into_series();
    let branch = branch_builder.finish().into_series();

    let out = StructChunked::new("iban", &[cc, cd, bban, bank, branch])?;
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_iban_country_code(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut cc_builder = StringChunkedBuilder::new("country_code", ca.len());

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
    let mut cc_builder = StringChunkedBuilder::new("country_code", ca.len());

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
    let mut cc_builder = StringChunkedBuilder::new("country_code", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                cc_builder.append_option(iban.bank_identifier());
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
fn pl_iban_branch_identifier(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut cc_builder = StringChunkedBuilder::new("country_code", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(iban) = Iban::from_str(s) {
                cc_builder.append_option(iban.branch_identifier());
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
fn pl_iban_bban(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut cc_builder = StringChunkedBuilder::new("country_code", ca.len());

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
fn pl_iban_valid(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: BooleanChunked = ca.apply_values_generic(|s| s.parse::<Iban>().is_ok());
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_iban_check(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out = ca.apply_to_buffer(|s, buf| {
        let s = match Iban::from_str(s) {
            Ok(_) => "ok".to_string(),
            Err(e) => e.to_string(),
        };
        write!(buf, "{}", s).unwrap()
    });
    Ok(out.into_series())
}
