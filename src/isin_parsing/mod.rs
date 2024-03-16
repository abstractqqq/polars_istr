use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn isin_full_output(_: &[Field]) -> PolarsResult<Field> {
    let cc = Field::new("country_code", DataType::String);
    let id = Field::new("security_id", DataType::String);
    let cd = Field::new("check_digit", DataType::String);

    let v: Vec<Field> = vec![cc, id, cd];
    Ok(Field::new("", DataType::Struct(v)))
}

#[polars_expr(output_type_func=isin_full_output)]
fn pl_isin_full(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut cc_builder = StringChunkedBuilder::new("country_code", ca.len());
    let mut id_builder = StringChunkedBuilder::new("security_id", ca.len());
    let mut cd_builder = StringChunkedBuilder::new("check_digit", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(isin) = isin::parse(s) {
                cc_builder.append_value(isin.prefix());
                id_builder.append_value(isin.basic_code());
                cd_builder.append_value(isin.check_digit().to_string());
            } else {
                cc_builder.append_null();
                id_builder.append_null();
                cd_builder.append_null();
            }
        } else {
            cc_builder.append_null();
            id_builder.append_null();
            cd_builder.append_null();
        }
    });

    let cc = cc_builder.finish().into_series();
    let id = id_builder.finish().into_series();
    let cd = cd_builder.finish().into_series();

    let out = StructChunked::new("isin", &[cc, id, cd])?;
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_isin_country_code(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut builder = StringChunkedBuilder::new("country_code", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(isin) = isin::parse(s) {
                builder.append_value(isin.prefix());
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    });

    let out = builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_isin_security_id(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut builder = StringChunkedBuilder::new("security_id", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(isin) = isin::parse(s) {
                builder.append_value(isin.basic_code());
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    });

    let out = builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_isin_check_digit(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut builder = StringChunkedBuilder::new("check_digit", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(isin) = isin::parse(s) {
                builder.append_value(isin.check_digit().to_string());
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    });

    let out = builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_isin_is_valid(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut builder = BooleanChunkedBuilder::new("isin_valid", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            builder.append_value(isin::validate(s));
        } else {
            builder.append_value(false);
        }
    });

    let out = builder.finish();
    Ok(out.into_series())
}
