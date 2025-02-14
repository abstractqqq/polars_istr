use cusip::CUSIP;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

fn cusip_full_output(_: &[Field]) -> PolarsResult<Field> {
    let cc = Field::new("country_code".into(), DataType::String);
    let issuer = Field::new("issuer".into(), DataType::String);
    let issue = Field::new("issue".into(), DataType::String);
    let cd = Field::new("check_digit".into(), DataType::String);

    let v: Vec<Field> = vec![cc, issuer, issue, cd];
    Ok(Field::new("".into(), DataType::Struct(v)))
}

#[polars_expr(output_type_func=cusip_full_output)]
fn pl_cusip_full(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut cc_builder = StringChunkedBuilder::new("country_code".into(), ca.len());
    let mut ir_builder = StringChunkedBuilder::new("issuer".into(), ca.len());
    let mut is_builder = StringChunkedBuilder::new("issue".into(), ca.len());
    let mut cd_builder = StringChunkedBuilder::new("check_digit".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                if let Some(cins) = cusip.as_cins() {
                    cc_builder.append_value(cins.country_code().to_string());
                    ir_builder.append_value(cusip.issuer_num());
                    is_builder.append_value(cusip.issue_num());
                    cd_builder.append_value(cusip.check_digit().to_string());
                } else {
                    cc_builder.append_null();
                    ir_builder.append_value(cusip.issuer_num());
                    is_builder.append_value(cusip.issue_num());
                    cd_builder.append_value(cusip.check_digit().to_string());
                }
            } else {
                cc_builder.append_null();
                ir_builder.append_null();
                is_builder.append_null();
                cd_builder.append_null();
            }
        } else {
            cc_builder.append_null();
            ir_builder.append_null();
            is_builder.append_null();
            cd_builder.append_null();
        }
    });
    let cc = cc_builder.finish().into_series().into_column();
    let ir = ir_builder.finish().into_series().into_column();
    let is = is_builder.finish().into_series().into_column();
    let cd = cd_builder.finish().into_series().into_column();

    let out = StructChunked::from_columns("cusip".into(), cc.len(), &[cc, ir, is, cd])?;
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_cusip_issue_num(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut s_builder = StringChunkedBuilder::new("issue_num".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                s_builder.append_value(cusip.issue_num());
            } else {
                s_builder.append_null();
            }
        } else {
            s_builder.append_null();
        }
    });

    let out = s_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_cusip_issuer_num(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut s_builder = StringChunkedBuilder::new("issuer_num".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                if let Some(cins) = cusip.as_cins() {
                    s_builder.append_value(cins.issuer_num());
                } else {
                    s_builder.append_value(cusip.issuer_num());
                }
            } else {
                s_builder.append_null();
            }
        } else {
            s_builder.append_null();
        }
    });

    let out = s_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_cusip_country_code(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut s_builder = StringChunkedBuilder::new("country_code".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                if let Some(cins) = cusip.as_cins() {
                    s_builder.append_value(cins.country_code().to_string());
                } else {
                    s_builder.append_null();
                }
            } else {
                s_builder.append_null();
            }
        } else {
            s_builder.append_null();
        }
    });

    let out = s_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_cusip_check_digit(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut s_builder = StringChunkedBuilder::new("check_digit".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                s_builder.append_value(cusip.check_digit().to_string());
            } else {
                s_builder.append_null();
            }
        } else {
            s_builder.append_null();
        }
    });

    let out = s_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_cusip_payload(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut s_builder = StringChunkedBuilder::new("check_digit".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                s_builder.append_value(cusip.payload());
            } else {
                s_builder.append_null();
            }
        } else {
            s_builder.append_null();
        }
    });

    let out = s_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_cusip_is_private_issue(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut b_builder = BooleanChunkedBuilder::new("is_private_issue".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                b_builder.append_value(cusip.is_private_issue());
            } else {
                b_builder.append_null()
            }
        } else {
            b_builder.append_null();
        }
    });

    let out = b_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_cusip_has_private_issuer(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut b_builder = BooleanChunkedBuilder::new("has_private_issuer".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                b_builder.append_value(cusip.has_private_issuer());
            } else {
                b_builder.append_null()
            }
        } else {
            b_builder.append_null();
        }
    });

    let out = b_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_cusip_is_private_use(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut b_builder = BooleanChunkedBuilder::new("is_private_use".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                b_builder.append_value(cusip.is_private_use());
            } else {
                b_builder.append_null()
            }
        } else {
            b_builder.append_null();
        }
    });

    let out = b_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_cusip_is_cins(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut b_builder = BooleanChunkedBuilder::new("is_cins".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                b_builder.append_value(cusip.is_cins());
            } else {
                b_builder.append_null()
            }
        } else {
            b_builder.append_null();
        }
    });

    let out = b_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_cusip_is_cins_base(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut b_builder = BooleanChunkedBuilder::new("is_cins_base".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                if let Some(cins) = cusip.as_cins() {
                    b_builder.append_value(cins.is_base());
                } else {
                    b_builder.append_null();
                }
            } else {
                b_builder.append_null();
            }
        } else {
            b_builder.append_null();
        }
    });

    let out = b_builder.finish();
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn pl_cusip_is_cins_extended(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let mut b_builder = BooleanChunkedBuilder::new("is_cins_extended".into(), ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(cusip) = CUSIP::parse(s) {
                if let Some(cins) = cusip.as_cins() {
                    b_builder.append_value(cins.is_extended());
                } else {
                    b_builder.append_null();
                }
            } else {
                b_builder.append_null()
            }
        } else {
            b_builder.append_null();
        }
    });

    let out = b_builder.finish();
    Ok(out.into_series())
}
