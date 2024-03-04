use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::fmt::Write;
use url::Url;

#[polars_expr(output_type=String)]
fn pl_url_host(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut builder = StringChunkedBuilder::new("host", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(u) = Url::parse(s) {
                builder.append_option(u.host_str())
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
fn pl_url_domain(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut builder = StringChunkedBuilder::new("domain", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(u) = Url::parse(s) {
                builder.append_option(u.domain())
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
fn pl_url_fragment(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut builder = StringChunkedBuilder::new("fragment", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(u) = Url::parse(s) {
                builder.append_option(u.fragment())
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
fn pl_url_path(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut builder = StringChunkedBuilder::new("path", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(u) = Url::parse(s) {
                builder.append_value(u.path())
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
fn pl_url_query(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut builder = StringChunkedBuilder::new("query", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(u) = Url::parse(s) {
                builder.append_option(u.query())
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
fn pl_url_is_special(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut builder = BooleanChunkedBuilder::new("is_special", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(s) = op_s {
            if let Ok(u) = Url::parse(s) {
                builder.append_value(u.is_special());
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
fn pl_url_is_valid(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: BooleanChunked = ca.apply_values_generic(|s| s.parse::<Url>().is_ok());
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_url_check(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out = ca.apply_to_buffer(|s, buf| {
        let ss = match Url::parse(s) {
            Ok(_) => "ok".to_string(),
            Err(e) => e.to_string(),
        };
        write!(buf, "{}", ss).unwrap()
    });
    Ok(out.into_series())
}
