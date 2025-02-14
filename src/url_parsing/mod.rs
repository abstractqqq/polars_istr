use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use url::Url;

#[polars_expr(output_type=String)]
fn pl_url_host(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let mut builder = StringChunkedBuilder::new("host".into(), ca.len());

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
    let mut builder = StringChunkedBuilder::new("domain".into(), ca.len());

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
    let mut builder = StringChunkedBuilder::new("fragment".into(), ca.len());

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
    let mut builder = StringChunkedBuilder::new("path".into(), ca.len());

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
    let mut builder = StringChunkedBuilder::new("query".into(), ca.len());

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
    let mut builder = BooleanChunkedBuilder::new("is_special".into(), ca.len());

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
    let out: BooleanChunked = ca.apply_nonnull_values_generic(DataType::Boolean, |s| s.parse::<Url>().is_ok());
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn pl_url_check(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;

    let out = ca.apply_values(|s| 
        match Url::parse(s) {
            Ok(_) => "ok".into(),
            Err(e) => match e {
                url::ParseError::EmptyHost => "empty host".into(),
                url::ParseError::IdnaError => "invalid international domain name".into(),
                url::ParseError::InvalidPort => "invalid port number".into(),
                url::ParseError::InvalidIpv4Address => "invalid IPv4 address".into(),
                url::ParseError::InvalidIpv6Address => "invalid IPv6 address".into(),
                url::ParseError::InvalidDomainCharacter => "invalid domain character".into(),
                url::ParseError::RelativeUrlWithoutBase => "relative URL without a base".into(),
                url::ParseError::RelativeUrlWithCannotBeABaseBase => "relative URL with a cannot-be-a-base base".into(),
                url::ParseError::SetHostOnCannotBeABaseUrl => "a cannot-be-a-base URL doesn’t have a host to set".into(),
                url::ParseError::Overflow => "URLs more than 4 GB are not supported".into(),
                _ => "unknown error".into()
            },
        }
    );
    Ok(out.into_series())
}
