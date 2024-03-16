mod cusip_parsing;
mod iban_parsing;
mod isin_parsing;
mod url_parsing;
mod utils;
use pyo3::{pymodule, types::PyModule, PyResult, Python};

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;

#[pymodule]
#[pyo3(name = "_polars_istr")]
fn _polars_istr(_py: Python<'_>, _m: &PyModule) -> PyResult<()> {
    Ok(())
}
