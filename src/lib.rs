mod cusip_parsing;
mod iban_parsing;
mod isin_parsing;
mod url_parsing;
mod utils;
use pyo3::{pymodule, types::{PyModule, PyModuleMethods}, Bound, PyResult, Python};

use pyo3_polars::PolarsAllocator;
#[global_allocator]
static ALLOC: PolarsAllocator = PolarsAllocator::new();

#[pymodule]
#[pyo3(name = "_polars_istr")]
fn _polars_istr(_py: Python<'_>, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
