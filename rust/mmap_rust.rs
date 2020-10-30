use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn mmap(a: usize, b: usize) -> PyResult<int> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn mmap_rust(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(mmap, m)?)?;

    Ok(())
}