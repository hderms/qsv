mod csv;
mod db;
mod parser;
mod qsv;

use crate::qsv::{execute_query, write_to_stdout};
use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let query = args[1].as_str();
    let results = execute_query(query)?;
    write_to_stdout(results)?;
    Ok(())
}
