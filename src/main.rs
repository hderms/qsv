mod csv;
mod db;
mod parser;
mod qsv;

use crate::qsv::{execute_query, write_to_stdout};
use std::error::Error;
use simple_logger::SimpleLogger;
use clap::{AppSettings, Clap};
#[derive(Clap)]
#[clap(version = "0.1", author = "Dermot H. <dermot.thomas.haughey@gmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    query: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::from_env().init()?;
    let opts: Opts = Opts::parse();
    let query = opts.query;
    let results = execute_query(query.as_str())?;
    write_to_stdout(results)?;
    Ok(())
}
