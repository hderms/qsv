mod csv;
mod db;
mod parser;
mod qsv;

use crate::qsv::{execute_query, write_to_stdout};
use std::error::Error;
use simple_logger::SimpleLogger;
use clap::{AppSettings, Clap};
use crate::qsv::Options;

#[derive(Clap)]
#[clap(version = "0.1", author = "Dermot H. <dermot.thomas.haughey@gmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    query: String,
    #[clap(short, long, default_value=",")]
    delimiter: char
}

fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::from_env().init()?;
    let opts: Opts = Opts::parse();
    let query = opts.query;
    let delimiter = opts.delimiter;
    let options = Options{delimiter};
    let results = execute_query(query.as_str(), &options)?;
    write_to_stdout(results)?;
    Ok(())
}
