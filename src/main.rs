mod csv;
mod db;
mod parser;
mod qsv;

use crate::qsv::{Options, write_to_stdout_with_header};
use crate::qsv::{execute_analysis, execute_query, write_to_stdout};
use clap::{AppSettings, Clap};
use simple_logger::SimpleLogger;
use std::error::Error;

#[derive(Clap)]
#[clap(
    version = "0.1",
    author = "Dermot H. <dermot.thomas.haughey@gmail.com>"
)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(subcommand)]
    subcommand: SubCommand,
}
#[derive(Clap)]
enum SubCommand {
    Query(Query),
    Analyze(Analyze),
}

#[derive(Clap)]
struct Query {
    query: String,
    #[clap(short, long, default_value = ",")]
    delimiter: char,
    #[clap(long)]
    trim: bool,
    #[clap(long)]
    textonly: bool,
    #[clap(short, long("output-header"))]
    outputheader: bool
}

#[derive(Clap)]
struct Analyze {
    query: String,
    #[clap(short, long, default_value = ",")]
    delimiter: char,
    #[clap(long)]
    trim: bool,
}
fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::from_env().init()?;
    let opts: Opts = Opts::parse();
    match opts.subcommand {
        SubCommand::Query(subcmd) => {
            let delimiter = subcmd.delimiter;
            let trim = subcmd.trim;
            let textonly = subcmd.textonly;
            let options = Options {
                delimiter,
                trim,
                textonly,
            };
            let (header, results) = execute_query(subcmd.query.as_str(), &options)?;
            if subcmd.outputheader{

                write_to_stdout_with_header(results, &header)?;
            } else {
                write_to_stdout(results)?;
            }
        }
        SubCommand::Analyze(subcmd) => {
            let delimiter = subcmd.delimiter;
            let trim = subcmd.trim;
            let options = Options {
                delimiter,
                trim,
                textonly: false,
            };
            let results = execute_analysis(subcmd.query.as_str(), &options)?;
            println!("{}", results);
        }
    }
    Ok(())
}
