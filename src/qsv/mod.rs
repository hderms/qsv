use std::error::Error;
use std::fs::File;
use std::io::Write;

use flate2::read::GzDecoder;
use log::error;

pub use analysis::execute_analysis;
pub use query::execute_query;
pub use statistics::execute_statistics;

use crate::csv::csv_data::{CsvData, CsvStream};
use crate::db::Rows;

mod analysis;
mod query;
mod statistics;
mod util;

pub struct Options {
    pub delimiter: char,
    pub trim: bool,
    pub textonly: bool,
}

fn csv_data_from_mime_type(
    filename: &str,
    mime_type: &str,
    options: &Options,
) -> Result<CsvData, Box<dyn Error>> {
    if mime_type == "application/gzip" {
        let reader = File::open(filename)?;
        let d = GzDecoder::new(reader);
        CsvData::from_reader(d, filename, options.delimiter, options.trim)
    } else if mime_type == "text/plain" {
        CsvData::from_filename(filename, options.delimiter, options.trim)
    } else {
        let error_format = format!("Unsupported MIME type {} for file {}", mime_type, filename);
        error!("{}", error_format);
        Err(error_format.into())
    }
}

trait ReadAndSeek: std::io::Read + std::io::Seek {}
fn csv_stream_from_mime_type(
    filename: &str,
    mime_type: &str,
    options: &Options,
) -> Result<CsvStream<File>, Box<dyn Error>> {
    if mime_type == "text/plain" {
        let reader = File::open(filename)?;
        CsvStream::from_reader(reader, filename, options.delimiter, options.trim)
    } else {
        let error_format = format!("Unsupported MIME type {} for file {}", mime_type, filename);
        error!("{}", error_format);
        Err(error_format.into())
    }
}

///Writes a set of rows to STDOUT
pub fn write_to_stdout(results: Rows) -> Result<(), Box<dyn Error>> {
    let stdout = std::io::stdout();
    let lock = stdout.lock();
    let mut buf = std::io::BufWriter::new(lock);
    for result in results {
        buf.write_all(result.join(",").as_str().as_bytes())?;
        buf.write_all(b"\n")?;
    }
    Ok(())
}

///Writes a set of rows to STDOUT, with the header included
pub fn write_to_stdout_with_header(results: Rows, header: &[String]) -> Result<(), Box<dyn Error>> {
    let header = header.join(",");
    println!("{}", header);
    write_to_stdout(results)
}
