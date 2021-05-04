use crate::csv::csv_data::CsvData;
use crate::csv::inference::{ColumnInference, ColumnInferences};
use crate::db::utils::to_table_parameters;
use crate::db::Db;
use crate::parser::collector::Collector;
use crate::parser::rewriter::Rewriter;
use crate::parser::Parser;
use flate2::read::GzDecoder;
use log::{debug, error};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

type Rows = Vec<Vec<String>>;
pub struct Options {
    pub delimiter: char,
    pub trim: bool,
    pub textonly: bool,
}

///Executes a query, possibly returning Rows
pub fn execute_query(query: &str, options: &Options) -> Result<(Vec<String>, Rows), Box<dyn Error>> {
    let mut collector = Collector::new();

    let ast = Parser::parse_sql(query)?;
    let mut db = Db::open_in_memory()?;
    let statement = &ast[0];

    collector.collect(statement); //TODO: should we handle multiple SQL statements later?
    let mut files_to_tables = HashMap::new();
    for filename in collector.table_identifiers.iter() {
        let maybe_load = maybe_load_file(&mut files_to_tables, filename, &mut db, options);
        match maybe_load {
            Ok(Some(())) => {
                debug!(
                    "Potential filename from SQL was able to be loaded: {}",
                    filename
                );
            }
            Ok(None) => {
                debug!(
                    "Identifier in SQL could not be loaded as file, as it didn't exist: {}",
                    filename
                );
            }
            Err(e) => return Err(e),
        }
    }
    let rewritten = Rewriter::new(files_to_tables);
    let mut to_rewrite = statement.clone();
    rewritten.rewrite(&mut to_rewrite);
    debug!("Rewritten query: {}", to_rewrite.to_string());
    db.select_statement(to_rewrite.to_string().as_str())
}

///Executes a query, possibly returning Rows
pub fn execute_analysis(
    query: &str,
    options: &Options,
) -> Result<ColumnInferences, Box<dyn Error>> {
    let mut collector = Collector::new();
    let ast = Parser::parse_sql(query)?;
    let statement = &ast[0];

    collector.collect(statement); //TODO: should we handle multiple SQL statements later?
    let mut hashmap: HashMap<String, ColumnInference> = HashMap::new();
    for filename in collector.table_identifiers.iter() {
        if let Ok(Some(inference)) = maybe_load_analysis(filename, options) {
            hashmap.insert(filename.clone(), inference);
            debug!(
                "Potential filename from SQL was able to be loaded: {}",
                filename
            );
        } else {
            debug!(
                "Identifier in SQL could not be loaded as file: {}",
                filename
            );
        }
    }
    Ok(ColumnInferences::new(hashmap))
}

fn maybe_load_analysis(
    filename: &str,
    options: &Options,
) -> Result<Option<ColumnInference>, Box<dyn Error>> {
    let path = Path::new(filename);
    if !path.exists() {
        return Ok(None);
    }
    let mime_type = tree_magic::from_filepath(path);
    let csv = csv_data_from_mime_type(filename, mime_type.as_str(), options)?;
    let inference = if options.textonly {
        ColumnInference::default_inference(&csv)
    } else {
        ColumnInference::from_csv(&csv)
    };
    Ok(Some(inference))
}
fn maybe_load_file(
    files_to_tables: &mut HashMap<String, String>,
    filename: &str,
    db: &mut Db,
    options: &Options,
) -> Result<Option<()>, Box<dyn Error>> {
    let path = Path::new(filename);
    if !path.exists() {
        return Ok(None);
    }
    let mime_type = tree_magic::from_filepath(path);
    debug!("File '{}' has MIME type: '{}'", filename, mime_type);
    let csv = csv_data_from_mime_type(filename, mime_type.as_str(), options)?;
    let path = Path::new(filename);
    debug!(
        "Attempting to load identifier from SQL as file: {}",
        filename
    );
    let without_extension = remove_extension(path);
    let table_name = sanitize(without_extension)
        .unwrap_or_else(|| String::from("t") + &Uuid::new_v4().as_u128().to_string());
    let inference = if options.textonly {
        ColumnInference::default_inference(&csv)
    } else {
        ColumnInference::from_csv(&csv)
    };
    let table_parameters = to_table_parameters(&csv, &inference);
    let table_parameters: Vec<&str> = table_parameters.iter().map(|s| s.as_str()).collect();
    let table_name = table_name.as_str();
    debug!(
        "Attempting to create table {} for filename {}",
        table_name, filename
    );
    db.create_table(table_name, &table_parameters)?;
    let headers: Vec<&str> = csv.headers.iter().collect();
    let records: Vec<Vec<&str>> = csv.records.iter().map(|r| r.iter().collect()).collect();
    debug!("Inserting {} rows into {}", records.len(), table_name);
    db.insert(table_name, &headers, records);
    files_to_tables.insert(filename.to_string(), String::from(table_name));
    Ok(Some(()))
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

fn remove_extension(p0: &Path) -> Option<String> {
    let file_name = p0.file_name()?;
    let file_str = file_name.to_str()?;
    let mut split = file_str.split('.');
    if let Some(str) = split.next() {
        Some(String::from(str))
    } else {
        None
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
pub fn write_to_stdout_with_header(results: Rows, header: &Vec<String>) -> Result<(), Box<dyn Error>> {
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();
    let header = header.join(",");
    lock.write(header.as_bytes())?;
    lock.write(&['\n' as u8])?;
    write_to_stdout(results)
}

fn sanitize(str: Option<String>) -> Option<String> {
    match str {
        Some(s) => Some(s.replace(" ", "_")),
        None => None,
    }
}
