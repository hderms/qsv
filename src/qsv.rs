use crate::csv::csv_data::{CsvData, CsvStream, CsvType};
use crate::csv::inference::{ColumnInference, ColumnInferences};
use crate::db::utils::to_table_parameters;
use crate::db::{Db, Header, Rows};
use crate::parser::collector::Collector;
use crate::parser::rewriter::Rewriter;
use crate::parser::Parser;
use flate2::read::GzDecoder;
use log::{debug, error};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{Write, Read,  Seek};
use std::path::Path;
use uuid::Uuid;
use csv::Position;

pub struct Options {
    pub delimiter: char,
    pub trim: bool,
    pub textonly: bool,
}

///Executes a query, possibly returning Rows
pub fn execute_query(query: &str, options: &Options) -> Result<(Header, Rows), Box<dyn Error>> {
    let mut collector = Collector::new();

    let ast = Parser::parse_sql(query)?;
    let mut db = Db::open_in_memory()?;
    if ast.len() != 1 {
        return Err("Expected exactly one SQL statement in query input".into());
    }
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

///Executes a query, possibly returning Rows
pub fn execute_statistics(
    filename: &String,
    options: &Options,
) -> Result<ColumnInferences, Box<dyn Error>> {
    let mut hashmap: HashMap<String, ColumnInference> = HashMap::new();
    if let Ok((inference, ref mut csv_stream)) = maybe_load_stats(filename, options) {
        for (key, value) in inference.columns_to_types {
            match value {
                CsvType::Integer => {
                    let mut beginning = Position::new();
                    beginning.set_line(1);
                    csv_stream.stream.seek(beginning.clone())?;
                    let mut amount = 0;
                    csv_stream.stream.records().next();
                    for record in csv_stream.stream.records() {
                        let record = record?;
                        let index = inference.columns_to_indexes.get(&key).unwrap();
                        let parsed: usize = record.get(*index).unwrap().parse().unwrap();
                        amount += parsed;

                    }
                    println!("total: {}", amount);

                }
                CsvType::Float => {}
                CsvType::String => {}
            }

        }
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

fn maybe_load_stats(
    filename: &str,
    options: &Options,
) -> Result<(ColumnInference, CsvStream<File>), Box<dyn Error>> {
    let path = Path::new(filename);
    if !path.exists() {
        return Err("failed to find path".into())
    }
    let mime_type = tree_magic::from_filepath(path);
    let mut csv = csv_stream_from_mime_type(filename, mime_type.as_str(), options)?;
    let inference = if options.textonly {
        ColumnInference::default_inference_csv_stream(&csv)
    } else {
        ColumnInference::from_stream(&mut csv)?
    };
    println!("{}", inference);
    Ok((inference, csv))
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

    if !files_to_tables.values().any(|s| s == table_name) {
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
    } else {
        debug!(
            "Table already exists {} for filename {}, not creating it or inserting records",
            table_name, filename
        );
    }
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
pub fn write_to_stdout_with_header(results: Rows, header: &[String]) -> Result<(), Box<dyn Error>> {
    let header = header.join(",");
    println!("{}", header);
    write_to_stdout(results)
}

fn sanitize(str: Option<String>) -> Option<String> {
    match str {
        Some(s) => Some(s.replace(" ", "_")),
        None => None,
    }
}
