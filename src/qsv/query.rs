use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use log::debug;
use uuid::Uuid;

use crate::csv::inference::ColumnInference;
use crate::db::utils::to_table_parameters;
use crate::db::{Db, Header, Rows};
use crate::parser::collector::Collector;
use crate::parser::rewriter::Rewriter;
use crate::parser::Parser;
use crate::qsv::util::{remove_extension, sanitize};
use crate::qsv::{csv_data_from_mime_type, Options};

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
