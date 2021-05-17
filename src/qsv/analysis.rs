use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use log::debug;

use crate::csv::inference::{ColumnInference, ColumnInferences};
use crate::parser::collector::Collector;
use crate::parser::Parser;
use crate::qsv::{csv_data_from_mime_type, Options};

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
        ColumnInference::default_inference(&csv.headers)
    } else {
        ColumnInference::from_csv(&csv)
    };
    Ok(Some(inference))
}
