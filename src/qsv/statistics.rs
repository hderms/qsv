use crate::csv::csv_data::{CsvStream, CsvType};
use crate::csv::inference::ColumnInference;
use crate::qsv::{csv_stream_from_mime_type, Options};
use csv::Position;
use log::debug;
use stats::{Frequencies, OnlineStats};
use std::error::Error;
use std::fs::File;
use std::path::Path;

///Executes a query, possibly returning Rows
pub fn execute_statistics(filename: &str, options: &Options) -> Result<(), Box<dyn Error>> {
    if let Ok((inference, ref mut csv_stream)) = maybe_load_stats(filename, options) {
        for (key, value) in inference.columns_to_types {
            match value {
                CsvType::Integer => {
                    let mut beginning = Position::new();
                    beginning.set_line(1);
                    csv_stream.stream.seek(beginning.clone())?;

                    let mut stats = OnlineStats::new();
                    let mut freqs = Frequencies::new();
                    csv_stream.stream.records().next();
                    for record in csv_stream.stream.records() {
                        let record = record?;
                        let index = inference.columns_to_indexes.get(&key).unwrap();
                        let parsed: usize = record.get(*index).unwrap().parse().unwrap();
                        stats.add(parsed);
                        freqs.add(parsed);
                    }
                    println!("avg: {}, stddev: {}", stats.mean(), stats.stddev());
                    println!(
                        "top 10: {:?}, cardinality: {}",
                        freqs.most_frequent().iter().take(5),
                        freqs.cardinality()
                    );
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
    Ok(())
}

fn maybe_load_stats(
    filename: &str,
    options: &Options,
) -> Result<(ColumnInference, CsvStream<File>), Box<dyn Error>> {
    let path = Path::new(filename);
    if !path.exists() {
        return Err("failed to find path".into());
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
