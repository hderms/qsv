use crate::csv::csv_data::{CsvStream, CsvType, reset_stream};
use crate::csv::inference::ColumnInference;
use crate::qsv::{csv_stream_from_mime_type, Options};
use log::debug;
use stats::{OnlineStats, Frequencies};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

pub enum Statistics{
    StatsAndFrequencies{ column: String, stats: OnlineStats,   top_10: Vec<String>},
    Stats{column: String,  stats: OnlineStats},
    Frequencies{ column: String, top_10: Vec<String>},
}
impl Display for Statistics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Statistics::StatsAndFrequencies { column, stats, top_10 } => {
                writeln!(f, "Column: {}", column)?;
                writeln!(f, "Mean: {}, stddev: {}", stats.mean(), stats.stddev())?;
                writeln!(f, "Top ten most common occurrences:")?;
                for ten in top_10 {
                    writeln!(f, "\t{}", ten)?
                }

            }
            Statistics::Stats{column, stats} => {
                writeln!(f, "Column: {}", column)?;
                writeln!(f, "Mean: {}, stddev: {}", stats.mean(), stats.stddev())?
            }
            Statistics::Frequencies { column, top_10 } => {
                writeln!(f, "Column: {}", column)?;
                writeln!(f, "Top ten most common occurrences:")?;
                for ten in top_10 {
                    writeln!(f, "\t{}", ten)?
                }
            }
        }
        Ok(())

    }
}


///Executes a query, possibly returning Rows
pub fn execute_statistics(filename: &str, options: &Options) -> Result<Vec<Statistics>, Box<dyn Error>> {
    if let Ok((inference, ref mut csv_stream)) = maybe_load_stats(filename, options) {
        let mut vec = Vec::with_capacity(10);
        for (key, value) in &inference.columns_to_types {
             reset_stream(csv_stream).unwrap();
            match value {
                CsvType::Integer => {
                    let mut stats = OnlineStats::new();
                    let mut freqs = Frequencies::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: i64| {
                        stats.add(element);
                        freqs.add(element);
                    })?;
                    let result = Statistics::StatsAndFrequencies{column: key.clone(), stats, top_10: format_top_10(freqs) };
                    vec.push(result)
                }
                CsvType::Float => {
                    let mut stats = OnlineStats::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: f64| {
                        stats.add(element);
                    })?;

                    let result = Statistics::Stats{column: key.clone(), stats};
                    vec.push(result)
                }

                CsvType::String => {
                    let mut freqs = Frequencies::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: String| {
                        freqs.add(element);
                    })?;
                    let result = Statistics::Frequencies{ column: key.clone(), top_10: format_top_10(freqs)};
                    vec.push(result)
                }
            }
        }
        debug!(
            "Filename was able to be loaded: {}",
            filename
        );
        Ok(vec)
    } else {
        debug!(
            "Filename could not be loaded: {}",
            filename
        );
        Err("failed to load file".into())
    }
}
fn format_top_10<T: Eq + Hash + Display>(freqs: Frequencies<T>) -> Vec<String> {
    freqs.most_frequent().iter().take(10).map(|(element, count)| format!("element: {}, count: {}", element, count)).collect()

}

fn compute_statistics_closure<Parse: FromStr , F>(csv_stream: &mut CsvStream<File>, inference: &ColumnInference, key: &str, mut f:   F)
    -> Result<(), Box<dyn Error>> where
    F: FnMut(Parse) -> ()

{
    for record in csv_stream.stream.records() {
        let record = record?;
        let index = inference.columns_to_indexes.get(key).unwrap();
        let try_parse: Parse = record.get(*index).unwrap().parse().map_err(|_err| {
            let error: Box<dyn Error> = "Error parsing type".into();
            error
        }).unwrap();
        f(try_parse);
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
