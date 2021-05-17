use crate::csv::csv_data::{reset_stream, CsvStream, CsvType};
use crate::csv::inference::ColumnInference;
use crate::qsv::{csv_stream_from_mime_type, Options};
use log::debug;
use stats::{Frequencies, MinMax, OnlineStats};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::Hash;
use std::path::Path;
use std::str::FromStr;

#[derive(Copy, Clone)]
enum MinValue {
    Float(f64),
    Int(i64),
}
impl Display for MinValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MinValue::Float(v) => {
                write!(f, "{:.5}", v)
            }
            MinValue::Int(v) => {
                write!(f, "{}", v)
            }
        }
    }
}

pub struct Statistics {
    pub column: String,
    stats: Option<OnlineStats>,
    top_10: Option<Vec<String>>,
    min: Option<MinValue>,
    max: Option<MinValue>,
    cardinality: Option<u64>,
}
impl Display for Statistics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(ref stats) = self.stats {
            print_statistics(stats, f)?;
        }
        if let Some(min) = self.min {
            writeln!(f, "\tMin: {}", min)?;
        }
        if let Some(max) = self.max {
            writeln!(f, "\tMax: {}", max)?;
        }
        if let Some(ref cardinality) = self.cardinality {
            writeln!(f, "\tUnique: {}", cardinality)?;
        }
        if let Some(ref top_10) = self.top_10 {
            print_frequencies(top_10, f)?;
        }

        Ok(())
    }
}

fn print_statistics(stats: &OnlineStats, f: &mut Formatter) -> std::fmt::Result {
    writeln!(f, "\tMean: {:.5}", stats.mean())?;
    writeln!(f, "\tStdev: {:.5}", stats.stddev())
}

fn print_frequencies(top_10: &[String], f: &mut Formatter) -> std::fmt::Result {
    writeln!(f, "\tTop ten most common occurrences:")?;
    for ten in top_10 {
        writeln!(f, "\t{}", ten)?
    }
    Ok(())
}

///Calculates some statistics from the CSV
/// Depending on what type the column has, we can calculate different values such as:
/// mean, stddev, count unique, top 10 most frequent values
pub fn execute_statistics(
    filename: &str,
    options: &Options,
) -> Result<Vec<Statistics>, Box<dyn Error>> {
    if let Ok((inference, ref mut csv_stream)) = maybe_load_stats(filename, options) {
        let mut vec = Vec::with_capacity(10);
        for (key, value) in &inference.columns_to_types {
            reset_stream(csv_stream).unwrap();
            match value {
                CsvType::Integer => {
                    let mut statistics = OnlineStats::new();
                    let mut frequencies = Frequencies::new();
                    let mut minmax = MinMax::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: i64| {
                        statistics.add(element);
                        frequencies.add(element);
                        minmax.add(element);
                    })?;
                    let min = minmax.min().map(|inner| MinValue::Int(*inner));
                    let max = minmax.max().map(|inner| MinValue::Int(*inner));
                    let cardinality = Some(frequencies.cardinality());

                    let result = Statistics {
                        column: key.clone(),
                        stats: Some(statistics),
                        top_10: Some(format_top_10(frequencies)),
                        min,
                        max,
                        cardinality,
                    };
                    vec.push(result)
                }
                CsvType::Float => {
                    let mut statistics = OnlineStats::new();
                    let mut minmax = MinMax::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: f64| {
                        statistics.add(element);
                        minmax.add(element);
                    })?;

                    let min = minmax.min().map(|inner| MinValue::Float(*inner));
                    let max = minmax.max().map(|inner| MinValue::Float(*inner));
                    let result = Statistics {
                        column: key.clone(),
                        stats: Some(statistics),
                        top_10: None,
                        min,
                        max,
                        cardinality: None,
                    };
                    vec.push(result)
                }

                CsvType::String => {
                    let mut frequencies = Frequencies::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: String| {
                        frequencies.add(element);
                    })?;
                    let cardinality = Some(frequencies.cardinality());
                    let result = Statistics {
                        column: key.clone(),
                        top_10: Some(format_top_10(frequencies)),
                        stats: None,
                        min: None,
                        max: None,
                        cardinality,
                    };
                    vec.push(result)
                }
            }
        }
        debug!("Filename was able to be loaded: {}", filename);
        Ok(vec)
    } else {
        debug!("Filename could not be loaded: {}", filename);
        Err("failed to load file".into())
    }
}
fn format_top_10<T: Eq + Hash + Display>(freqs: Frequencies<T>) -> Vec<String> {
    freqs
        .most_frequent()
        .iter()
        .take(10)
        .map(|(element, count)| format!("element: {}, count: {}", element, count))
        .collect()
}

///This allows us to parse a given type from a string value'd CSV cell
///provided we know what type that cell should be
///we do this for every cell in the column, calling the closure `process`  on it
fn compute_statistics_closure<Parse: FromStr, F>(
    csv_stream: &mut CsvStream<File>,
    inference: &ColumnInference,
    key: &str,
    mut process: F,
) -> Result<(), Box<dyn Error>>
where
    F: FnMut(Parse),
{
    for record in csv_stream.stream.records() {
        let record = record?;
        let index = inference.columns_to_indexes.get(key).unwrap();
        let try_parse: Parse = record
            .get(*index)
            .unwrap()
            .parse()
            .map_err(|_err| {
                let error: Box<dyn Error> = "Error parsing type".into();
                error
            })
            .unwrap();
        process(try_parse);
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
        ColumnInference::default_inference(&csv.headers)
    } else {
        ColumnInference::from_stream(&mut csv)?
    };
    Ok((inference, csv))
}
