use crate::csv::csv_data::{CsvStream, CsvType, reset_stream};
use crate::csv::inference::ColumnInference;
use crate::qsv::{csv_stream_from_mime_type, Options};
use log::debug;
use stats::{OnlineStats, Frequencies, MinMax};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

#[derive(Copy, Clone)]
 enum MinValue{
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

pub struct Statistics{
    pub column: String, stats: Option<OnlineStats>,   top_10: Option<Vec<String>>, min: Option<MinValue>, max: Option<MinValue>
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
        if let Some(ref top_10) = self.top_10 {
            print_frequencies(top_10, f)?;
        }
        Ok(())

    }
}

fn print_statistics(stats: & OnlineStats, f: &mut Formatter) -> std::fmt::Result {
    writeln!(f, "\tMean: {:.5}", stats.mean())?;
    writeln!(f, "\tStdev: {:.5}",  stats.stddev())
}


fn print_frequencies(top_10: &Vec<String>, f: &mut Formatter) -> std::fmt::Result {
    writeln!(f, "\tTop ten most common occurrences:")?;
    for ten in top_10 {
        writeln!(f, "\t{}", ten)?
    }
    Ok(())
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
                    let mut minmax = MinMax::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: i64| {
                        stats.add(element);
                        freqs.add(element);
                        minmax.add(element);
                    })?;
                    let min = minmax.min().map(|inner| MinValue::Int(inner.clone()));
                    let max = minmax.max().map(|inner| MinValue::Int(inner.clone()));

                    let result = Statistics{column: key.clone(), stats: Some(stats), top_10: Some(format_top_10(freqs)), min, max };
                    vec.push(result)
                }
                CsvType::Float => {
                    let mut stats = OnlineStats::new();
                    let mut minmax = MinMax::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: f64| {
                        stats.add(element);
                        minmax.add(element);
                    })?;

                    let min = minmax.min().map(|inner| MinValue::Float(inner.clone()));
                    let max = minmax.max().map(|inner| MinValue::Float(inner.clone()));
                    let result = Statistics{column: key.clone(), stats: Some(stats), top_10: None, min, max};
                    vec.push(result)
                }

                CsvType::String => {
                    let mut freqs = Frequencies::new();
                    compute_statistics_closure(csv_stream, &inference, key, |element: String| {
                        freqs.add(element);
                    })?;
                    let result = Statistics{ column: key.clone(), top_10: Some(format_top_10(freqs)), stats: None, min: None, max: None};
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
    Ok((inference, csv))
}
