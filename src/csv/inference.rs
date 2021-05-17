use crate::csv::csv_data::{reset_stream, CsvData, CsvStream, CsvType, CsvWrapper};
use csv::StringRecord;
use log::debug;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::io::{Read, Seek};
use std::num::{ParseFloatError, ParseIntError};

/// a record of the inferred types for columns in a CSV
#[derive(Debug)]
pub struct ColumnInference {
    pub columns_to_types: HashMap<String, CsvType>,
    pub columns_to_indexes: HashMap<String, usize>,
}
impl Display for ColumnInference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (column, inferred_type) in self.columns_to_types.iter() {
            writeln!(f, "{} -> {}", column, inferred_type)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ColumnInferences {
    hashmap: HashMap<String, ColumnInference>,
}
impl ColumnInferences {
    pub fn new(hashmap: HashMap<String, ColumnInference>) -> ColumnInferences {
        ColumnInferences { hashmap }
    }
}

impl Display for ColumnInferences {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (table_name, inference) in self.hashmap.iter() {
            writeln!(f, "{}:", table_name)?;
            for (column, inferred_type) in inference.columns_to_types.iter() {
                writeln!(f, "\t{} -> {}", column, inferred_type)?;
            }
        }
        Ok(())
    }
}

impl ColumnInference {
    /// build inference from a CSV
    pub fn from_csv(csv: &CsvData) -> ColumnInference {
        let mut columns_to_types: HashMap<String, CsvType> = HashMap::with_capacity(8);
        let mut columns_to_indexes: HashMap<String, usize> = HashMap::with_capacity(8);
        for (i, header) in csv.headers.iter().enumerate() {
            let t = get_type_of_column(&mut csv.records.iter(), i);
            columns_to_types.insert(String::from(header), t);
            columns_to_indexes.insert(String::from(header), i);
        }
        debug!(
            "Inferred columns for file {}: {:?} ",
            csv.filename, columns_to_types
        );
        ColumnInference {
            columns_to_types,
            columns_to_indexes,
        }
    }

    pub fn from_stream<A: Read + Seek>(
        csv: &mut CsvStream<A>,
    ) -> Result<ColumnInference, csv::Error> {
        let mut columns_to_types: HashMap<String, CsvType> = HashMap::with_capacity(8);
        let mut columns_to_indexes: HashMap<String, usize> = HashMap::with_capacity(8);
        let headers: Vec<String> = csv.headers.iter().map(String::from).collect();
        for (i, header) in headers.iter().enumerate() {
            reset_stream(csv).unwrap();
            let mut records = csv.stream.records();
            let t = get_type_of_column_stream(&mut records, i)?;
            columns_to_types.insert(String::from(header), t);
            columns_to_indexes.insert(String::from(header), i);
        }
        debug!(
            "Inferred columns for file {}: {:?} ",
            csv.filename, columns_to_types
        );
        Ok(ColumnInference {
            columns_to_types,
            columns_to_indexes,
        })
    }

    /// build column 'inference' with every column artificially inferred as a String
    pub fn default_inference(headers: &StringRecord) -> ColumnInference {
        let mut columns_to_types: HashMap<String, CsvType> = HashMap::new();
        let mut columns_to_indexes: HashMap<String, usize> = HashMap::new();
        for (i, header) in headers.iter().enumerate() {
            columns_to_types.insert(String::from(header), CsvType::String);
            columns_to_indexes.insert(String::from(header), i);
        }
        ColumnInference {
            columns_to_types,
            columns_to_indexes,
        }
    }

    /// get the type of a column, referenced by its string name
    pub fn get_type(&self, s: String) -> Option<&CsvType> {
        self.columns_to_types.get(s.as_str())
    }
}
fn parse(s: &str) -> CsvWrapper {
    let is_integer: Result<i64, ParseIntError> = s.parse();
    let is_float: Result<f64, ParseFloatError> = s.parse();
    let is_integer = is_integer.map(CsvWrapper::Integer);
    let is_float = is_float.map(CsvWrapper::Float);
    is_integer
        .or(is_float)
        .unwrap_or_else(|_| CsvWrapper::String(String::from(s)))
}

fn get_type_of_column<'a, I: Iterator<Item = &'a StringRecord>>(
    csv: &mut I,
    index: usize,
) -> CsvType {
    let mut distinct_types = HashSet::new();
    for record in csv {
        let parsed_type = parse(record.get(index).unwrap()).get_type();
        distinct_types.insert(parsed_type);
    }
    if distinct_types.contains(&CsvType::String) {
        CsvType::String
    } else if distinct_types.contains(&CsvType::Integer) && distinct_types.contains(&CsvType::Float)
    {
        CsvType::Float
    } else if distinct_types.len() == 1 {
        distinct_types.iter().next().unwrap().to_owned()
    } else {
        CsvType::String
    }
}

fn get_type_of_column_stream<I: Iterator<Item = csv::Result<StringRecord>>>(
    csv: &mut I,
    index: usize,
) -> csv::Result<CsvType> {
    let mut distinct_types = HashSet::with_capacity(8);
    for record in csv {
        let record = record?;
        let parsed_type = parse(record.get(index).unwrap()).get_type();
        distinct_types.insert(parsed_type);

        if distinct_types.contains(&CsvType::String) {
            return Ok(CsvType::String);
        }
    }

    let found_type = if distinct_types.contains(&CsvType::String) {
        debug!("Distinct types contains String");
        CsvType::String
    } else if distinct_types.contains(&CsvType::Integer) && distinct_types.contains(&CsvType::Float)
    {
        debug!("Distinct types contains Integer and Float");
        CsvType::Float
    } else if distinct_types.len() == 1 {
        debug!("Distinct types contains single value");
        distinct_types.iter().next().unwrap().to_owned()
    } else {
        debug!("all else");
        CsvType::String
    };
    debug!("distinct types {:?} for index {}", distinct_types, index);
    Ok(found_type)
}
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn it_should_parse_integers() {
        assert_eq!(parse("1"), CsvWrapper::Integer(1));
        assert_eq!(parse("-1"), CsvWrapper::Integer(-1));
    }
    #[test]
    fn it_should_parse_strings() {
        assert_eq!(parse("foo"), CsvWrapper::String(String::from("foo")));
        assert_eq!(parse("bar"), CsvWrapper::String(String::from("bar")));
    }

    #[test]
    fn it_should_parse_floats() {
        assert_eq!(parse("1.00000009"), CsvWrapper::Float(1.00000009f64));
    }

    #[test]
    fn it_should_recognize_integer_column() {
        let filename: String = String::from("foo.csv");
        let headers = StringRecord::from(vec!["bar"]);
        let records = vec![StringRecord::from(vec!["1"]), StringRecord::from(vec!["2"])];
        let inference = ColumnInference::from_csv(&CsvData {
            records,
            headers,
            filename,
        });
        assert_eq!(
            inference.get_type(String::from("bar")),
            Some(&CsvType::Integer)
        );
    }

    #[test]
    fn it_should_recognize_float_column() {
        let filename: String = String::from("foo.csv");
        let headers = StringRecord::from(vec!["bar"]);
        let records = vec![
            StringRecord::from(vec!["1.0"]),
            StringRecord::from(vec!["2.0"]),
        ];
        let inference = ColumnInference::from_csv(&CsvData {
            records,
            headers,
            filename,
        });
        assert_eq!(
            inference.get_type(String::from("bar")),
            Some(&CsvType::Float)
        );
    }

    #[test]
    fn it_should_classify_mixed_floats_as_float() {
        let filename: String = String::from("foo.csv");
        let headers = StringRecord::from(vec!["foo", "bar"]);
        let records = vec![
            StringRecord::from(vec!["entry1", "1"]),
            StringRecord::from(vec!["entry2", "2.0"]),
        ];
        let inference = ColumnInference::from_csv(&CsvData {
            records,
            headers,
            filename,
        });
        assert_eq!(
            inference.get_type(String::from("foo")),
            Some(&CsvType::String)
        );
        assert_eq!(
            inference.get_type(String::from("bar")),
            Some(&CsvType::Float)
        );
    }

    #[test]
    fn it_should_classify_any_column_with_string_as_string() {
        let filename: String = String::from("foo.csv");
        let headers = StringRecord::from(vec!["foo", "bar"]);
        let records = vec![
            StringRecord::from(vec!["entry1", "1"]),
            StringRecord::from(vec!["entry2", "2.0"]),
            StringRecord::from(vec!["entry3", "foobar"]),
        ];
        let inference = ColumnInference::from_csv(&CsvData {
            records,
            headers,
            filename,
        });
        assert_eq!(
            inference.get_type(String::from("foo")),
            Some(&CsvType::String)
        );
        assert_eq!(
            inference.get_type(String::from("bar")),
            Some(&CsvType::String)
        );
    }

    #[test]
    fn it_should_use_default_column_type_if_inference_disabled() {
        let headers = StringRecord::from(vec!["foo", "bar"]);
        let inference = ColumnInference::default_inference(&headers);
        assert_eq!(
            inference.get_type(String::from("foo")),
            Some(&CsvType::String)
        );
        assert_eq!(
            inference.get_type(String::from("bar")),
            Some(&CsvType::String)
        );
    }
}
