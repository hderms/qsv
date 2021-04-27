use crate::csv::csv_data::{CsvData, CsvType, CsvWrapper};
use csv::StringRecord;
use std::collections::HashMap;
use std::num::ParseIntError;
use log::debug;

/// a record of the inferred types for columns in a CSV
pub struct ColumnInference {
    columns_to_types: HashMap<String, CsvType>,
}

impl ColumnInference {
    /// build inference from a CSV
    pub fn from_csv(csv: &CsvData) -> ColumnInference {
        let mut columns_to_types: HashMap<String, CsvType> = HashMap::new();
        for (i, header) in csv.headers.iter().enumerate() {
            let t = get_type_of_column(&csv.records, i);
            columns_to_types.insert(String::from(header), t);
        }
        debug!("Inferred columns for file {}: {:?} ", csv.filename, columns_to_types);
        ColumnInference { columns_to_types }
    }

    /// get the type of a column, referenced by its string name
    pub fn get_type(&self, s: String) -> Option<&CsvType> {
        self.columns_to_types.get(s.as_str())
    }
}
fn parse(s: &str) -> CsvWrapper {
    let is_numeric: Result<i64, ParseIntError> = s.parse();
    is_numeric
        .map(CsvWrapper::Numeric)
        .unwrap_or_else(|_| CsvWrapper::String(String::from(s)))
}

fn get_type_of_column(csv: &[StringRecord], index: usize) -> CsvType {
    let t = parse(csv[0].get(index).unwrap()).get_type();
    for record in csv.iter() {
        let parsed_type = parse(record.get(index).unwrap()).get_type();
        if parsed_type != t {
            return CsvType::String;
        }
    }
    t
}
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn it_should_parse_integers() {
        assert_eq!(parse("1"), CsvWrapper::Numeric(1));
        assert_eq!(parse("-1"), CsvWrapper::Numeric(-1));
    }
    #[test]
    fn it_should_parse_strings() {
        assert_eq!(parse("foo"), CsvWrapper::String(String::from("foo")));
        assert_eq!(parse("bar"), CsvWrapper::String(String::from("bar")));
    }
    #[test]
    fn it_should_recognize_integer_column() {
        let headers = StringRecord::from(vec!["foo", "bar"]);
        let records = vec![
            StringRecord::from(vec!["entry1", "1"]),
            StringRecord::from(vec!["entry2", "2"]),
        ];
        let filename = String::from("foo.csv");
        let inference = ColumnInference::from_csv(&CsvData { headers, records, filename });
        assert_eq!(
            inference.get_type(String::from("foo")),
            Some(&CsvType::String)
        );
        assert_eq!(
            inference.get_type(String::from("bar")),
            Some(&CsvType::Numeric)
        );
    }
}
