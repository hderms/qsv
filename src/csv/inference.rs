use crate::csv::csv::{Csv, CsvType, CsvWrapper};
use csv::StringRecord;
use itertools::Itertools;
use std::collections::HashMap;
use std::num::ParseIntError;

struct ColumnInference {
    columns_to_types: HashMap<String, CsvType>,
}

impl ColumnInference {
    fn from_csv(csv: Csv) -> ColumnInference {
        let mut columns_to_types: HashMap<String, CsvType> = HashMap::new();
        for (i, header) in csv.headers.iter().enumerate() {
            let t: Vec<CsvWrapper> = csv
                .records
                .iter()
                .map(|s| parse(s.get(i).unwrap()))
                .collect();
            let types: Vec<CsvType> = t.iter().map(|s| s.get_type()).collect();
            let unique_types: Vec<&CsvType> = types.iter().unique().collect();

            if unique_types.len() == 1 {
                columns_to_types.insert(String::from(header), unique_types[0].to_owned());
            } else {
                columns_to_types.insert(String::from(header), CsvType::String);
            }
        }
        ColumnInference { columns_to_types }
    }
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
        let inference = ColumnInference::from_csv(Csv { headers, records });
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
