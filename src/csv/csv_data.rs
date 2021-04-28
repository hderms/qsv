use csv::{StringRecord, Trim};
use std::error::Error;
use log::debug;
use std::fmt::{Display, Formatter};

#[derive(Eq, PartialEq, Debug)]
pub enum CsvWrapper {
    Numeric(i64),
    String(String),
}
impl CsvWrapper {
    pub fn get_type(&self) -> CsvType {
        match self {
            CsvWrapper::Numeric(_) => CsvType::Numeric,
            CsvWrapper::String(_) => CsvType::String,
        }
    }
}

#[derive(Debug, Eq, Hash, Clone, Copy, PartialEq)]
pub enum CsvType {
    Numeric,
    String,
}
impl Display for CsvType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CsvType::Numeric => f.write_str("numeric"),
            CsvType::String => f.write_str("text")
        }
    }
}
///A representation of CSV data loaded into memory
pub struct CsvData {
    pub records: Vec<StringRecord>,
    pub headers: StringRecord,
    pub filename: String
}
impl CsvData {
    ///Load CSVData from a filename
    pub fn from_filename(filename: &str, delimiter: char, trim: bool) -> Result<CsvData, Box<dyn Error>> {
        debug!("Trying to load CSV from filename {}", filename);
        let mut records = Vec::with_capacity(10000);
        let trim = if trim {
            Trim::All
        } else {
            Trim::None
        };
        let mut rdr = csv::ReaderBuilder::new()
            .buffer_capacity(16 * (1 << 10))
            .delimiter(delimiter as u8)
            .trim(trim)
            .from_path(filename)?;

        for result in rdr.records() {
            let record = result?;
            records.push(record);
        }
        let headers = rdr.headers()?;
        debug!("Filename has headers: {:?}", headers);
        Ok(CsvData {
            records,
            headers: headers.to_owned(),
            filename: String::from(filename)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const DELIMITER: char = ',';
    #[test]
    fn it_can_load_file() {
        let csv = CsvData::from_filename("testdata/test.csv", DELIMITER, false).unwrap();
        assert_eq!(csv.records, vec!(StringRecord::from(vec!("bar", "13"))))
    }

    #[test]
    fn it_can_load_file_with_alternate_delimiter() {
        let csv = CsvData::from_filename("testdata/slash_as_separator.csv", '/', true).unwrap();
        assert_eq!(csv.records, vec!(
            StringRecord::from(vec!("Bartender", "32")),
            StringRecord::from(vec!("Construction Worker", "25")),
        ))
    }

    #[test]
    fn it_can_load_file_with_trim() {
        let csv = CsvData::from_filename("testdata/occupations_with_extraneous_spaces.csv", DELIMITER, true).unwrap();
        assert_eq!(csv.records, vec!(
            StringRecord::from(vec!("Bartender", "18")),
            StringRecord::from(vec!("Construction Worker", "18")),
        ))
    }
}
