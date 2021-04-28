use csv::StringRecord;
use std::error::Error;
use log::debug;
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
///A representation of CSV data loaded into memory
pub struct CsvData {
    pub records: Vec<StringRecord>,
    pub headers: StringRecord,
    pub filename: String
}
impl CsvData {
    ///Load CSVData from a filename
    pub fn from_filename(filename: &str, delimiter: char) -> Result<CsvData, Box<dyn Error>> {
        debug!("Trying to load CSV from filename {}", filename);
        let mut records = Vec::with_capacity(10000);
        let mut rdr = csv::ReaderBuilder::new()
            .buffer_capacity(16 * (1 << 10))
            .delimiter(delimiter as u8)
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
    #[test]
    fn it_can_load_file() {
        let csv = CsvData::from_filename("testdata/test.csv").unwrap();
        assert_eq!(csv.records, vec!(StringRecord::from(vec!("bar", "13"))))
    }
}
