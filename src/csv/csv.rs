use csv::StringRecord;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
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
pub struct Csv {
    pub records: Vec<StringRecord>,
    pub headers: StringRecord,
}
impl Csv {
    fn from_filename(filename: &str) -> Result<Csv, Box<dyn Error>> {
        let mut records = Vec::with_capacity(100);
        let file_reader = File::open(filename)?;
        let mut rdr = csv::Reader::from_reader(BufReader::new(file_reader));
        for result in rdr.records() {
            let record = result?;
            records.push(record);
        }
        let headers = rdr.headers()?;
        Ok(Csv {
            records,
            headers: headers.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_can_load_file() {
        let csv = Csv::from_filename("testdata/test.csv").unwrap();
        assert_eq!(csv.records, vec!(StringRecord::from(vec!("bar", "13"))))
    }
}
