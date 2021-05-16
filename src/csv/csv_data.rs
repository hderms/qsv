use csv::{Reader, StringRecord, Trim, Position};
use log::debug;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;

#[derive(PartialEq, Debug)]
pub enum CsvWrapper {
    Integer(i64),
    Float(f64),
    String(String),
}
impl CsvWrapper {
    pub fn get_type(&self) -> CsvType {
        match self {
            CsvWrapper::Integer(_) => CsvType::Integer,
            CsvWrapper::Float(_) => CsvType::Float,
            CsvWrapper::String(_) => CsvType::String,
        }
    }
}

#[derive(Debug, Eq, Hash, Clone, Copy, PartialEq)]
pub enum CsvType {
    Integer,
    Float,
    String,
}
impl Display for CsvType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CsvType::Integer => f.write_str("integer"),
            CsvType::Float => f.write_str("float"),
            CsvType::String => f.write_str("text"),
        }
    }
}
///A representation of CSV data loaded into memory
pub struct CsvData {
    pub records: Vec<StringRecord>,
    pub headers: StringRecord,
    pub filename: String,
}
impl CsvData {
    ///Load CSVData from a filename
    pub fn from_filename(
        filename: &str,
        delimiter: char,
        trim: bool,
    ) -> Result<CsvData, Box<dyn Error>> {
        debug!("Trying to load CSV from filename {}", filename);
        let file = File::open(filename)?;
        CsvData::from_reader(file, filename, delimiter, trim)
    }

    pub fn from_reader<R: std::io::Read>(
        reader: R,
        filename: &str,
        delimiter: char,
        trim: bool,
    ) -> Result<CsvData, Box<dyn Error>> {
        let mut records = Vec::with_capacity(10000);
        let trim = if trim { Trim::All } else { Trim::None };
        let mut rdr = csv::ReaderBuilder::new()
            .buffer_capacity(16 * (1 << 10))
            .delimiter(delimiter as u8)
            .trim(trim)
            .from_reader(reader);

        for result in rdr.records() {
            let record = result?;
            records.push(record);
        }
        let headers = rdr.headers()?;
        debug!("Filename has headers: {:?}", headers);
        Ok(CsvData {
            records,
            headers: headers.to_owned(),
            filename: String::from(filename),
        })
    }
}
pub struct CsvStream<R: std::io::Read + std::io::Seek> {
    pub headers: StringRecord,
    pub filename: String,
    pub stream: Reader<R>,
}
impl<R: std::io::Read + std::io::Seek> CsvStream<R> {
    pub fn from_reader(
        reader: R,
        filename: &str,
        delimiter: char,
        trim: bool,
    ) -> Result<CsvStream<R>, Box<dyn Error>> {
        let trim = if trim { Trim::All } else { Trim::None };
        let mut stream: Reader<R> = csv::ReaderBuilder::new()
            .buffer_capacity(16 * (1 << 10))
            .delimiter(delimiter as u8)
            .trim(trim)
            .from_reader(reader);

        let headers = stream.headers()?;
        let csv_stream: CsvStream<R> = CsvStream {
            headers: headers.clone(),
            filename: String::from(filename),
            stream,
        };
        Ok(csv_stream)
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
        assert_eq!(
            csv.records,
            vec!(
                StringRecord::from(vec!("Bartender", "32")),
                StringRecord::from(vec!("Construction Worker", "25")),
            )
        )
    }

    #[test]
    fn it_can_load_file_with_trim() {
        let csv = CsvData::from_filename(
            "testdata/occupations_with_extraneous_spaces.csv",
            DELIMITER,
            true,
        )
        .unwrap();
        assert_eq!(
            csv.records,
            vec!(
                StringRecord::from(vec!("Bartender", "18")),
                StringRecord::from(vec!("Construction Worker", "18")),
            )
        )
    }
}
pub fn reset_stream(csv_stream: &mut CsvStream<File>) -> Result<(), Box<dyn Error>> {
    let mut beginning = Position::new();
    beginning.set_line(1);
    csv_stream.stream.seek(beginning.clone())?;
    csv_stream.stream.records().next();
    Ok(())

}
