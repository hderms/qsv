use crate::csv::csv_data::{CsvData, CsvType};
use crate::csv::inference::ColumnInference;

const INTEGER_STRING: &str = "integer";
const TEXT_STRING: &str = "text";
const FLOAT_STRING: &str = "real";
pub fn to_table_parameters(csv_data: &CsvData, column_inference: &ColumnInference) -> Vec<String> {
    let mut vec = Vec::with_capacity(csv_data.headers.len());
    for header in csv_data.headers.iter() {
        let column_type = column_inference.get_type(header.to_string()).unwrap();
        let string = match column_type {
            CsvType::Integer => {
                format!("{} {}", header, INTEGER_STRING)
            }
            CsvType::String => {
                format!("{} {}", header, TEXT_STRING)
            }

            CsvType::Float => {
                format!("{} {}", header, FLOAT_STRING)
            }
        };
        vec.push(string);
    }
    vec
}

/// repeat parameters a specific number of times for use in SQL interpolation
/// ```
/// use qsv::db::utils::repeat_vars;
/// assert_eq!(repeat_vars(3), "?,?,?");
/// ```
pub fn repeat_vars(count: usize) -> String {
    assert_ne!(count, 0);
    assert!(count <= 1000);
    let mut s = "?,".repeat(count);
    // Remove trailing comma
    s.pop();
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_repeats_variables_a_number_of_times() {
        assert_eq!(repeat_vars(1), String::from("?"));
        assert_eq!(repeat_vars(2), String::from("?,?"));
        assert_eq!(repeat_vars(3), String::from("?,?,?"));
        assert_eq!(repeat_vars(4), String::from("?,?,?,?"));
        assert_eq!(repeat_vars(5), String::from("?,?,?,?,?"));
    }

    #[test]
    #[should_panic]
    fn it_fails_on_zero() {
        repeat_vars(0);
    }
    #[test]
    #[should_panic]
    fn it_fails_above_1000() {
        repeat_vars(1001);
    }
}
