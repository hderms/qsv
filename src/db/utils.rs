use crate::csv::csv_data::{CsvData, CsvType};
use crate::csv::inference::ColumnInference;
use format_sql_query::{Column, Table};

const INTEGER_STRING: &str = "integer";
const TEXT_STRING: &str = "text";
const FLOAT_STRING: &str = "real";
pub fn to_table_parameters(csv_data: &CsvData, column_inference: &ColumnInference) -> Vec<String> {
    let mut vec = Vec::with_capacity(csv_data.headers.len());
    for header in csv_data.headers.iter() {
        let column_type = column_inference.get_type(header.to_string()).unwrap();
        let table_name = escape_table(header);
        let string = match column_type {
            CsvType::Integer => {
                format!("{} {}", table_name, INTEGER_STRING)
            }
            CsvType::String => {
                format!("{} {}", table_name, TEXT_STRING)
            }

            CsvType::Float => {
                format!("{} {}", table_name, FLOAT_STRING)
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

pub fn escape_fields(fields: &[&str]) -> Vec<String> {
    fields
        .iter()
        .map(|&field| format!("{}", Column(field.to_string().as_str().into())))
        .collect()
}
pub fn escape_table(table_name: &str) -> String {
    format!("{}", Table(table_name.to_string().as_str().into()))
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

    #[test]
    fn it_escapes_tables() {
        assert_eq!(escape_table("foo bar"), String::from("\"foo bar\""));
        assert_eq!(
            escape_table("bobby\"; drop table foo"),
            String::from("\"bobby\"\"; drop table foo\"")
        )
    }

    #[test]
    fn it_escapes_fields() {
        assert_eq!(
            escape_fields(&["foo bar"]),
            vec!(String::from("\"foo bar\""))
        );
        assert_eq!(
            escape_fields(&["foo bar", "\"foo; drop table bar;"]),
            vec!(
                String::from("\"foo bar\""),
                String::from("\"\"\"foo; drop table bar;\"")
            )
        )
    }
}
