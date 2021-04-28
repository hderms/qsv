use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser as SqlParser, ParserError};

pub mod collector;
pub mod rewriter;
pub struct Parser {}
impl Parser {
    /// Parse SQL using our CSVDialect
    pub fn parse_sql(str: &str) -> Result<Vec<Statement>, ParserError> {
        SqlParser::parse_sql(&CsvDialect, str)
    }
}

#[derive(Debug, Default)]
/// SQL Dialect based off of SQLite dialect but allowing common path characters to be used as well
pub struct CsvDialect;

impl Dialect for CsvDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"' || ch == '['
    }

    #[allow(clippy::nonminimal_bool)]
    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.sqlite.org/draft/tokenreq.html
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ch == '_'
            || ch == '$'
            || ('\u{007f}'..='\u{ffff}').contains(&ch)
            || ch == '_'
            || ch == '_'
            || ch == '.'
            || ch == '/'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ('0'..='9').contains(&ch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_sql_with_filenames_for_tables() {
        let sql = "select * from ./foo.csv";
        let ast = Parser::parse_sql(sql).unwrap();

        assert_eq!(ast[0].to_string(), "SELECT * FROM ./foo.csv");
        println!("AST: {:?}", ast)
    }

    #[test]
    fn it_parses_sql_with_filenames_for_tables_in_subqueries() {
        let sql = "select * from (select * from ./foo.csv)";
        let ast = Parser::parse_sql(sql).unwrap();
        assert_eq!(
            ast[0].to_string(),
            "SELECT * FROM (SELECT * FROM ./foo.csv)"
        );
        println!("AST: {:?}", ast)
    }

    #[test]
    fn it_parses_sql_with_backticks_around_columns_with_spaces() {
        let sql = "select * from (select ` foo bar` from ./file.csv)";
        let ast = Parser::parse_sql(sql).unwrap();
        assert_eq!(
            ast[0].to_string(),
            "SELECT * FROM (SELECT ` foo bar` FROM ./file.csv)"
        );
        println!("AST: {:?}", ast)
    }

    #[test]
    fn it_parses_sql_files_with_spaces_with_backticks_around_filename() {
        let sql = "select * from (select foo from `./file with spaces.csv`)";
        let ast = Parser::parse_sql(sql).unwrap();
        assert_eq!(
            ast[0].to_string(),
            "SELECT * FROM (SELECT foo FROM `./file with spaces.csv`)"
        );
        println!("AST: {:?}", ast)
    }
}
