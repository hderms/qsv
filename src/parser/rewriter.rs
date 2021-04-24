use std::collections::HashMap;

use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor, TableWithJoins};

/// Allows rewriting a SQL AST by mapping table identifiers to tablenames in mapping
pub struct Rewriter {
    files_to_tables: HashMap<String, String>,
}

impl Rewriter {
    pub fn new(files_to_tables: HashMap<String, String>) -> Rewriter {
        Rewriter { files_to_tables }
    }
    /// Rewrite the table identifiers in a SQL AST based on a mapping from the table identifier to
    /// the table name
    pub fn rewrite(&self, ast: &mut Statement) {
        match ast {
            Statement::Query(boxed) => self.recurse_query(boxed),

            _ => {
                panic!("unrecognized")
            }
        }
    }

    fn recurse_query(&self, boxed: &mut Query) {
        {
            if let Some(ref mut with) = boxed.with {
                for cte in with.cte_tables.iter_mut() {
                    self.recurse_query(&mut cte.query);
                }
            }
            match boxed.body {
                SetExpr::Select(ref mut boxed) => {
                    for from in boxed.from.iter_mut() {
                        self.recurse_table_with_joins(from);
                    }
                }
                SetExpr::Query(ref mut query) => self.recurse_query(query),
                SetExpr::SetOperation {
                    ref mut left,
                    ref mut right,
                    ..
                } => {
                    match left.as_mut() {
                        SetExpr::Select(select) => {
                            self.recurse_select(select);
                        }
                        SetExpr::Query(query) => {
                            self.recurse_query(query);
                        }
                        _ => (),
                    }

                    match right.as_mut() {
                        SetExpr::Select(select) => {
                            self.recurse_select(select);
                        }
                        SetExpr::Query(query) => {
                            self.recurse_query(query);
                        }
                        _ => (),
                    }
                }
                SetExpr::Values(_) => {}
                SetExpr::Insert(_) => {}
            }
        }
    }

    fn recurse_select(&self, select: &mut Select) {
        for from in select.from.iter_mut() {
            self.recurse_table_with_joins(from);
        }
    }
    fn handle_relation(&self, relation: &mut TableFactor) {
        match relation {
            TableFactor::Table { ref mut name, .. } => {
                for ident in name.0.iter_mut() {
                    if let Some(entry) = self.files_to_tables.get(ident.value.to_string().as_str())
                    {
                        ident.value = entry.clone()
                    }
                }
            }
            TableFactor::Derived {
                ref mut subquery, ..
            } => self.recurse_query(subquery),
            TableFactor::TableFunction { .. } => {}
            TableFactor::NestedJoin(_) => {}
        }
    }
    fn recurse_table_with_joins(&self, from: &mut TableWithJoins) {
        self.handle_relation(&mut from.relation);
        for join in from.joins.iter_mut() {
            self.handle_relation(&mut join.relation);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use sqlparser::parser::Parser;

    use crate::parser::rewriter::Rewriter;
    use crate::parser::CsvDialect;
    #[test]
    fn it_rewrites() {
        let dialect = CsvDialect;
        let mut hm = HashMap::new();
        hm.insert(String::from("./foo.csv"), String::from("bar_table"));
        let rewriter = Rewriter::new(hm);
        let sql = "select * from (select * from ./foo.csv)";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        rewriter.rewrite(&mut ast[0]);
        assert_eq!(
            ast[0].to_string(),
            "SELECT * FROM (SELECT * FROM bar_table)"
        );

        let sql = "select * from (select * from (select * from ./foo.csv))";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        rewriter.rewrite(&mut ast[0]);
        assert_eq!(
            ast[0].to_string(),
            "SELECT * FROM (SELECT * FROM (SELECT * FROM bar_table))"
        );
    }

    #[test]
    fn it_rewrites_filenames_in_ctes() {
        let dialect = CsvDialect;
        let mut hm = HashMap::new();
        hm.insert(
            String::from("testdata/people.csv"),
            String::from("people_table"),
        );
        hm.insert(
            String::from("testdata/occupations.csv"),
            String::from("occupations_table"),
        );
        let rewriter = Rewriter::new(hm);
        let sql = " with some_cte (age) as (select distinct(age) from testdata/people.csv) select * from testdata/occupations.csv occupation INNER JOIN foo on (occupation.minimum_age = foo.age)";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        rewriter.rewrite(&mut ast[0]);

        assert_eq!(
            ast[0].to_string(),
            "WITH some_cte (age) AS (SELECT DISTINCT (age) FROM people_table) SELECT * FROM occupations_table AS occupation JOIN foo ON (occupation.minimum_age = foo.age)"
        );
    }

    #[test]
    fn it_rewrites_filenames_in_unions() {
        let dialect = CsvDialect;
        let mut hm = HashMap::new();
        hm.insert(
            String::from("testdata/people.csv"),
            String::from("people_table"),
        );
        hm.insert(
            String::from("testdata/occupations.csv"),
            String::from("occupations_table"),
        );
        let rewriter = Rewriter::new(hm);
        let sql = "select * from testdata/people.csv union select * from testdata/occupations.csv";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        rewriter.rewrite(&mut ast[0]);

        assert_eq!(
            ast[0].to_string(),
            "SELECT * FROM people_table UNION SELECT * FROM occupations_table"
        );
    }
}
