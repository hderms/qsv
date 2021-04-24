use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor, TableWithJoins};

/// Collection of table identifiers parsed from SQL
pub struct Collector {
    pub table_identifiers: Vec<String>,
}
impl Default for Collector {
    fn default() -> Self {
        Collector::new()
    }
}

impl Collector {
    pub fn new() -> Collector {
        let table_identifiers = vec![];
        Self { table_identifiers }
    }
    /// Collect all the table identifiers in a statement
    pub fn collect(&mut self, ast: &Statement) {
        match ast {
            Statement::Query(boxed) => self.recurse_query(boxed),

            _ => {
                panic!("unrecognized")
            }
        }
    }

    fn recurse_query(&mut self, boxed: &Query) {
        {
            if let Some(with) = &boxed.with {
                for cte in with.cte_tables.iter() {
                    for ident in cte.from.iter() {
                        self.table_identifiers.push(ident.value.to_string());
                    }
                    let query = &cte.query;
                    self.recurse_query(query);
                }
            }
            match &boxed.body {
                SetExpr::Select(ref boxed) => {
                    for from in boxed.from.iter() {
                        self.recurse_table_with_joins(from);
                    }
                }
                SetExpr::Query(boxed_query) => {
                    self.recurse_query(boxed_query);
                }
                SetExpr::SetOperation { left, right, .. } => {
                    match left.as_ref() {
                        SetExpr::Select(select) => {
                            self.recurse_select(select);
                        }
                        SetExpr::Query(query) => {
                            self.recurse_query(query);
                        }
                        _ => (),
                    }

                    match right.as_ref() {
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
    fn recurse_select(&mut self, select: &Select) {
        for from in select.from.iter() {
            self.recurse_table_with_joins(from);
        }
    }
    fn recurse_table_with_joins(&mut self, from: &TableWithJoins) {
        match &from.relation {
            TableFactor::Table { name, .. } => {
                for ident in name.0.iter() {
                    self.table_identifiers.push(ident.value.to_string());
                }
            }
            TableFactor::Derived { subquery, .. } => self.recurse_query(&subquery),
            TableFactor::TableFunction { .. } => {}
            TableFactor::NestedJoin(_) => {
                println!("nested join")
            }
        }
        for join in from.joins.iter() {
            match &join.relation {
                TableFactor::Table { name, .. } => {
                    for ident in name.0.iter() {
                        self.table_identifiers.push(ident.value.to_string())
                    }
                }
                TableFactor::Derived { .. } => {}
                TableFactor::TableFunction { .. } => {}
                TableFactor::NestedJoin(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::CsvDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn it_collects() {
        let dialect = CsvDialect;
        let mut collector = Collector::new();
        let sql = "select * from (select * from ./foo.csv)";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        collector.collect(&mut ast[0]);
        assert_eq!(collector.table_identifiers, vec!(String::from("./foo.csv")));
    }

    #[test]
    fn it_collects_recursively() {
        let dialect = CsvDialect;
        let mut collector = Collector::new();
        let sql = "select * from (select * from (select * from ./foo.csv))";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        collector.collect(&mut ast[0]);
        assert_eq!(collector.table_identifiers, vec!(String::from("./foo.csv")));
    }

    #[test]
    fn it_collects_filenames_from_ctes() {
        let dialect = CsvDialect;
        let mut collector = Collector::new();
        let sql = " with some_cte (age) as (select distinct(age) from testdata/people.csv) select * from testdata/occupations.csv occupation INNER JOIN foo on (occupation.minimum_age = foo.age)";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        collector.collect(&mut ast[0]);
        assert_eq!(
            collector.table_identifiers,
            vec!(
                String::from("testdata/people.csv"),
                String::from("testdata/occupations.csv"),
                String::from("foo"),
            )
        );
    }

    #[test]
    fn it_collects_filenames_from_unions() {
        let dialect = CsvDialect;
        let mut collector = Collector::new();
        let sql = "select * from testdata/people.csv union select * from testdata/occupations.csv";
        let mut ast = Parser::parse_sql(&dialect, sql).unwrap();
        collector.collect(&mut ast[0]);
        assert_eq!(
            collector.table_identifiers,
            vec!(
                String::from("testdata/people.csv"),
                String::from("testdata/occupations.csv")
            )
        );
    }
}
