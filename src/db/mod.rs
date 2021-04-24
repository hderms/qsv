use std::str;
use std::time::Instant;

use rusqlite::types::ValueRef;
use rusqlite::{CachedStatement, Connection, Result};

use crate::db::utils::repeat_vars;
use std::error::Error;

pub mod utils;

pub struct Db {
    pub connection: Connection,
}

impl Db {
    pub fn open_in_memory() -> Result<Db> {
        let connection = Connection::open_in_memory()?;
        let mmap_size: u32 = 0;
        connection.pragma_update(None, "mmap_size", &mmap_size)?;
        connection.pragma_update(None, "journal_mode", &"off")?;
        connection.pragma_update(None, "synchronous", &"off")?;
        connection.pragma_update(None, "cache_size", &-16_000i32)?;
        connection.pragma_update(None, "read_uncommitted", &"true")?;
        connection.pragma_update(None, "wal_autocheckpoint", &0u32)?;
        connection.pragma_update(None, "threads", &8u32)?;
        Ok(Db { connection })
    }

    pub fn create_table(&mut self, table_name: &str, fields: &[&str]) -> Result<usize> {
        let string = format!("create table {} ({});", table_name, fields.join(", "));
        self.connection.execute(string.as_str(), [])
    }

    pub fn insert(&mut self, table_name: &str, fields: &[&str], values: Vec<Vec<&str>>) {
        let fields_len = fields.len();
        let fields = fields.join(",");
        let string = format!(
            "INSERT INTO {} ({}) values ({})",
            table_name,
            fields,
            repeat_vars(fields_len)
        );
        let mut stmt = self.connection.prepare_cached(string.as_str()).unwrap();
        self.connection.execute_batch("BEGIN TRANSACTION").unwrap();
        let now = Instant::now();

        for value in values.iter() {
            let params_from_iter = rusqlite::params_from_iter(value);
            stmt.execute(params_from_iter).unwrap();
        }
        let elapsed = now.elapsed().as_millis();
        println!("wrote {} records in {} ms", values.len(), elapsed);
        self.connection.execute_batch("END TRANSACTION").unwrap();
    }

    pub fn select_statement(&self, query: &str) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
        println!("statement is {:?}", query);
        let mut statement: CachedStatement = self.connection.prepare_cached(query).unwrap();
        let results = statement
            .query_map([], move |row| {
                let mut vec = Vec::with_capacity(row.column_count());
                for i in 0..row.column_count() {
                    let value = row.get_ref_unwrap(i);
                    let v = match value {
                        ValueRef::Null => String::from("null"),
                        ValueRef::Integer(i) => i.to_string(),
                        ValueRef::Text(buf) => String::from(str::from_utf8(buf).unwrap()),

                        _ => String::from("foo"),
                    };
                    vec.push(v);
                }
                Ok(vec)
            })
            .unwrap();
        let mut vec = Vec::with_capacity(1000);
        for result in results {
            vec.push(result?);
        }
        Ok(vec)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn can_execute_a_query() {
        let db = Db::open_in_memory().unwrap();
        let result: usize = db
            .connection
            .query_row("SELECT 1 = 1", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn can_create_table() {
        let mut db = Db::open_in_memory().unwrap();
        db.create_table("foobar", &vec!["id integer", "name text"])
            .unwrap();
        let result: usize = db
            .connection
            .execute(
                "insert into foobar (id, name) values (?1, ?2)",
                ["42", "bar"],
            )
            .unwrap();
        assert_eq!(result, 1);
        let pair: (usize, String) = db
            .connection
            .query_row("select id, name from foobar where id = 42", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(pair.0, 42);
        assert_eq!(pair.1, "bar");
    }

    #[test]
    fn can_insert() {
        let mut db = Db::open_in_memory().unwrap();
        db.create_table("foobar", &vec!["id integer", "name text"])
            .unwrap();
        db.insert(
            "foobar",
            &vec!["id", "name"],
            vec![vec!["42", "bar"], vec!["43", "baz"]],
        );
        let pair: (usize, String) = db
            .connection
            .query_row("select id, name from foobar where id = 42", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(pair.0, 42);
        assert_eq!(pair.1, "bar");

        let count: u32 = db
            .connection
            .query_row(
                "select count(*) from foobar where id = 42 or id = 43",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }
}
