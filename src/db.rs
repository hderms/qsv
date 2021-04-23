use rusqlite::{Connection, Result};

struct Db {
    pub connection: Connection,
}
impl Db {
    fn open_in_memory() -> Result<Db> {
        let connection = Connection::open_in_memory()?;
        Ok(Db { connection })
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
}
