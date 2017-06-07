//! Provides a Cursor abstraction for use with the `postgres` crate.
//!
//! # Examples
//!
//! ```no_run
//! extern crate postgres;
//! extern crate postgres_cursor;
//!
//! use postgres::{Connection, TlsMode};
//! use postgres_cursor::Cursor;
//!
//! # fn main() {
//!
//! // First, establish a connection with postgres
//! let conn = Connection::connect("postgres://jwilm@127.0.0.1/foo", TlsMode::None)
//!     .expect("connect");
//!
//! // Build the cursor
//! let mut cursor = Cursor::build(&conn)
//!     // Batch size determines rows returned in each FETCH call
//!     .batch_size(10)
//!     // Query is the statement to build a cursor for
//!     .query("SELECT id FROM products")
//!     // Finalize turns this builder into a cursor
//!     .finalize();
//!
//! // Iterate over batches of rows
//! for result in cursor.iter().unwrap() {
//!     // Each item returned from the iterator is a Result<Rows>.
//!     // This is because each call to `next()` makes a query
//!     // to the database.
//!     let rows = result.unwrap();
//!
//!     // After handling errors, rows returned in this iteration
//!     // can be iterated over.
//!     for row in &rows {
//!         println!("{:?}", row);
//!     }
//! }
//!
//! # }
//! ```
extern crate postgres;

use postgres::Connection;
use postgres::rows::{Rows};

#[macro_use]
#[cfg(test)]
extern crate lazy_static;

/// Represents a PostgreSQL cursor.
///
/// The actual cursor in the database is only created and active _while_
/// `Iter` is in scope and calls to `next()` return `Some`.
pub struct Cursor<'a> {
    conn: &'a Connection,
    closed: bool,
    cursor_name: String,
    query: String,
    fetch_query: String,
    batch_size: u32
}

impl<'a> Cursor<'a> {
    fn new<'b>(builder: Builder<'b>) -> Cursor<'b> {
        // TODO real cursor names; something like cursor_{pid}_{counter}
        let cursor_name = "foo_cursor".to_string();
        let query = format!("DECLARE {} CURSOR FOR {}", cursor_name, builder.query);
        let fetch_query = format!("FETCH {} FROM {}", builder.batch_size, cursor_name);

        Cursor {
            closed: true,
            conn: builder.conn,
            cursor_name,
            query,
            fetch_query,
            batch_size: builder.batch_size,
        }
    }

    pub fn build<'b>(conn: &'b Connection) -> Builder<'b> {
        Builder::new(conn)
    }
}


/// Iterator returning `Rows` for every call to `next()`.
pub struct Iter<'b, 'a: 'b> {
    cursor: &'b mut Cursor<'a>,
}

impl<'b, 'a: 'b> Iterator for Iter<'b, 'a> {
    type Item = postgres::Result<Rows<'static>>;

    fn next(&mut self) -> Option<postgres::Result<Rows<'static>>> {
        if self.cursor.closed {
            None
        } else {
            Some(self.cursor.next_batch())
        }
    }
}

impl<'a> Cursor<'a> {
    pub fn iter<'b>(&'b mut self) -> postgres::Result<Iter<'b, 'a>> {
        self.declare_cursor()?;

        Ok(Iter {
            cursor: self,
        })
    }

    fn next_batch(&mut self) -> postgres::Result<Rows<'static>> {
        let rows = self.conn.query(&self.fetch_query[..], &[])?;
        if rows.len() < (self.batch_size as usize) {
            self.close()?;
        }
        Ok(rows)
    }

    fn declare_cursor(&mut self) -> postgres::Result<()> {
        self.conn.execute("BEGIN", &[])?;
        self.conn.execute(&self.query[..], &[])?;
        self.closed = false;
        Ok(())
    }

    fn close(&mut self) -> postgres::Result<()> {
        if !self.closed {
            let close_query = format!("CLOSE {}", self.cursor_name);
            self.conn.execute(&close_query[..], &[])?;
            self.conn.execute("COMMIT", &[])?;
            self.closed = true;
        }

        Ok(())
    }
}

impl<'a> Drop for Cursor<'a> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Builds a Cursor
///
/// This type is constructed by calling `Cursor::build`.
pub struct Builder<'a> {
    batch_size: u32,
    query: &'a str,
    conn: &'a Connection,
}

impl<'a> Builder<'a> {
    fn new<'b>(conn: &'b Connection) -> Builder<'b> {
        Builder {
            conn,
            batch_size: 5_000,
            query: "SELECT 1 as one",
        }
    }

    /// Set the batch size passed to `FETCH` on each iteration.
    ///
    /// Default is 5,000.
    pub fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the query to create a cursor for.
    ///
    /// Default is `SELECT 1`.
    pub fn query(mut self, query: &'a str) -> Self {
        self.query = query;
        self
    }

    /// Turn the builder into a `Cursor`.
    pub fn finalize(self) -> Cursor<'a> {
        Cursor::new(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use postgres::{Connection, TlsMode};
    use super::Cursor;

    lazy_static! {
        static ref LOCK: Mutex<u8> = {
            Mutex::new(0)
        };
    }

    fn synchronized<F: FnOnce() -> T, T>(func: F) -> T {
        let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        func()
    }

    fn with_items<F: FnOnce(&Connection) -> T, T>(items: i32, func: F) -> T {
        synchronized(|| {
            let conn = get_connection();
            conn.execute("TRUNCATE TABLE products", &[]).expect("truncate");
            // Highly inefficient; should optimize.
            for i in 0..items {
                conn.execute("INSERT INTO products (id) VALUES ($1)", &[&i]).expect("insert");
            }
            func(&conn)
        })
    }

    fn get_connection() -> Connection {
        Connection::connect("postgres://jwilm@127.0.0.1/postgresql_cursor_test", TlsMode::None)
            .expect("connect")
    }

    #[test]
    fn test_framework_works() {
        let count = 183;
        with_items(count, |conn| {
            for row in &conn.query("SELECT COUNT(*) FROM products", &[]).unwrap() {
                let got: i64 = row.get(0);
                assert_eq!(got, count as i64);
            }
        });
    }

    #[test]
    fn cursor_iter_works_when_batch_size_divisible() {
        with_items(200, |conn| {
            let mut cursor = Cursor::build(conn)
                .batch_size(10)
                .query("SELECT id FROM products")
                .finalize();

            let mut got = 0;
            for batch in cursor.iter().unwrap() {
                let batch = batch.unwrap();
                got += batch.len();
            }

            assert_eq!(got, 200);
        });
    }

    #[test]
    fn cursor_iter_works_when_batch_size_remainder() {
        with_items(197, |conn| {
            let mut cursor = Cursor::build(conn)
                .batch_size(10)
                .query("SELECT id FROM products")
                .finalize();

            let mut got = 0;
            for batch in cursor.iter().unwrap() {
                let batch = batch.unwrap();
                got += batch.len();
            }

            assert_eq!(got, 197);
        });
    }
}
