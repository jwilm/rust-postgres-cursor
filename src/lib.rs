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
//!     .finalize()
//!     .expect("cursor creation succeeded");
//!
//! // Iterate over batches of rows
//! for result in cursor.iter() {
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
extern crate rand;

#[macro_use]
#[cfg(test)]
extern crate lazy_static;

use std::{fmt, mem};

use postgres::Connection;
use postgres::types::ToSql;
use postgres::rows::{Rows};
use rand::{thread_rng, Rng};

struct Hex<'a>(&'a [u8]);
impl<'a> fmt::Display for Hex<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }

        Ok(())
    }
}

/// Represents a PostgreSQL cursor.
///
/// The actual cursor in the database is only created and active _while_
/// `Iter` is in scope and calls to `next()` return `Some`.
pub struct Cursor<'conn> {
    conn: &'conn Connection,
    closed: bool,
    cursor_name: String,
    fetch_query: String,
    batch_size: u32
}

impl<'conn> Cursor<'conn> {
    fn new<'c, 'a, D>(builder: Builder<'c, 'a, D>) -> postgres::Result<Cursor<'c>>
        where D: fmt::Display + ?Sized
    {
        let mut bytes: [u8; 8] = unsafe { mem::uninitialized() };
        thread_rng().fill_bytes(&mut bytes[..]);

        let cursor_name = format!("cursor:{}:{}", builder.tag, Hex(&bytes));
        let query = format!("DECLARE \"{}\" CURSOR FOR {}", cursor_name, builder.query);
        let fetch_query = format!("FETCH {} FROM \"{}\"", builder.batch_size, cursor_name);

        builder.conn.execute("BEGIN", &[])?;
        builder.conn.execute(&query[..], builder.params)?;

        Ok(Cursor {
            closed: false,
            conn: builder.conn,
            cursor_name,
            fetch_query,
            batch_size: builder.batch_size,
        })
    }

    pub fn build<'b>(conn: &'b Connection) -> Builder<'b, 'static, str> {
        Builder::<str>::new(conn)
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
    pub fn iter<'b>(&'b mut self) -> Iter<'b, 'a> {
        Iter {
            cursor: self,
        }
    }

    fn next_batch(&mut self) -> postgres::Result<Rows<'static>> {
        let rows = self.conn.query(&self.fetch_query[..], &[])?;
        if rows.len() < (self.batch_size as usize) {
            self.close()?;
        }
        Ok(rows)
    }

    fn close(&mut self) -> postgres::Result<()> {
        if !self.closed {
            let close_query = format!("CLOSE \"{}\"", self.cursor_name);
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
pub struct Builder<'conn, 'builder, D: ?Sized + 'builder> {
    batch_size: u32,
    query: &'builder str,
    conn: &'conn Connection,
    tag: &'builder D,
    params: &'builder [&'builder ToSql],
}

impl<'conn, 'builder, D: fmt::Display + ?Sized + 'builder> Builder<'conn, 'builder, D> {
    fn new<'c>(conn: &'c Connection) -> Builder<'c, 'static, str> {
        Builder {
            conn,
            batch_size: 5_000,
            query: "SELECT 1 as one",
            tag: "default",
            params: &[],
        }
    }

    /// Set query params for cursor creation
    pub fn query_params(mut self, params: &'builder [&'builder ToSql]) -> Self {
        self.params = params;
        self
    }

    /// Set the batch size passed to `FETCH` on each iteration.
    ///
    /// Default is 5,000.
    pub fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the tag for cursor name.
    ///
    /// Adding a tag to the cursor name can be helpful for identifying where
    /// cursors originate when viewing `pg_stat_activity`.
    ///
    /// Default is `default`.
    ///
    /// # Examples
    ///
    /// Any type that implements `fmt::Display` may be provided as a tag. For example, a simple
    /// string literal is one option.
    ///
    /// ```no_run
    /// # extern crate postgres;
    /// # extern crate postgres_cursor;
    /// # use postgres::{Connection, TlsMode};
    /// # use postgres_cursor::Cursor;
    /// # fn main() {
    /// # let conn = Connection::connect("postgres://jwilm@127.0.0.1/foo", TlsMode::None)
    /// #     .expect("connect");
    /// let mut cursor = Cursor::build(&conn)
    ///     .tag("custom-cursor-tag")
    ///     .finalize();
    /// # }
    /// ```
    ///
    /// Or maybe you want to build a tag at run-time without incurring an extra allocation:
    ///
    /// ```no_run
    /// # extern crate postgres;
    /// # extern crate postgres_cursor;
    /// # use postgres::{Connection, TlsMode};
    /// # use postgres_cursor::Cursor;
    /// # fn main() {
    /// # let conn = Connection::connect("postgres://jwilm@127.0.0.1/foo", TlsMode::None)
    /// #     .expect("connect");
    /// use std::fmt;
    ///
    /// struct Pid(i32);
    /// impl fmt::Display for Pid {
    ///     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    ///         write!(f, "pid-{}", self.0)
    ///     }
    /// }
    ///
    /// let tag = Pid(8123);
    /// let mut cursor = Cursor::build(&conn)
    ///     .tag(&tag)
    ///     .finalize();
    /// # }
    /// ```
    pub fn tag<D2: fmt::Display + ?Sized>(self, tag: &'builder D2) -> Builder<'conn, 'builder, D2> {
        Builder {
            batch_size: self.batch_size,
            query: self.query,
            conn: self.conn,
            tag: tag,
            params: self.params
        }
    }

    /// Set the query to create a cursor for.
    ///
    /// Default is `SELECT 1`.
    pub fn query(mut self, query: &'builder str) -> Self {
        self.query = query;
        self
    }

    /// Turn the builder into a `Cursor`.
    pub fn finalize(self) -> postgres::Result<Cursor<'conn>> {
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
                .finalize().unwrap();

            let mut got = 0;
            for batch in cursor.iter() {
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
                .finalize().unwrap();

            let mut got = 0;
            for batch in cursor.iter() {
                let batch = batch.unwrap();
                got += batch.len();
            }

            assert_eq!(got, 197);
        });
    }

    #[test]
    fn build_cursor_with_tag() {
        with_items(1, |conn| {
            {
                let cursor = Cursor::build(conn)
                    .tag("foobar")
                    .finalize().unwrap();

                assert!(cursor.cursor_name.starts_with("cursor:foobar"));
            }

            struct Foo;
            use std::fmt;
            impl fmt::Display for Foo {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    write!(f, "foo-{}", 1)
                }
            }

            {
                let foo = Foo;
                let cursor = Cursor::build(conn)
                    .tag(&foo)
                    .finalize().unwrap();

                println!("{}", cursor.cursor_name);
                assert!(cursor.cursor_name.starts_with("cursor:foo-1"));
            }
        });
    }

    #[test]
    fn cursor_with_long_tag() {
        with_items(100, |conn| {
            let mut cursor = Cursor::build(conn)
                .tag("really-long-tag-damn-that-was-only-three-words-foo-bar-baz")
                .query("SELECT id FROM products")
                .finalize().unwrap();

            let mut got = 0;
            for batch in cursor.iter() {
                let batch = batch.unwrap();
                got += batch.len();
            }

            assert_eq!(got, 100);
        });
    }

    #[test]
    fn cursor_with_params() {
        with_items(100, |conn| {
            let mut cursor = Cursor::build(conn)
                .query("SELECT id FROM products WHERE id > $1 AND id < $2")
                .query_params(&[&1, &10])
                .finalize().unwrap();

            let mut got = 0;
            for batch in cursor.iter() {
                let batch = batch.unwrap();
                got += batch.len();
            }

            assert_eq!(got, 8);
        });
    }
}
