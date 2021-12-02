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
//! for result in &mut cursor {
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

use std::iter::IntoIterator;
use std::{fmt, mem};

use postgres::row::Row;
use postgres::types::ToSql;
use postgres::Client;
use rand::thread_rng;
use rand::RngCore;

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
pub struct Cursor<'client> {
    client: &'client mut Client,
    closed: bool,
    cursor_name: String,
    fetch_query: String,
    batch_size: u32,
}

impl<'client> Cursor<'client> {
    fn new<'c, 'a, D>(builder: Builder<'c, 'a, D>) -> Result<Cursor<'c>, postgres::Error>
    where
        D: fmt::Display + ?Sized,
    {
        let mut bytes: [u8; 8] = unsafe { *mem::MaybeUninit::uninit().assume_init_ref() };
        let mut rng = thread_rng();
        rng.fill_bytes(&mut bytes[..]);

        let cursor_name = format!("cursor:{}:{}", builder.tag, Hex(&bytes));
        let query = format!("DECLARE \"{}\" CURSOR FOR {}", cursor_name, builder.query);
        let fetch_query = format!("FETCH {} FROM \"{}\"", builder.batch_size, cursor_name);

        builder.client.execute("BEGIN", &[])?;
        builder.client.execute(&query[..], builder.params)?;

        Ok(Cursor {
            closed: false,
            client: builder.client,
            cursor_name,
            fetch_query,
            batch_size: builder.batch_size,
        })
    }

    pub fn build<'b>(client: &'b mut Client) -> Builder<'b, 'static, str> {
        Builder::<str>::new(client)
    }
}

/// Iterator returning `Rows` for every call to `next()`.
pub struct Iter<'b, 'a: 'b> {
    cursor: &'b mut Cursor<'a>,
}

impl<'b, 'a: 'b> Iterator for Iter<'b, 'a> {
    type Item = Result<Vec<Row>, postgres::Error>;

    fn next(&mut self) -> Option<Result<Vec<Row>, postgres::Error>> {
        if self.cursor.closed {
            None
        } else {
            Some(self.cursor.next_batch())
        }
    }
}

impl<'a, 'client> IntoIterator for &'a mut Cursor<'client> {
    type Item = Result<Vec<Row>, postgres::Error>;
    type IntoIter = Iter<'a, 'client>;

    fn into_iter(self) -> Iter<'a, 'client> {
        self.iter()
    }
}

impl<'a> Cursor<'a> {
    pub fn iter<'b>(&'b mut self) -> Iter<'b, 'a> {
        Iter { cursor: self }
    }

    fn next_batch(&mut self) -> Result<Vec<Row>, postgres::Error> {
        let rows = self.client.query(&self.fetch_query[..], &[])?;
        if rows.len() < (self.batch_size as usize) {
            self.close()?;
        }
        Ok(rows)
    }

    fn close(&mut self) -> Result<(), postgres::Error> {
        if !self.closed {
            let close_query = format!("CLOSE \"{}\"", self.cursor_name);
            self.client.execute(&close_query[..], &[])?;
            self.client.execute("COMMIT", &[])?;
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
pub struct Builder<'client, 'builder, D: ?Sized + 'builder> {
    batch_size: u32,
    query: &'builder str,
    client: &'client mut Client,
    tag: &'builder D,
    params: &'builder [&'builder (dyn ToSql + Sync)],
}

impl<'client, 'builder, D: fmt::Display + ?Sized + 'builder> Builder<'client, 'builder, D> {
    fn new<'c>(client: &'c mut Client) -> Builder<'c, 'static, str> {
        Builder {
            client,
            batch_size: 5_000,
            query: "SELECT 1 as one",
            tag: "default",
            params: &[],
        }
    }

    /// Set query params for cursor creation
    pub fn query_params(mut self, params: &'builder [&'builder (dyn ToSql + Sync)]) -> Self {
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
    pub fn tag<D2: fmt::Display + ?Sized>(
        self,
        tag: &'builder D2,
    ) -> Builder<'client, 'builder, D2> {
        Builder {
            batch_size: self.batch_size,
            query: self.query,
            client: self.client,
            tag,
            params: self.params,
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
    pub fn finalize(self) -> Result<Cursor<'client>, postgres::Error> {
        Cursor::new(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::Cursor;
    use postgres::Client;
    use postgres::NoTls;

    lazy_static! {
        static ref LOCK: Mutex<u8> = Mutex::new(0);
    }

    fn synchronized<F: FnOnce() -> T, T>(func: F) -> T {
        let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        func()
    }

    fn with_items<F: FnOnce(&mut Client) -> T, T>(items: i32, func: F) -> T {
        synchronized(|| {
            let mut client = get_client();
            client
                .execute("TRUNCATE TABLE products", &[])
                .expect("truncate");
            // Highly inefficient; should optimize.
            for i in 0..items {
                client
                    .execute("INSERT INTO products (id) VALUES ($1)", &[&i])
                    .expect("insert");
            }
            func(&mut client)
        })
    }

    fn get_client() -> Client {
        Client::connect(
            "postgres://postgres@127.0.0.1/postgresql_cursor_test",
            NoTls,
        )
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
                .finalize()
                .unwrap();

            let mut got = 0;
            for batch in &mut cursor {
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
                .finalize()
                .unwrap();

            let mut got = 0;
            for batch in &mut cursor {
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
                let cursor = Cursor::build(conn).tag("foobar").finalize().unwrap();

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
                let cursor = Cursor::build(conn).tag(&foo).finalize().unwrap();

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
                .finalize()
                .unwrap();

            let mut got = 0;
            for batch in &mut cursor {
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
                .finalize()
                .unwrap();

            let mut got = 0;
            for batch in &mut cursor {
                let batch = batch.unwrap();
                got += batch.len();
            }

            assert_eq!(got, 8);
        });
    }
}
